package com.mycompany.batch.service;

import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.auth.BasicAuthProvider;
import com.mycompany.batch.auth.HttpAuthProvider;
import com.mycompany.batch.auth.JwtAuthProvider;
import com.mycompany.batch.auth.KerberosAuthProvider;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.xpath.XPathColumn;
import com.mycompany.batch.xpath.XPathExtractor;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.SequencedSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class BatchService {

    private final ObjectMapper objectMapper;
    private final XPathExtractor xpathExtractor;
    private final BatchProperties batchProperties;
    private final Map<String, HttpAuthProvider> authProviders = new LinkedHashMap<>();

    public BatchService(ObjectMapper objectMapper, XPathExtractor xpathExtractor,
                        BatchProperties batchProperties) {
        this.objectMapper = objectMapper;
        this.xpathExtractor = xpathExtractor;
        this.batchProperties = batchProperties;
    }

    @PostConstruct
    void validateConfig() {
        batchProperties.getOperations().forEach((name, op) -> {
            op.validate(name);
            try {
                authProviders.put(name, buildAuthProvider(name, op.getAuth()));
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to initialise auth for operation '" + name + "': " + e.getMessage(), e);
            }
        });
    }

    private HttpAuthProvider buildAuthProvider(String operationName,
                                               BatchProperties.AuthProperties auth) throws Exception {
        return switch (auth.getMethod().trim().toUpperCase()) {
            case "BASIC" -> {
                if (auth.getBasic().getUsername().isBlank() || auth.getBasic().getPassword().isBlank()) {
                    throw new IllegalStateException(
                            "BASIC auth for '" + operationName + "' requires basic.username and basic.password");
                }
                yield new BasicAuthProvider(auth.getBasic().getUsername(), auth.getBasic().getPassword());
            }
            case "JWT" -> {
                if (auth.getJwt().getUrl().isBlank() || auth.getJwt().getUsername().isBlank()
                        || auth.getJwt().getPassword().isBlank()) {
                    throw new IllegalStateException(
                            "JWT auth for '" + operationName + "' requires jwt.url, jwt.username and jwt.password");
                }
                yield new JwtAuthProvider(auth.getJwt().getApplicationName(), auth.getJwt().getUsername(),
                        auth.getJwt().getPassword(), auth.getJwt().getUrl(), objectMapper);
            }
            case "KERBEROS" -> {
                if (auth.getKerberos().getUsername().isBlank() || auth.getKerberos().getKeytab().isBlank()
                        || auth.getKerberos().getServicePrincipal().isBlank()) {
                    throw new IllegalStateException(
                            "KERBEROS auth for '" + operationName + "' requires kerberos.username, kerberos.keytab and kerberos.service-principal");
                }
                yield new KerberosAuthProvider(auth.getKerberos().getUsername(),
                        auth.getKerberos().getKeytab(), auth.getKerberos().getServicePrincipal());
            }
            default -> () -> null;
        };
    }

    // -------------------------------------------------------------------------
    // Public result types
    // -------------------------------------------------------------------------

    public record HttpStats(long minMs, long maxMs, double avgMs) {}
    public record ColumnDef(String columnName, String type, String displayName) {}
    public record BatchResult(int processed, int succeeded, int failed,
                              HttpStats httpStats,
                              List<ColumnDef> columns,
                              List<Map<String, Object>> results,
                              String batchUuid,
                              String timestamp,
                              long timeTakenMs,
                              double responseSizeKb) {}
    public record PsvResult(int processed, int succeeded, int failed, String outputFile,
                            String batchUuid, String timestamp) {}

    // -------------------------------------------------------------------------
    // Public API — unified RunRequest entry point
    // -------------------------------------------------------------------------

    /**
     * Primary entry point used by both the REST controller and the WebSocket handler.
     * Resolves the input source, executes the operation and returns the result.
     */
    public BatchResult run(RunRequest request) throws Exception {
        String operationType = request.operationType();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);

        String inputSourceType = (request.inputSource() != null && !request.inputSource().isBlank())
                ? request.inputSource().trim().toUpperCase()
                : op.getInputSource().getType().trim().toUpperCase();

        List<String> identifiers = switch (inputSourceType) {
            case "FILE" -> {
                String path = request.inputFilePath();
                if (path == null || path.isBlank()) {
                    throw new IllegalArgumentException("inputFilePath is required for FILE input source");
                }
                if (!Files.exists(Path.of(path))) {
                    throw new IllegalArgumentException("inputFilePath does not exist: " + path);
                }
                yield readIdentifiersFromFile(path, request.inputCount(), op);
            }
            case "HTTPGET", "HTTPPOST" -> {
                List<String> ids = request.ids();
                if (ids == null || ids.isEmpty()) {
                    throw new IllegalArgumentException(
                            "ids list is required for " + inputSourceType + " input source");
                }
                Integer count = request.inputCount();
                yield count != null ? ids.stream().limit(count).toList() : List.copyOf(ids);
            }
            case "HTTPCONFIG" -> {
                List<String> ids = fetchIdentifiersFromHttpConfig(op.getInputSource().getHttpConfig());
                Integer count = request.inputCount();
                yield count != null ? ids.stream().limit(count).toList() : ids;
            }
            default -> throw new IllegalArgumentException("Unknown inputSource type: " + inputSourceType);
        };

        if (request.debugMode() != null && request.debugMode() == -1) {
            return debugResult(identifiers);
        }

        return runCore(identifiers, op, operationType);
    }

    // -------------------------------------------------------------------------
    // Public API — convenience overloads kept for backward compatibility
    // -------------------------------------------------------------------------

    /** File-based run. Validates mandatory-attributes against the file header. */
    public BatchResult run(String filePath, Integer inputCount, String operationType) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);
        List<String> identifiers = readIdentifiersFromFile(filePath, inputCount, op);
        return runCore(identifiers, op, operationType);
    }

    /** Direct list of identifiers (e.g. already resolved by the caller). */
    public BatchResult run(List<String> identifiers, String operationType) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);
        return runCore(identifiers, op, operationType);
    }

    /** Runs from file and writes PSV output; returns summary metadata. */
    public PsvResult runToPsv(String inputFilePath, String outputFilePath,
                              Integer inputCount, String operationType) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);
        List<String> identifiers = readIdentifiersFromFile(inputFilePath, inputCount, op);
        BatchResult batch = runCore(identifiers, op, operationType);
        writeToPsv(batch, outputFilePath);
        return new PsvResult(batch.processed(), batch.succeeded(), batch.failed(),
                Path.of(outputFilePath).toAbsolutePath().toString(),
                batch.batchUuid(), batch.timestamp());
    }

    // -------------------------------------------------------------------------
    // Input-source helpers
    // -------------------------------------------------------------------------

    /**
     * Reads identifier lines from a delimited file.
     * The first line is treated as a header and validated against
     * {@code mandatory-attributes} if any are configured.
     */
    public List<String> readIdentifiersFromFile(String filePath, Integer inputCount,
                                                BatchProperties.OperationProperties op) throws Exception {
        List<String> allLines = Files.readAllLines(Path.of(filePath));
        List<String> headerColumns = allLines.isEmpty() ? List.of()
                : Arrays.stream(allLines.get(0).split("[,|\\t]"))
                        .map(String::trim).filter(s -> !s.isBlank()).toList();

        List<String> missing = op.getMandatoryAttributeList().stream()
                .filter(attr -> !headerColumns.contains(attr))
                .toList();
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Input file is missing mandatory attribute(s): " + missing
                            + ". Found headers: " + headerColumns);
        }

        var stream = allLines.stream().skip(1).map(String::trim).filter(s -> !s.isBlank());
        if (inputCount != null) stream = stream.limit(inputCount);
        return stream.collect(Collectors.toList());
    }

    /**
     * Calls the configured HTTPCONFIG URL, optionally applies a JSONata transform,
     * and returns the resulting list of string identifiers.
     */
    public List<String> fetchIdentifiersFromHttpConfig(
            BatchProperties.HttpConfigSourceProperties config) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(config.getUrl()));
        if ("POST".equalsIgnoreCase(config.getMethod())) {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.GET();
        }

        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("HTTPCONFIG source returned HTTP " + response.statusCode());
        }

        Object parsed = objectMapper.readValue(response.body(), Object.class);

        String transform = resolveJsonataExpression(config.getJsonataTransform());
        if (transform != null && !transform.isBlank()) {
            parsed = Jsonata.jsonata(transform).evaluate(parsed);
        }

        if (parsed instanceof List<?> list) {
            return list.stream()
                    .map(item -> item != null ? item.toString() : "")
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());
        }
        throw new RuntimeException("HTTPCONFIG source: expected JSON array, got: "
                + (parsed == null ? "null" : parsed.getClass().getSimpleName()));
    }

    // -------------------------------------------------------------------------
    // File output helper
    // -------------------------------------------------------------------------

    /** Writes a {@link BatchResult} to a pipe-separated-values file. */
    public void writeToPsv(BatchResult batch, String outputFilePath) throws Exception {
        SequencedSet<String> columns = new LinkedHashSet<>();
        columns.add("identifier");
        for (Map<String, Object> row : batch.results()) {
            if (!"FAILED".equals(row.get("operationStatus"))) {
                row.keySet().stream()
                        .filter(k -> !k.equals("identifier"))
                        .forEach(columns::add);
                break;
            }
        }

        Path outputPath = Path.of(outputFilePath);
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write(String.join("|", columns));
            writer.newLine();
            for (Map<String, Object> row : batch.results()) {
                List<String> line = new ArrayList<>();
                for (String col : columns) {
                    Object val = row.get(col);
                    if (val == null && "FAILED".equals(row.get("operationStatus"))) {
                        val = col.equals("identifier") ? row.get("identifier")
                                : "ERROR: " + row.get("errorMessage");
                    }
                    line.add(val != null ? val.toString() : "");
                }
                writer.write(String.join("|", line));
                writer.newLine();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Debug mode (skip extraction)
    // -------------------------------------------------------------------------

    /** Returns input identifiers as plain objects without making any HTTP calls. */
    private BatchResult debugResult(List<String> identifiers) {
        List<Map<String, Object>> results = identifiers.stream()
                .map(id -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("identifier", id);
                    return row;
                })
                .collect(Collectors.toList());

        String batchUuid = UUID.randomUUID().toString();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

        return new BatchResult(
                identifiers.size(), identifiers.size(), 0,
                new HttpStats(0, 0, 0),
                List.of(new ColumnDef("identifier", "string", "Identifier")),
                results, batchUuid, timestamp, 0L, 0.0);
    }

    // -------------------------------------------------------------------------
    // Core execution
    // -------------------------------------------------------------------------

    private BatchResult runCore(List<String> identifiers,
                                BatchProperties.OperationProperties op,
                                String operationType) throws Exception {
        String extractionType = op.getDataExtraction().getType().trim().toUpperCase();

        List<XPathColumn> xpathColumns = List.of();
        Map<String, String> xpathMap = Map.of();

        if ("XPATH".equals(extractionType)) {
            xpathColumns = loadXPaths(op.getXpath().getConfig());
            xpathMap = xpathColumns.stream()
                    .collect(Collectors.toMap(XPathColumn::getColumnName, XPathColumn::getXpath,
                            (a, b) -> a, LinkedHashMap::new));
        }

        ExecutorService httpPool = Executors.newFixedThreadPool(op.getHttp().getThreadCount());
        ExecutorService xpathPool = Executors.newFixedThreadPool(op.getXpath().getThreadCount());
        HttpClient httpClient = HttpClient.newBuilder().build();

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();
        List<Long> httpDurationsMs = new CopyOnWriteArrayList<>();
        AtomicLong totalResponseBytes = new AtomicLong();

        String authHeader = authProviders.get(operationType).getAuthorizationHeader();

        long batchStart = System.currentTimeMillis();
        final String resolvedBodyTemplate = resolveJsonataExpression(op.getHttp().getBodyTemplate());

        List<CompletableFuture<Void>> futures;

        if ("JSON".equals(extractionType)) {
            final String jsonataTransform = resolveJsonataExpression(op.getDataExtraction().getJsonataTransform());
            futures = identifiers.stream()
                    .map(id -> processOneJson(id, httpClient, httpPool, jsonataTransform,
                            succeeded, failed, results, httpDurationsMs, totalResponseBytes, authHeader, op, resolvedBodyTemplate))
                    .collect(Collectors.toList());
        } else {
            final Map<String, String> finalXpathMap = Map.copyOf(xpathMap);
            futures = identifiers.stream()
                    .map(id -> processOneXpath(id, httpClient, httpPool, finalXpathMap, xpathPool,
                            succeeded, failed, results, httpDurationsMs, totalResponseBytes, authHeader, op, resolvedBodyTemplate))
                    .collect(Collectors.toList());
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long timeTakenMs = System.currentTimeMillis() - batchStart;
        httpPool.shutdown();
        xpathPool.shutdown();

        LongSummaryStatistics stats = httpDurationsMs.stream()
                .mapToLong(Long::longValue).summaryStatistics();
        HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

        List<ColumnDef> columns = "JSON".equals(extractionType)
                ? buildColumnDefsFromResults(results)
                : buildColumnDefs(xpathColumns);

        String batchUuid = UUID.randomUUID().toString();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

        double responseSizeKb = totalResponseBytes.get() / 1024.0;

        return new BatchResult(identifiers.size(), succeeded.get(), failed.get(),
                httpStats, columns, results, batchUuid, timestamp, timeTakenMs, responseSizeKb);
    }

    // -------------------------------------------------------------------------
    // Per-record processing — XPath extraction
    // -------------------------------------------------------------------------

    private CompletableFuture<Void> processOneXpath(
            String id, HttpClient httpClient, ExecutorService httpPool,
            Map<String, String> xpaths, ExecutorService xpathPool,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes, String authHeader,
            BatchProperties.OperationProperties op, String resolvedBodyTemplate) {

        HttpRequest request = buildRequest(id, authHeader, op, resolvedBodyTemplate);

        return CompletableFuture
                .supplyAsync(() -> {
                    long start = System.currentTimeMillis();
                    try {
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new RuntimeException("HTTP " + response.statusCode()
                                    + " for identifier '" + id + "'");
                        }
                        String body = response.body();
                        totalResponseBytes.addAndGet(body.length());
                        return body;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while calling HTTP endpoint for '" + id + "'", e);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } finally {
                        httpDurationsMs.add(System.currentTimeMillis() - start);
                    }
                }, httpPool)
                .thenCompose(xml -> xpathExtractor.extractAsync(xml, xpaths, xpathPool))
                .thenAccept(attributes -> {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("identifier", id);
                    entry.put("operationStatus", "SUCCESS");
                    entry.putAll(attributes);
                    results.add(entry);
                    succeeded.incrementAndGet();
                })
                .exceptionally(ex -> {
                    failed.incrementAndGet();
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("identifier", id);
                    entry.put("operationStatus", "FAILED");
                    entry.put("errorMessage", message);
                    results.add(entry);
                    return null;
                });
    }

    // -------------------------------------------------------------------------
    // Per-record processing — JSON extraction
    // -------------------------------------------------------------------------

    /**
     * Makes the HTTP call, parses the JSON response, applies an optional JSONata transform,
     * then adds one result row per element in the resulting JSON array.
     * A single identifier can therefore produce zero or more output rows.
     */
    private CompletableFuture<Void> processOneJson(
            String id, HttpClient httpClient, ExecutorService httpPool,
            String jsonataTransform,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes, String authHeader,
            BatchProperties.OperationProperties op, String resolvedBodyTemplate) {

        HttpRequest request = buildRequest(id, authHeader, op, resolvedBodyTemplate);

        return CompletableFuture
                .supplyAsync(() -> {
                    long start = System.currentTimeMillis();
                    try {
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new RuntimeException("HTTP " + response.statusCode()
                                    + " for identifier '" + id + "'");
                        }
                        String body = response.body();
                        totalResponseBytes.addAndGet(body.length());
                        return body;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while calling HTTP endpoint for '" + id + "'", e);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } finally {
                        httpDurationsMs.add(System.currentTimeMillis() - start);
                    }
                }, httpPool)
                .thenAccept(responseBody -> {
                    try {
                        List<Map<String, Object>> rows = extractJson(responseBody, jsonataTransform);
                        for (Map<String, Object> row : rows) {
                            Map<String, Object> entry = new LinkedHashMap<>();
                            entry.put("identifier", id);
                            entry.put("operationStatus", "SUCCESS");
                            entry.putAll(row);
                            results.add(entry);
                        }
                        succeeded.incrementAndGet();
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        Map<String, Object> entry = new LinkedHashMap<>();
                        entry.put("identifier", id);
                        entry.put("operationStatus", "FAILED");
                        entry.put("errorMessage", "JSON extraction failed: " + e.getMessage());
                        results.add(entry);
                    }
                })
                .exceptionally(ex -> {
                    failed.incrementAndGet();
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("identifier", id);
                    entry.put("operationStatus", "FAILED");
                    entry.put("errorMessage", message);
                    results.add(entry);
                    return null;
                });
    }

    // -------------------------------------------------------------------------
    // JSON extraction helper
    // -------------------------------------------------------------------------

    /**
     * Parses {@code responseBody} as JSON, applies an optional JSONata transform,
     * then returns the result as a list of flat key→value maps.
     * A JSON object is wrapped in a single-element list; a JSON array yields one map per element.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractJson(String responseBody,
                                                  String jsonataTransform) throws Exception {
        Object parsed = objectMapper.readValue(responseBody, Object.class);

        if (jsonataTransform != null && !jsonataTransform.isBlank()) {
            parsed = Jsonata.jsonata(jsonataTransform).evaluate(parsed);
        }

        if (parsed instanceof List<?> list) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    ((Map<String, Object>) map).forEach(row::put);
                    result.add(row);
                } else if (item != null) {
                    result.add(Map.of("value", item));
                }
            }
            return result;
        }

        if (parsed instanceof Map<?, ?> map) {
            Map<String, Object> row = new LinkedHashMap<>();
            ((Map<String, Object>) map).forEach(row::put);
            return List.of(row);
        }

        throw new RuntimeException("JSON extraction: expected an array or object, got: "
                + (parsed == null ? "null" : parsed.getClass().getSimpleName()));
    }

    // -------------------------------------------------------------------------
    // Column definition builders
    // -------------------------------------------------------------------------

    private List<ColumnDef> buildColumnDefs(List<XPathColumn> xpathColumns) {
        List<ColumnDef> columns = new ArrayList<>();
        columns.add(new ColumnDef("identifier", "string", "Identifier"));
        columns.add(new ColumnDef("operationStatus", "string", "Operation Status"));
        xpathColumns.forEach(c -> columns.add(new ColumnDef(c.getColumnName(), c.getColumnType(), c.getDisplayName())));
        columns.add(new ColumnDef("errorMessage", "string", "Error Message"));
        return columns;
    }

    /** Derives column definitions dynamically from the first successful result row. */
    private List<ColumnDef> buildColumnDefsFromResults(List<Map<String, Object>> results) {
        SequencedSet<String> keys = new LinkedHashSet<>();
        keys.add("identifier");
        keys.add("operationStatus");
        for (Map<String, Object> row : results) {
            if (!"FAILED".equals(row.get("operationStatus"))) {
                row.keySet().forEach(keys::add);
                break;
            }
        }
        keys.add("errorMessage");
        return keys.stream()
                .map(k -> new ColumnDef(k, "string", toDisplayName(k)))
                .toList();
    }

    private static String toDisplayName(String key) {
        String spaced = key.replaceAll("([A-Z])", " $1").trim();
        return Character.toUpperCase(spaced.charAt(0)) + spaced.substring(1);
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    private HttpRequest buildRequest(String id, String authHeader,
                                     BatchProperties.OperationProperties op,
                                     String resolvedBodyTemplate) {
        BatchProperties.HttpProperties http = op.getHttp();
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        if (http.getMethod().equalsIgnoreCase("GET")) {
            builder.uri(URI.create(http.getUrl().replace("{id}", id))).GET();
        } else {
            builder.uri(URI.create(http.getUrl().replace("{id}", id)))
                   .header("Content-Type", http.getContentType())
                   .POST(HttpRequest.BodyPublishers.ofString(resolvedBodyTemplate.replace("{id}", id)));
        }
        http.getHeader().forEach(builder::header);
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return builder.build();
    }

    /**
     * Resolves a JSONata transform value to the actual expression string.
     * <ul>
     *   <li>{@code classpath:transforms/myop.jsonata} — loaded from the classpath</li>
     *   <li>{@code C:/work/config/myop.jsonata} — loaded from the filesystem</li>
     *   <li>Any other value — used as-is (inline expression)</li>
     * </ul>
     */
    private String resolveJsonataExpression(String value) throws Exception {
        if (value == null || value.isBlank()) return value;
        if (value.startsWith("classpath:")) {
            String resource = value.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                if (is == null) {
                    throw new FileNotFoundException("JSONata file not found on classpath: " + resource);
                }
                return new String(is.readAllBytes()).strip();
            }
        }
        Path path = Path.of(value);
        if (Files.exists(path)) {
            return Files.readString(path).strip();
        }
        return value;
    }

    private List<XPathColumn> loadXPaths(String xpathsConfig) throws Exception {
        InputStream is;
        if (xpathsConfig.startsWith("classpath:")) {
            String resource = xpathsConfig.substring("classpath:".length());
            is = getClass().getClassLoader().getResourceAsStream(resource);
            if (is == null) {
                throw new FileNotFoundException("XPath config not found on classpath: " + resource);
            }
        } else {
            is = new FileInputStream(xpathsConfig);
        }
        try (is) {
            return objectMapper.readValue(is, new TypeReference<List<XPathColumn>>() {});
        }
    }
}
