package com.mycompany.batch.service;

import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.enricher.EnricherConfig;
import com.mycompany.batch.enricher.EnricherService;
import com.mycompany.batch.jsonpath.JsonPathColumn;
import com.mycompany.batch.jsonpath.JsonPathExtractor;
import com.mycompany.batch.model.DataRow;
import com.mycompany.batch.model.FilterRule;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.auth.BasicAuthProvider;
import com.mycompany.batch.auth.DigestAuthProvider;
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
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
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
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class BatchService {

    private final ObjectMapper objectMapper;
    private final XPathExtractor xpathExtractor;
    private final JsonPathExtractor jsonPathExtractor;
    private final CacheFactory cacheFactory;
    private final EnricherService enricherService;
    private final BatchProperties batchProperties;
    private final Map<String, HttpAuthProvider> authProviders = new LinkedHashMap<>();

    public BatchService(ObjectMapper objectMapper, XPathExtractor xpathExtractor,
                        JsonPathExtractor jsonPathExtractor, CacheFactory cacheFactory,
                        EnricherService enricherService, BatchProperties batchProperties) {
        this.objectMapper = objectMapper;
        this.xpathExtractor = xpathExtractor;
        this.jsonPathExtractor = jsonPathExtractor;
        this.cacheFactory = cacheFactory;
        this.enricherService = enricherService;
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
            case "DIGEST" -> {
                if (auth.getDigest().getUrl().isBlank() || auth.getDigest().getUsername().isBlank()
                        || auth.getDigest().getPassword().isBlank()) {
                    throw new IllegalStateException(
                            "DIGEST auth for '" + operationName + "' requires digest.url, digest.username and digest.password");
                }
                yield new DigestAuthProvider(auth.getDigest().getUsername(), auth.getDigest().getPassword(),
                        auth.getDigest().getUrl(), objectMapper);
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

    // Pre-loaded per-activity resources (resolved once per runCore call)
    private record ResolvedActivity(
            BatchProperties.ActivityProperties config,
            String resolvedBodyTemplate,        // for HTTP activities
            Map<String, String> xpathMap,       // XPATH extraction (null otherwise)
            String jsonataTransform,            // JSON/JSONATA extraction (null otherwise)
            List<JsonPathColumn> jsonPathColumns // JSONPATH extraction (null otherwise)
    ) {}

    // -------------------------------------------------------------------------
    // Alias resolution — merges preset fields into the incoming request
    // -------------------------------------------------------------------------

    /**
     * Looks up the alias named {@code request.alias()} in the operation's alias list and
     * returns a new {@link RunRequest} where alias values fill in any fields that are
     * {@code null} / blank / empty in the incoming request.
     * Incoming fields always win. Returns the original request unchanged if no alias is set.
     */
    public RunRequest resolveAlias(RunRequest req) {
        if (req.alias() == null || req.alias().isBlank()) return req;

        BatchProperties.OperationProperties op = batchProperties.getOperation(req.operation());

        BatchProperties.AliasProperties aliasProps = op.getAlias().stream()
                .filter(a -> a.getName().equalsIgnoreCase(req.alias().trim()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown alias '" + req.alias() + "' for operation '" + req.operation()
                                + "'. Available: "
                                + op.getAlias().stream()
                                        .map(BatchProperties.AliasProperties::getName).toList()));

        // Deserialise the alias request map into a RunRequest so we get typed fields
        // (e.g. List<FilterRule> instead of List<Map<String,Object>>).
        // Fields absent from the map are null in the resulting record.
        RunRequest a = objectMapper.convertValue(aliasProps.getRequest(), RunRequest.class);

        return new RunRequest(
                req.operation(),
                mergeStr(req.inputSource(),       a.inputSource()),
                mergeStr(req.inputFilePath(),     a.inputFilePath()),
                mergeStr(req.inputHttpUrl(),      a.inputHttpUrl()),
                mergeList(req.ids(),              a.ids()),
                mergeList(req.raw(),              a.raw()),
                req.inputCount()      != null ? req.inputCount()      : a.inputCount(),
                mergeStr(req.outputData(),        a.outputData()),
                mergeStr(req.outputFilePath(),    a.outputFilePath()),
                req.debugMode()       != null ? req.debugMode()       : a.debugMode(),
                req.httpThreadCount() != null ? req.httpThreadCount() : a.httpThreadCount(),
                req.httpTimeoutMs()   != null ? req.httpTimeoutMs()   : a.httpTimeoutMs(),
                mergeList(req.filterInput(),      a.filterInput()),
                mergeList(req.filterOutput(),     a.filterOutput()),
                mergeStr(req.executionMode(),     a.executionMode()),
                req.alias(),
                mergeStr(req.responseProcessor(), a.responseProcessor()));
    }

    /** Returns {@code a} if non-null and non-blank, otherwise {@code b}. */
    private static String mergeStr(String a, String b) {
        return (a != null && !a.isBlank()) ? a : b;
    }

    /** Returns {@code a} if non-null and non-empty, otherwise {@code b}. */
    private static <T> List<T> mergeList(List<T> a, List<T> b) {
        return (a != null && !a.isEmpty()) ? a : b;
    }

    // -------------------------------------------------------------------------
    // Public API — unified RunRequest entry point
    // -------------------------------------------------------------------------

    public BatchResult run(RunRequest request) throws Exception {
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        List<DataRow> rows = buildInputRows(request);

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;
        if (debugMode == -1) {
            return debugResult(rows);
        }

        applyPreEnricher(rows, op.getEnricher());

        BatchProperties.ResponseProcessorProperties responseProc =
                resolveResponseProcessor(op, request.responseProcessor());
        BatchResult result = runCore(rows, op, operation,
                request.httpThreadCount(), request.httpTimeoutMs(), debugMode, responseProc);

        applyPostEnricher(result.results(), op.getEnricher());

        // Rebuild columns after post-enrichment so enriched attributes appear in the grid
        List<ColumnDef> enrichedColumns = buildColumnDefsFromResults(result.results());
        enrichedColumns = applyColumnTemplate(enrichedColumns, op.getColumnTemplate());
        result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                result.httpStats(), enrichedColumns, result.results(),
                result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb());

        // Apply output filter after execution (and after post-enrichment)
        List<Map<String, Object>> filtered = applyFilterOutput(result.results(), request.filterOutput());
        if (filtered != result.results()) {
            result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                    result.httpStats(), result.columns(), filtered,
                    result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb());
        }
        return result;
    }

    /**
     * Parses the input rows from a {@link RunRequest} and applies {@code filterInput}.
     * Used by the ASYNC WebSocket handler to obtain the row count for the ACK message
     * before kicking off background processing.
     */
    public List<DataRow> buildInputRows(RunRequest request) throws Exception {
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        String inputSourceType = (request.inputSource() != null && !request.inputSource().isBlank())
                ? request.inputSource().trim().toUpperCase()
                : op.getInputSource().getType().trim().toUpperCase();

        List<DataRow> rows = switch (inputSourceType) {
            case "FILE" -> {
                String path = request.inputFilePath();
                if (path == null || path.isBlank()) {
                    throw new IllegalArgumentException("inputFilePath is required for FILE input source");
                }
                if (!Files.exists(Path.of(path))) {
                    throw new IllegalArgumentException("inputFilePath does not exist: " + path);
                }
                yield readDataRowsFromFile(path, request.inputCount(), op);
            }
            case "REQUEST", "HTTPGET", "HTTPPOST" -> {
                if (request.raw() != null && !request.raw().isEmpty()) {
                    yield readDataRowsFromRaw(request.raw(), request.inputCount());
                } else if (request.ids() != null && !request.ids().isEmpty()) {
                    yield readDataRowsFromRequest(request.ids(), request.inputCount());
                } else {
                    throw new IllegalArgumentException(
                            "ids or raw data is required for " + inputSourceType + " input source");
                }
            }
            case "HTTP", "HTTPCONFIG" -> {
                BatchProperties.HttpConfigSourceProperties httpCfg = op.getInputSource().getHttpConfig();
                if (request.inputHttpUrl() != null && !request.inputHttpUrl().isBlank()) {
                    // Request-supplied URL overrides (and is mandatory for HTTPCONFIG from the UI)
                    BatchProperties.HttpConfigSourceProperties override =
                            new BatchProperties.HttpConfigSourceProperties();
                    override.setUrl(request.inputHttpUrl().trim());
                    override.setMethod(httpCfg.getMethod());
                    override.setJsonataTransform(httpCfg.getJsonataTransform());
                    httpCfg = override;
                } else if (httpCfg.getUrl() == null || httpCfg.getUrl().isBlank()) {
                    throw new IllegalArgumentException(
                            "inputHttpUrl is required in the request when inputSource=HTTPCONFIG "
                                    + "and no url is configured in operations.json");
                }
                yield readDataRowsFromHttp(httpCfg, request.inputCount());
            }
            default -> throw new IllegalArgumentException("Unknown inputSource type: " + inputSourceType);
        };

        return applyFilterInput(rows, request.filterInput());
    }

    /**
     * Processes rows asynchronously, streaming each completed result row to {@code rowCallback}
     * as soon as it finishes (after applying {@code filterOutput} per row).
     *
     * <p>The returned {@link CompletableFuture} completes when all rows are done. The
     * {@link BatchResult} it carries has an empty {@code results} list — rows have already
     * been streamed — but carries the full batch metadata (columns, stats, uuid, etc.).
     *
     * <p>Callers should send an ACK to the client <em>before</em> calling this method.
     */
    public CompletableFuture<BatchResult> runAsync(List<DataRow> rows, RunRequest request,
                                                    Consumer<Map<String, Object>> rowCallback) throws Exception {
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        BatchProperties.EnricherProperties enricherProps = op.getEnricher();

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;
        if (debugMode == -1) {
            BatchResult r = debugResult(rows);
            for (Map<String, Object> row : r.results()) rowCallback.accept(row);
            return CompletableFuture.completedFuture(
                    new BatchResult(r.processed(), r.succeeded(), r.failed(),
                            r.httpStats(), r.columns(), List.of(),
                            r.batchUuid(), r.timestamp(), r.timeTakenMs(), r.responseSizeKb()));
        }

        // Pre-enrichment — runs before any activity futures are submitted
        applyPreEnricher(rows, enricherProps);

        // Post-enrichment — wrap the callback so each streamed row is enriched before being sent
        Consumer<Map<String, Object>> effectiveCallback = rowCallback;
        if (enricherProps != null && "post".equalsIgnoreCase(enricherProps.getType())) {
            EnricherConfig cfg = enricherService.loadConfig(enricherProps.getEnhancer());
            Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
            effectiveCallback = row -> {
                enricherService.enrichRow(row, cfg.getData(), datasets);
                rowCallback.accept(row);
            };
        }

        BatchProperties.ResponseProcessorProperties responseProc =
                resolveResponseProcessor(op, request.responseProcessor());
        return runCoreAsync(rows, op, operation,
                request.httpThreadCount(), request.httpTimeoutMs(), debugMode,
                request.filterOutput(), effectiveCallback, responseProc);
    }

    // -------------------------------------------------------------------------
    // Public API — convenience overloads kept for backward compatibility
    // -------------------------------------------------------------------------

    /** File-based run. */
    public BatchResult run(String filePath, Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(filePath, inputCount, op);
        return runCore(rows, op, operation, null, null, 0, null);
    }

    /** Direct list of identifiers — each ID becomes a DataRow with key "id". */
    public BatchResult run(List<String> identifiers, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromRequest(identifiers, null);
        return runCore(rows, op, operation, null, null, 0, null);
    }

    /** Runs from file and writes PSV output; returns summary metadata. */
    public PsvResult runToPsv(String inputFilePath, String outputFilePath,
                              Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(inputFilePath, inputCount, op);
        BatchResult batch = runCore(rows, op, operation, null, null, 0, null);
        writeToPsv(batch, outputFilePath);
        return new PsvResult(batch.processed(), batch.succeeded(), batch.failed(),
                Path.of(outputFilePath).toAbsolutePath().toString(),
                batch.batchUuid(), batch.timestamp());
    }

    // -------------------------------------------------------------------------
    // Input-source helpers
    // -------------------------------------------------------------------------

    /**
     * Reads a delimited file and produces one DataRow per data line.
     *
     * <ul>
     *   <li>First non-blank line = header. Delimiter auto-detected: comma if more than one token,
     *       else pipe.</li>
     *   <li>Mandatory-attribute validation performed against header columns.</li>
     *   <li>Each row contains all header→value pairs plus {@code SEQUENCE_NUMBER} (1-based).</li>
     * </ul>
     */
    public List<DataRow> readDataRowsFromFile(String filePath, Integer inputCount,
                                              BatchProperties.OperationProperties op) throws Exception {
        List<String> allLines = Files.readAllLines(Path.of(filePath));
        if (allLines.isEmpty()) return List.of();

        String headerLine      = allLines.get(0);
        String[] commaTokens   = headerLine.split(",", -1);
        String   delimiter     = commaTokens.length > 1 ? "," : "|";
        String[] headers       = delimiter.equals(",") ? commaTokens
                                  : headerLine.split(Pattern.quote("|"), -1);
        headers = Arrays.stream(headers).map(String::trim).toArray(String[]::new);

        List<String> headerList = Arrays.asList(headers);
        List<String> missing = op.getMandatoryAttributeList().stream()
                .filter(attr -> !headerList.contains(attr)).toList();
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Input file is missing mandatory attribute(s): " + missing
                            + ". Found headers: " + headerList);
        }

        String delimPattern = Pattern.quote(delimiter);
        List<String> dataLines = allLines.stream()
                .skip(1).map(String::trim).filter(s -> !s.isBlank())
                .collect(Collectors.toList());
        if (inputCount != null) {
            dataLines = dataLines.stream().limit(inputCount).collect(Collectors.toList());
        }

        List<DataRow> rows = new ArrayList<>(dataLines.size());
        int seq = 1;
        for (String line : dataLines) {
            String[]  values = line.split(delimPattern, -1);
            DataRow   row    = new DataRow();
            for (int i = 0; i < headers.length; i++) {
                row.getData().put(headers[i], i < values.length ? values[i].trim() : "");
            }
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Converts a list of plain string IDs into DataRows.
     * Each row has key {@code "id"} plus {@code SEQUENCE_NUMBER}.
     */
    public List<DataRow> readDataRowsFromRequest(List<String> ids, Integer inputCount) {
        List<String> list = inputCount != null ? ids.stream().limit(inputCount).toList() : ids;
        List<DataRow> rows = new ArrayList<>(list.size());
        int seq = 1;
        for (String id : list) {
            DataRow row = new DataRow();
            row.getData().put("id", id);
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Converts a list of pre-parsed row maps into DataRows (used when the UI sends a CSV textarea
     * as a {@code raw} JSON array). Each map becomes a DataRow with {@code SEQUENCE_NUMBER} added.
     */
    public List<DataRow> readDataRowsFromRaw(List<Map<String, Object>> rawData, Integer inputCount) {
        List<Map<String, Object>> list = inputCount != null
                ? rawData.stream().limit(inputCount).toList()
                : rawData;
        List<DataRow> rows = new ArrayList<>(list.size());
        int seq = 1;
        for (Map<String, Object> rawRow : list) {
            DataRow row = new DataRow();
            row.getData().putAll(rawRow);
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Calls the configured HTTP endpoint and returns one DataRow per element in the
     * {@code "data"} array of the JSON response. Each row additionally contains {@code SEQUENCE_NUMBER}.
     */
    @SuppressWarnings("unchecked")
    public List<DataRow> readDataRowsFromHttp(BatchProperties.HttpConfigSourceProperties config,
                                              Integer inputCount) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(config.getUrl()));
        if ("POST".equalsIgnoreCase(config.getMethod())) {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.GET();
        }

        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("HTTP input source returned HTTP " + response.statusCode());
        }

        Map<String, Object> parsed = objectMapper.readValue(
                response.body(), new TypeReference<Map<String, Object>>() {});
        Object dataObj = parsed.get("data");
        if (!(dataObj instanceof List<?>)) {
            throw new RuntimeException(
                    "HTTP input source: expected a 'data' array in the JSON response");
        }

        List<Object> dataArray = (List<Object>) dataObj;
        if (inputCount != null) {
            dataArray = dataArray.stream().limit(inputCount).toList();
        }

        List<DataRow> rows = new ArrayList<>(dataArray.size());
        int seq = 1;
        for (Object item : dataArray) {
            if (item instanceof Map<?, ?> m) {
                DataRow row = new DataRow();
                row.getData().putAll((Map<String, Object>) m);
                row.getData().put("SEQUENCE_NUMBER", seq++);
                rows.add(row);
            }
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // File output helper
    // -------------------------------------------------------------------------

    public void writeToPsv(BatchResult batch, String outputFilePath) throws Exception {
        if (batch.results().isEmpty()) {
            Files.writeString(Path.of(outputFilePath), "");
            return;
        }

        SequencedSet<String> columns = new LinkedHashSet<>();
        batch.results().get(0).keySet().forEach(columns::add);
        for (Map<String, Object> row : batch.results()) {
            if (!"FAILED".equals(row.get("operationStatus"))) {
                row.keySet().forEach(columns::add);
                break;
            }
        }
        columns.add("errorMessage");

        Path outputPath = Path.of(outputFilePath);
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            writer.write(String.join("|", columns));
            writer.newLine();
            for (Map<String, Object> row : batch.results()) {
                List<String> line = new ArrayList<>();
                for (String col : columns) {
                    Object val = row.get(col);
                    line.add(val != null ? val.toString() : "");
                }
                writer.write(String.join("|", line));
                writer.newLine();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Debug mode
    // -------------------------------------------------------------------------

    private BatchResult debugResult(List<DataRow> rows) {
        List<Map<String, Object>> results = rows.stream()
                .map(row -> new LinkedHashMap<>(row.getData()))
                .collect(Collectors.toList());

        String batchUuid  = UUID.randomUUID().toString();
        String timestamp  = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        List<ColumnDef> columns = results.isEmpty() ? List.of() : buildColumnDefsFromResults(results);

        return new BatchResult(rows.size(), rows.size(), 0,
                new HttpStats(0, 0, 0), columns, results, batchUuid, timestamp, 0L, 0.0);
    }

    // -------------------------------------------------------------------------
    // Core execution
    // -------------------------------------------------------------------------

    private BatchResult runCore(List<DataRow> rows,
                                BatchProperties.OperationProperties op,
                                String operation,
                                Integer threadCountOverride,
                                Integer timeoutMsOverride,
                                int debugMode,
                                BatchProperties.ResponseProcessorProperties responseProc) throws Exception {

        List<BatchProperties.ActivityProperties> activities =
                op.getActivity() != null ? op.getActivity() : List.of();
        boolean useActivities = !activities.isEmpty();

        AtomicInteger succeeded       = new AtomicInteger();
        AtomicInteger failed          = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();
        List<Long> httpDurationsMs    = new CopyOnWriteArrayList<>();
        AtomicLong totalResponseBytes = new AtomicLong();

        String authHeader = authProviders.get(operation).getAuthorizationHeader();
        long batchStart   = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures;

        if (useActivities) {
            // --- Activity-based path ---

            // Determine effective thread count and timeout from the HTTP activity
            int activityHttpThreads = activities.stream()
                    .filter(a -> "HTTP".equalsIgnoreCase(a.getType()))
                    .findFirst()
                    .map(a -> a.getHttp().getThreadCount())
                    .orElse(op.getHttp().getThreadCount());
            int effectiveThreadCount = threadCountOverride != null ? threadCountOverride : activityHttpThreads;

            int activityTimeoutMs = activities.stream()
                    .filter(a -> "HTTP".equalsIgnoreCase(a.getType()))
                    .findFirst()
                    .map(a -> a.getHttp().getTimeoutMs())
                    .orElse(op.getHttp().getTimeoutMs());
            int effectiveTimeoutMs = timeoutMsOverride != null ? timeoutMsOverride : activityTimeoutMs;

            int xpathThreadCount = activities.stream()
                    .filter(a -> "dataextraction".equalsIgnoreCase(a.getType()))
                    .filter(a -> "XPATH".equalsIgnoreCase(a.getDataExtraction().getType()))
                    .findFirst()
                    .map(a -> a.getDataExtraction().getThreadCount())
                    .orElse(op.getXpath().getThreadCount());

            ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
            ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
            HttpClient httpClient     = HttpClient.newBuilder().build();

            // Pre-load activity resources (classpath files, etc.) once for all rows
            List<ResolvedActivity> resolvedActivities = preloadActivities(activities, effectiveTimeoutMs);

            boolean includeMetadata = debugMode == 1;
            Map<String, String> opProperties = op.getProperties();

            futures = rows.stream()
                    .map(row -> processOneRowWithActivities(
                            row, resolvedActivities, httpClient, httpPool, xpathPool,
                            succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                            authHeader, includeMetadata, opProperties, null, null))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long timeTakenMs = System.currentTimeMillis() - batchStart;
            httpPool.shutdown();
            xpathPool.shutdown();

            LongSummaryStatistics stats = httpDurationsMs.stream().mapToLong(Long::longValue).summaryStatistics();
            HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                    : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

            List<Map<String, Object>> rawResults = applyResponseProcessor(new ArrayList<>(results), responseProc);
            List<Map<String, Object>> sanitizedResults = sanitizeKeys(rawResults);
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());

            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            double responseSizeKb = totalResponseBytes.get() / 1024.0;

            return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb);

        } else {
            // --- Legacy path (no activity array defined) ---

            String extractionType = op.getDataExtraction().getType().trim().toUpperCase();

            List<XPathColumn> xpathColumns = List.of();
            Map<String, String> xpathMap   = Map.of();
            if ("XPATH".equals(extractionType)) {
                xpathColumns = loadXPaths(op.getXpath().getConfig());
                xpathMap = xpathColumns.stream()
                        .collect(Collectors.toMap(XPathColumn::getColumnName, XPathColumn::getXpath,
                                (a, b) -> a, LinkedHashMap::new));
            }

            int effectiveThreadCount = threadCountOverride != null ? threadCountOverride : op.getHttp().getThreadCount();
            int effectiveTimeoutMs   = timeoutMsOverride   != null ? timeoutMsOverride   : op.getHttp().getTimeoutMs();

            ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
            ExecutorService xpathPool = Executors.newFixedThreadPool(op.getXpath().getThreadCount());
            HttpClient httpClient     = HttpClient.newBuilder().build();

            final String resolvedBodyTemplate = resolveJsonataExpression(op.getHttp().getBodyTemplate());

            if ("JSON".equals(extractionType)) {
                final String jsonataTransform = resolveJsonataExpression(op.getDataExtraction().getJsonataTransform());
                futures = rows.stream()
                        .map(row -> processOneJson(row.getData(), httpClient, httpPool, jsonataTransform,
                                succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                                authHeader, op, resolvedBodyTemplate, effectiveTimeoutMs))
                        .collect(Collectors.toList());
            } else {
                final Map<String, String> finalXpathMap = Map.copyOf(xpathMap);
                futures = rows.stream()
                        .map(row -> processOneXpath(row.getData(), httpClient, httpPool, finalXpathMap, xpathPool,
                                succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                                authHeader, op, resolvedBodyTemplate, effectiveTimeoutMs))
                        .collect(Collectors.toList());
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long timeTakenMs = System.currentTimeMillis() - batchStart;
            httpPool.shutdown();
            xpathPool.shutdown();

            LongSummaryStatistics stats = httpDurationsMs.stream().mapToLong(Long::longValue).summaryStatistics();
            HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                    : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

            List<Map<String, Object>> sanitizedResults = sanitizeKeys(new ArrayList<>(results));
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());

            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            double responseSizeKb = totalResponseBytes.get() / 1024.0;

            return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb);
        }
    }

    // -------------------------------------------------------------------------
    // Async core execution (activity-based path only)
    // -------------------------------------------------------------------------

    /**
     * Activity-based async execution. Each completed row is sanitised and streamed to
     * {@code rowCallback} immediately (after per-row {@code filterOutput} check).
     * The returned future resolves with metadata-only {@link BatchResult} (empty results list).
     */
    private CompletableFuture<BatchResult> runCoreAsync(
            List<DataRow> rows,
            BatchProperties.OperationProperties op,
            String operation,
            Integer threadCountOverride,
            Integer timeoutMsOverride,
            int debugMode,
            List<FilterRule> filterOutput,
            Consumer<Map<String, Object>> rowCallback,
            BatchProperties.ResponseProcessorProperties responseProc) throws Exception {

        List<BatchProperties.ActivityProperties> activities =
                op.getActivity() != null ? op.getActivity() : List.of();

        AtomicInteger succeeded       = new AtomicInteger();
        AtomicInteger failed          = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();   // for column-def derivation
        List<Long> httpDurationsMs    = new CopyOnWriteArrayList<>();
        AtomicLong totalResponseBytes = new AtomicLong();

        String authHeader = authProviders.get(operation).getAuthorizationHeader();
        long batchStart   = System.currentTimeMillis();

        int activityHttpThreads = activities.stream()
                .filter(a -> "HTTP".equalsIgnoreCase(a.getType()))
                .findFirst()
                .map(a -> a.getHttp().getThreadCount())
                .orElse(op.getHttp().getThreadCount());
        int effectiveThreadCount = threadCountOverride != null ? threadCountOverride : activityHttpThreads;

        int activityTimeoutMs = activities.stream()
                .filter(a -> "HTTP".equalsIgnoreCase(a.getType()))
                .findFirst()
                .map(a -> a.getHttp().getTimeoutMs())
                .orElse(op.getHttp().getTimeoutMs());
        int effectiveTimeoutMs = timeoutMsOverride != null ? timeoutMsOverride : activityTimeoutMs;

        int xpathThreadCount = activities.stream()
                .filter(a -> "dataextraction".equalsIgnoreCase(a.getType()))
                .filter(a -> "XPATH".equalsIgnoreCase(a.getDataExtraction().getType()))
                .findFirst()
                .map(a -> a.getDataExtraction().getThreadCount())
                .orElse(op.getXpath().getThreadCount());

        final String rpType = responseProc != null ? responseProc.getType().trim().toLowerCase() : "";
        final String rpName = responseProc != null ? responseProc.getName() : "";

        // For aggregation: suppress per-row streaming — rows are aggregated and sent in thenApply.
        // For attribute: parse the attribute's JSON content and stream that as the row.
        final Consumer<Map<String, Object>> effectiveRowCallback;
        if ("aggregation".equals(rpType)) {
            effectiveRowCallback = null;   // suppress individual row streaming
        } else if ("attribute".equals(rpType)) {
            effectiveRowCallback = row -> rowCallback.accept(expandAttributeAsJson(row, rpName));
        } else {
            effectiveRowCallback = rowCallback;
        }

        ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
        ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
        HttpClient httpClient     = HttpClient.newBuilder().build();

        List<ResolvedActivity> resolvedActivities = preloadActivities(activities, effectiveTimeoutMs);

        boolean includeMetadata = debugMode == 1;
        Map<String, String> opProperties = op.getProperties();

        List<CompletableFuture<Void>> futures = rows.stream()
                .map(row -> processOneRowWithActivities(
                        row, resolvedActivities, httpClient, httpPool, xpathPool,
                        succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                        authHeader, includeMetadata, opProperties,
                        filterOutput, effectiveRowCallback))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    long timeTakenMs = System.currentTimeMillis() - batchStart;
                    httpPool.shutdown();
                    xpathPool.shutdown();

                    LongSummaryStatistics stats = httpDurationsMs.stream()
                            .mapToLong(Long::longValue).summaryStatistics();
                    HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                            : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

                    List<Map<String, Object>> sanitizedForCols = sanitizeKeys(
                            applyResponseProcessor(new ArrayList<>(results), responseProc));

                    // Apply post-enrichment so enriched attributes appear in column defs
                    try { applyPostEnricher(sanitizedForCols, op.getEnricher()); } catch (Exception ignored) {}

                    // For aggregation: stream aggregated rows now (individual rows were suppressed above)
                    if ("aggregation".equals(rpType)) {
                        sanitizedForCols.forEach(rowCallback);
                    }

                    List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedForCols);
                    try { columns = applyColumnTemplate(columns, op.getColumnTemplate()); }
                    catch (Exception ignored) {}

                    String batchUuid = UUID.randomUUID().toString();
                    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                    double responseSizeKb = totalResponseBytes.get() / 1024.0;

                    return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                            httpStats, columns, List.of(), batchUuid, timestamp, timeTakenMs, responseSizeKb);
                });
    }

    // -------------------------------------------------------------------------
    // Activity pre-loading
    // -------------------------------------------------------------------------

    /**
     * Pre-loads classpath/filesystem resources for each activity so they are resolved once
     * rather than on every row. Returns one {@link ResolvedActivity} per input activity.
     */
    private List<ResolvedActivity> preloadActivities(
            List<BatchProperties.ActivityProperties> activities,
            int defaultTimeoutMs) throws Exception {
        List<ResolvedActivity> resolved = new ArrayList<>(activities.size());
        for (BatchProperties.ActivityProperties act : activities) {
            String type = act.getType() == null ? "" : act.getType().trim();
            if ("HTTP".equalsIgnoreCase(type)) {
                String bodyTemplate = resolveJsonataExpression(act.getHttp().getBodyTemplate());
                resolved.add(new ResolvedActivity(act, bodyTemplate, null, null, null));
            } else if ("dataextraction".equalsIgnoreCase(type)) {
                String extractType = act.getDataExtraction().getType().trim().toUpperCase();
                if ("XPATH".equals(extractType)) {
                    String config = act.getDataExtraction().getConfig();
                    if (config == null || config.isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "': dataExtraction.config is required for XPATH extraction");
                    }
                    Map<String, String> xpathMap = loadXPathMap(config);
                    resolved.add(new ResolvedActivity(act, null, xpathMap, null, null));
                } else if ("JSONPATH".equals(extractType)) {
                    String config = act.getDataExtraction().getConfig();
                    if (config == null || config.isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "': dataExtraction.config is required for JSONPATH extraction");
                    }
                    List<JsonPathColumn> cols = loadJsonPathColumns(config);
                    resolved.add(new ResolvedActivity(act, null, null, null, cols));
                } else {
                    // JSON or JSONATA — JSONata transform
                    String transform = resolveJsonataExpression(act.getDataExtraction().getJsonataTransform());
                    resolved.add(new ResolvedActivity(act, null, null, transform, null));
                }
            } else {
                resolved.add(new ResolvedActivity(act, null, null, null, null));
            }
        }
        return resolved;
    }

    // -------------------------------------------------------------------------
    // Activity-based per-row processing
    // -------------------------------------------------------------------------

    private CompletableFuture<Void> processOneRowWithActivities(
            DataRow inputRow,
            List<ResolvedActivity> activities,
            HttpClient httpClient,
            ExecutorService httpPool,
            ExecutorService xpathPool,
            AtomicInteger succeeded, AtomicInteger failed,
            List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader,
            boolean includeMetadata,
            Map<String, String> opProperties,
            List<FilterRule> filterOutput,          // nullable — only used when rowCallback != null
            Consumer<Map<String, Object>> rowCallback) { // nullable — ASYNC streaming

        // Chain activities as CompletableFuture stages
        CompletableFuture<DataRow> chain = CompletableFuture.completedFuture(inputRow);

        for (ResolvedActivity activity : activities) {
            String type = activity.config().getType() == null ? "" : activity.config().getType().trim();
            if ("HTTP".equalsIgnoreCase(type)) {
                chain = chain.thenCompose(row -> executeHttpActivity(
                        row, activity, httpClient, httpPool,
                        httpDurationsMs, totalResponseBytes, authHeader, opProperties));
            } else if ("dataextraction".equalsIgnoreCase(type)) {
                chain = chain.thenCompose(row -> executeExtractionActivity(row, activity, xpathPool));
            }
        }

        return chain.thenAccept(row -> {
            List<Map<String, Object>> expandedRows = row.getExpandedRows();
            if (expandedRows != null && !expandedRows.isEmpty()) {
                // JSON extraction produced multiple output rows — expand them
                for (Map<String, Object> expandedData : expandedRows) {
                    Map<String, Object> resultMap = row.toResponseMap(includeMetadata);
                    resultMap.putAll(expandedData);
                    resultMap.put("operationStatus", "SUCCESS");
                    results.add(resultMap);
                    if (rowCallback != null) {
                        Map<String, Object> sanitized = sanitizeRow(resultMap);
                        if (matchesAll(sanitized, filterOutput != null ? filterOutput : List.of())) {
                            rowCallback.accept(sanitized);
                        }
                    }
                }
            } else {
                Map<String, Object> resultMap = row.toResponseMap(includeMetadata);
                resultMap.put("operationStatus", "SUCCESS");
                results.add(resultMap);
                if (rowCallback != null) {
                    Map<String, Object> sanitized = sanitizeRow(resultMap);
                    if (matchesAll(sanitized, filterOutput != null ? filterOutput : List.of())) {
                        rowCallback.accept(sanitized);
                    }
                }
            }
            succeeded.incrementAndGet();
        }).exceptionally(ex -> {
            failed.incrementAndGet();
            Throwable cause   = ex.getCause() != null ? ex.getCause() : ex;
            String    message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
            // Use input row data only (no metadata) for FAILED rows
            Map<String, Object> resultMap = new LinkedHashMap<>(inputRow.getData());
            resultMap.put("operationStatus", "FAILED");
            resultMap.put("errorMessage", message);
            results.add(resultMap);
            if (rowCallback != null) {
                rowCallback.accept(sanitizeRow(resultMap)); // stream FAILED rows too
            }
            return null;
        });
    }

    /** Returns a copy of {@code row} with every key's {@code '.'} replaced by {@code '_'}. */
    private static Map<String, Object> sanitizeRow(Map<String, Object> row) {
        Map<String, Object> sanitized = new LinkedHashMap<>();
        row.forEach((k, v) -> sanitized.put(k.replace('.', '_'), v));
        return sanitized;
    }

    /** Makes the HTTP call for one DataRow, captures timing + URL in metadata, stores response body. */
    private CompletableFuture<DataRow> executeHttpActivity(
            DataRow row,
            ResolvedActivity activity,
            HttpClient httpClient,
            ExecutorService httpPool,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader,
            Map<String, String> opProperties) {

        BatchProperties.HttpProperties httpConfig = activity.config().getHttp();
        String resolvedUrl;
        String resolvedBody;
        try {
            resolvedUrl  = resolveTemplate(httpConfig.getUrl(), row.getData(), opProperties);
            resolvedBody = activity.resolvedBodyTemplate() != null
                    ? resolveTemplate(activity.resolvedBodyTemplate(), row.getData(), opProperties)
                    : "";
        } catch (IllegalArgumentException e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        // --- Cache check (synchronous, fast path) ---
        String activityName = activity.config().getName();
        BatchProperties.CacheProperties cacheConfig = httpConfig.getCache();
        String cacheName = null;
        String resolvedCacheKey = null;
        if (cacheConfig != null && !cacheConfig.getName().isBlank()) {
            try {
                cacheName        = cacheConfig.getName();
                resolvedCacheKey = resolveTemplate(cacheConfig.getKey(), row.getData(), opProperties);
                String cached    = cacheFactory.get(cacheName, resolvedCacheKey);
                if (cached != null) {
                    totalResponseBytes.addAndGet(cached.length());
                    row.setResponseBody(cached);
                    httpDurationsMs.add(0L);
                    row.getMetadata().put(activityName + ".timetakenmillis", 0L);
                    row.getMetadata().put(activityName + ".httpurl", resolvedUrl + " [CACHED]");
                    return CompletableFuture.completedFuture(row);
                }
            } catch (IllegalArgumentException e) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        }

        HttpRequest request = buildRequestFromHttpConfig(resolvedUrl, authHeader, httpConfig, resolvedBody);

        final String finalCacheName       = cacheName;
        final String finalResolvedCacheKey = resolvedCacheKey;
        final String finalResolvedUrl      = resolvedUrl;

        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    throw new RuntimeException("HTTP " + response.statusCode());
                }
                String body = response.body();
                totalResponseBytes.addAndGet(body.length());
                if (finalCacheName != null) {
                    cacheFactory.save(finalCacheName, finalResolvedCacheKey, body, finalResolvedUrl);
                }
                row.setResponseBody(body);
                return row;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while calling HTTP endpoint", e);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                long elapsed = System.currentTimeMillis() - start;
                httpDurationsMs.add(elapsed);
                row.getMetadata().put(activityName + ".timetakenmillis", elapsed);
                row.getMetadata().put(activityName + ".httpurl", finalResolvedUrl);
            }
        }, httpPool);
    }

    /** Runs extraction (XPATH or JSON) against the response body stored in the DataRow. */
    private CompletableFuture<DataRow> executeExtractionActivity(
            DataRow row, ResolvedActivity activity, ExecutorService xpathPool) {

        String responseBody = row.getResponseBody();
        if (responseBody == null) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException(
                    "Activity '" + activity.config().getName() + "' requires an HTTP response body "
                            + "from a preceding HTTP activity"));
            return f;
        }

        String extractionType = activity.config().getDataExtraction().getType().trim().toUpperCase();

        if ("XPATH".equals(extractionType)) {
            return xpathExtractor.extractAsync(responseBody, activity.xpathMap(), xpathPool)
                    .thenApply(attributes -> {
                        row.getData().putAll(attributes);
                        return row;
                    });
        }

        if ("JSON".equals(extractionType) || "JSONATA".equals(extractionType)) {
            try {
                List<Map<String, Object>> extracted = extractJson(responseBody, activity.jsonataTransform());
                if (extracted.size() <= 1) {
                    if (!extracted.isEmpty()) {
                        row.getData().putAll(extracted.get(0));
                    }
                } else {
                    // Multiple output rows — store for expansion in the outer thenAccept
                    row.setExpandedRows(extracted);
                }
                return CompletableFuture.completedFuture(row);
            } catch (Exception e) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(new RuntimeException("JSON/JSONATA extraction failed: " + e.getMessage(), e));
                return f;
            }
        }

        if ("JSONPATH".equals(extractionType)) {
            try {
                Map<String, String> extracted = jsonPathExtractor.extract(responseBody, activity.jsonPathColumns());
                row.getData().putAll(extracted);
                return CompletableFuture.completedFuture(row);
            } catch (Exception e) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(new RuntimeException("JSONPATH extraction failed: " + e.getMessage(), e));
                return f;
            }
        }

        CompletableFuture<DataRow> f = new CompletableFuture<>();
        f.completeExceptionally(new IllegalArgumentException(
                "Unknown extraction type: " + activity.config().getDataExtraction().getType()));
        return f;
    }

    // -------------------------------------------------------------------------
    // Legacy per-record processing (used when no activity array is defined)
    // -------------------------------------------------------------------------

    private CompletableFuture<Void> processOneXpath(
            Map<String, Object> row, HttpClient httpClient, ExecutorService httpPool,
            Map<String, String> xpaths, ExecutorService xpathPool,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes, String authHeader,
            BatchProperties.OperationProperties op, String resolvedBodyTemplate, int timeoutMs) {

        HttpRequest request;
        try {
            request = buildRequest(row, authHeader, op, resolvedBodyTemplate, timeoutMs);
        } catch (IllegalArgumentException e) {
            failed.incrementAndGet();
            Map<String, Object> entry = new LinkedHashMap<>(row);
            entry.put("operationStatus", "FAILED");
            entry.put("errorMessage", e.getMessage());
            results.add(entry);
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture
                .supplyAsync(() -> {
                    long start = System.currentTimeMillis();
                    try {
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new RuntimeException("HTTP " + response.statusCode());
                        }
                        String body = response.body();
                        totalResponseBytes.addAndGet(body.length());
                        return body;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while calling HTTP endpoint", e);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } finally {
                        httpDurationsMs.add(System.currentTimeMillis() - start);
                    }
                }, httpPool)
                .thenCompose(xml -> xpathExtractor.extractAsync(xml, xpaths, xpathPool))
                .thenAccept(attributes -> {
                    Map<String, Object> entry = new LinkedHashMap<>(row);
                    entry.put("operationStatus", "SUCCESS");
                    entry.putAll(attributes);
                    results.add(entry);
                    succeeded.incrementAndGet();
                })
                .exceptionally(ex -> {
                    failed.incrementAndGet();
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, Object> entry = new LinkedHashMap<>(row);
                    entry.put("operationStatus", "FAILED");
                    entry.put("errorMessage", message);
                    results.add(entry);
                    return null;
                });
    }

    private CompletableFuture<Void> processOneJson(
            Map<String, Object> row, HttpClient httpClient, ExecutorService httpPool,
            String jsonataTransform,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes, String authHeader,
            BatchProperties.OperationProperties op, String resolvedBodyTemplate, int timeoutMs) {

        HttpRequest request;
        try {
            request = buildRequest(row, authHeader, op, resolvedBodyTemplate, timeoutMs);
        } catch (IllegalArgumentException e) {
            failed.incrementAndGet();
            Map<String, Object> entry = new LinkedHashMap<>(row);
            entry.put("operationStatus", "FAILED");
            entry.put("errorMessage", e.getMessage());
            results.add(entry);
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture
                .supplyAsync(() -> {
                    long start = System.currentTimeMillis();
                    try {
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new RuntimeException("HTTP " + response.statusCode());
                        }
                        String body = response.body();
                        totalResponseBytes.addAndGet(body.length());
                        return body;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while calling HTTP endpoint", e);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    } finally {
                        httpDurationsMs.add(System.currentTimeMillis() - start);
                    }
                }, httpPool)
                .thenAccept(responseBody -> {
                    try {
                        List<Map<String, Object>> extractedRows = extractJson(responseBody, jsonataTransform);
                        for (Map<String, Object> extracted : extractedRows) {
                            Map<String, Object> entry = new LinkedHashMap<>(row);
                            entry.put("operationStatus", "SUCCESS");
                            entry.putAll(extracted);
                            results.add(entry);
                        }
                        succeeded.incrementAndGet();
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        Map<String, Object> entry = new LinkedHashMap<>(row);
                        entry.put("operationStatus", "FAILED");
                        entry.put("errorMessage", "JSON extraction failed: " + e.getMessage());
                        results.add(entry);
                    }
                })
                .exceptionally(ex -> {
                    failed.incrementAndGet();
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, Object> entry = new LinkedHashMap<>(row);
                    entry.put("operationStatus", "FAILED");
                    entry.put("errorMessage", message);
                    results.add(entry);
                    return null;
                });
    }

    // -------------------------------------------------------------------------
    // JSON extraction helper
    // -------------------------------------------------------------------------

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
                    Map<String, Object> r = new LinkedHashMap<>();
                    ((Map<String, Object>) map).forEach(r::put);
                    result.add(r);
                } else if (item != null) {
                    result.add(Map.of("value", item));
                }
            }
            return result;
        }
        if (parsed instanceof Map<?, ?> map) {
            Map<String, Object> r = new LinkedHashMap<>();
            ((Map<String, Object>) map).forEach(r::put);
            return List.of(r);
        }
        throw new RuntimeException("JSON extraction: expected an array or object, got: "
                + (parsed == null ? "null" : parsed.getClass().getSimpleName()));
    }

    // -------------------------------------------------------------------------
    // Column definition builders
    // -------------------------------------------------------------------------

    private List<ColumnDef> buildColumnDefsFromResults(List<Map<String, Object>> results) {
        if (results.isEmpty()) return List.of();
        SequencedSet<String> keys = new LinkedHashSet<>();
        results.get(0).keySet().forEach(keys::add);
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

    /**
     * Reorders/filters the derived column list according to the operation's {@code columnTemplate}.
     * If no template is configured the original list is returned unchanged.
     */
    private List<ColumnDef> applyColumnTemplate(List<ColumnDef> columns,
                                                BatchProperties.ColumnTemplateProperties template) throws Exception {
        if (template == null || template.getSource() == null || template.getSource().isBlank()) {
            return columns;
        }
        List<String> orderedNames = loadColumnNames(template.getSource());
        if (orderedNames.isEmpty()) return columns;

        Map<String, ColumnDef> defMap = columns.stream()
                .collect(Collectors.toMap(ColumnDef::columnName, c -> c, (a, b) -> a, LinkedHashMap::new));

        return orderedNames.stream()
                .filter(defMap::containsKey) // only include columns that actually exist in results
                .map(defMap::get)
                .collect(Collectors.toList());
    }

    /**
     * Loads a column name list from:
     * <ul>
     *   <li>{@code classpath:...} — one column name per line from a classpath resource</li>
     *   <li>A filesystem path — one column name per line from a file</li>
     *   <li>Anything else — treated as a comma-separated inline list</li>
     * </ul>
     */
    private List<String> loadColumnNames(String source) throws Exception {
        if (source.startsWith("classpath:")) {
            String resource = source.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                if (is == null) throw new FileNotFoundException("Column template not found on classpath: " + resource);
                return Arrays.stream(new String(is.readAllBytes()).split("\\r?\\n"))
                        .map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList());
            }
        }
        Path path = Path.of(source);
        if (Files.exists(path)) {
            return Files.readAllLines(path).stream()
                    .map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList());
        }
        return Arrays.stream(source.split(","))
                .map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList());
    }

    private static String toDisplayName(String key) {
        if (key.equals(key.toUpperCase())) return key;
        String spaced = key.replaceAll("([A-Z])", " $1").trim();
        return Character.toUpperCase(spaced.charAt(0)) + spaced.substring(1);
    }

    // -------------------------------------------------------------------------
    // Shared HTTP request builders
    // -------------------------------------------------------------------------

    /** Legacy request builder for the non-activity path. */
    private HttpRequest buildRequest(Map<String, Object> row, String authHeader,
                                     BatchProperties.OperationProperties op,
                                     String resolvedBodyTemplate, int timeoutMs) {
        BatchProperties.HttpProperties http = op.getHttp();
        String resolvedUrl  = resolveTemplate(http.getUrl(), row);
        String resolvedBody = resolvedBodyTemplate != null ? resolveTemplate(resolvedBodyTemplate, row) : "";
        return buildRequestFromHttpConfig(resolvedUrl, authHeader, http, resolvedBody, timeoutMs,
                http.getTimeoutMs());
    }

    /** Builds an {@link HttpRequest} from a pre-resolved URL + body. */
    private HttpRequest buildRequestFromHttpConfig(
            String resolvedUrl, String authHeader,
            BatchProperties.HttpProperties http, String resolvedBody) {
        return buildRequestFromHttpConfig(resolvedUrl, authHeader, http, resolvedBody,
                http.getTimeoutMs(), http.getTimeoutMs());
    }

    private HttpRequest buildRequestFromHttpConfig(
            String resolvedUrl, String authHeader,
            BatchProperties.HttpProperties http, String resolvedBody,
            int timeoutMs, int ignored) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .timeout(Duration.ofMillis(timeoutMs))
                .uri(URI.create(resolvedUrl));

        if (http.getMethod().equalsIgnoreCase("GET")) {
            builder.GET();
        } else {
            builder.header("Content-Type", http.getContentType())
                   .POST(HttpRequest.BodyPublishers.ofString(resolvedBody != null ? resolvedBody : ""));
        }
        http.getHeader().forEach(builder::header);
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return builder.build();
    }

    // -------------------------------------------------------------------------
    // Enricher helpers
    // -------------------------------------------------------------------------

    private void applyPreEnricher(List<DataRow> rows,
                                   BatchProperties.EnricherProperties props) throws Exception {
        if (props == null || !"pre".equalsIgnoreCase(props.getType())) return;
        EnricherConfig cfg = enricherService.loadConfig(props.getEnhancer());
        Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
        rows.forEach(row -> enricherService.enrichRow(row.getData(), cfg.getData(), datasets));
    }

    private void applyPostEnricher(List<Map<String, Object>> results,
                                    BatchProperties.EnricherProperties props) throws Exception {
        if (props == null || !"post".equalsIgnoreCase(props.getType())) return;
        EnricherConfig cfg = enricherService.loadConfig(props.getEnhancer());
        Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
        results.forEach(row -> enricherService.enrichRow(row, cfg.getData(), datasets));
    }

    // -------------------------------------------------------------------------
    // Response processor
    // -------------------------------------------------------------------------

    /**
     * Parses the value of {@code attrName} in {@code row} as a JSON string.
     * If the value is a valid JSON object its fields become the result row directly.
     * Otherwise falls back to returning {@code {attrName: value}}.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> expandAttributeAsJson(Map<String, Object> row, String attrName) {
        Object val = row.get(attrName);
        String json = val != null ? val.toString().trim() : "";
        try {
            Object parsed = objectMapper.readValue(json, Object.class);
            if (parsed instanceof Map<?, ?> map) {
                Map<String, Object> out = new LinkedHashMap<>();
                ((Map<String, Object>) map).forEach(out::put);
                return out;
            }
        } catch (Exception ignored) {}
        Map<String, Object> fallback = new LinkedHashMap<>();
        fallback.put(attrName, val);
        return fallback;
    }

    private static BatchProperties.ResponseProcessorProperties resolveResponseProcessor(
            BatchProperties.OperationProperties op, String name) {
        if (name == null || name.isBlank() || op.getResponseProcessor().isEmpty()) return null;
        return op.getResponseProcessor().stream()
                .filter(e -> name.trim().equalsIgnoreCase(e.getName()))
                .findFirst()
                .map(BatchProperties.ResponseProcessorEntryProperties::getResponseProcessor)
                .orElse(null);
    }

    /**
     * Applies the optional response-processor activity to the full result set.
     * <ul>
     *   <li>{@code attribute} — each row is reduced to just the named attribute value
     *       (plus {@code operationStatus} / {@code errorMessage}).</li>
     *   <li>{@code aggregation} — rows are grouped by the named attribute; all other
     *       string-valued attributes are concatenated with {@code ,}.</li>
     * </ul>
     * Returns the original list unchanged when {@code proc} is {@code null}.
     */
    private List<Map<String, Object>> applyResponseProcessor(
            List<Map<String, Object>> rows,
            BatchProperties.ResponseProcessorProperties proc) {

        if (proc == null) return rows;
        String rpType = proc.getType().trim().toLowerCase();
        String rpName = proc.getName();

        if ("attribute".equals(rpType)) {
            return rows.stream()
                    .map(row -> expandAttributeAsJson(row, rpName))
                    .collect(Collectors.toList());
        }

        if ("aggregation".equals(rpType)) {
            Map<String, Map<String, Object>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> row : rows) {
                Object keyVal = row.get(rpName);
                String keyStr = keyVal != null ? keyVal.toString() : "(null)";
                grouped.compute(keyStr, (k, agg) -> {
                    if (agg == null) return new LinkedHashMap<>(row);
                    for (Map.Entry<String, Object> e : row.entrySet()) {
                        if (e.getKey().equals(rpName) || e.getKey().equals("operationStatus")
                                || e.getKey().equals("errorMessage")) continue;
                        Object existing = agg.get(e.getKey());
                        Object incoming = e.getValue();
                        if (incoming == null) continue;
                        agg.put(e.getKey(), existing == null ? incoming
                                : existing.toString() + "," + incoming);
                    }
                    return agg;
                });
            }
            return new ArrayList<>(grouped.values());
        }

        return rows;
    }

    // -------------------------------------------------------------------------
    // Filter helpers
    // -------------------------------------------------------------------------

    /**
     * Drops DataRows that do not match every rule in {@code filters}.
     * If {@code filters} is null or empty the original list is returned unchanged.
     * Rules whose {@code column} is absent from the row are skipped (row passes through).
     */
    private static List<DataRow> applyFilterInput(List<DataRow> rows, List<FilterRule> filters) {
        if (filters == null || filters.isEmpty()) return rows;
        return rows.stream()
                .filter(row -> matchesAll(row.getData(), filters))
                .collect(Collectors.toList());
    }

    /**
     * Drops result maps that do not match every rule in {@code filters}.
     * If {@code filters} is null or empty the original list is returned unchanged.
     */
    private static List<Map<String, Object>> applyFilterOutput(List<Map<String, Object>> results,
                                                               List<FilterRule> filters) {
        if (filters == null || filters.isEmpty()) return results;
        return results.stream()
                .filter(row -> matchesAll(row, filters))
                .collect(Collectors.toList());
    }

    private static boolean matchesAll(Map<String, Object> data, List<FilterRule> filters) {
        for (FilterRule f : filters) {
            if (f.getColumn() == null || !data.containsKey(f.getColumn())) continue;
            String rowValue    = Objects.toString(data.get(f.getColumn()), "");
            String filterValue = f.getValue() != null ? f.getValue() : "";
            String op          = f.getOperation() != null ? f.getOperation().toLowerCase() : "eq";
            boolean matches = switch (op) {
                case "like" -> rowValue.matches(filterValue);
                default     -> rowValue.equals(filterValue);
            };
            if (!matches) return false;
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Key sanitisation (dot → underscore in result map keys)
    // -------------------------------------------------------------------------

    /**
     * Returns a new list where every key in every result map has {@code '.'} replaced by {@code '_'}.
     * Applied in {@link #runCore} so that column defs are built from the sanitised keys.
     */
    private static List<Map<String, Object>> sanitizeKeys(List<Map<String, Object>> results) {
        List<Map<String, Object>> sanitized = new ArrayList<>(results.size());
        for (Map<String, Object> row : results) {
            Map<String, Object> newRow = new LinkedHashMap<>();
            row.forEach((k, v) -> newRow.put(k.replace('.', '_'), v));
            sanitized.add(newRow);
        }
        return sanitized;
    }

    /**
     * Replaces all {@code {key}} placeholders in {@code template} with values from {@code row}.
     * If a key is not found in the row, {@code fallback} (operation-level properties) is consulted.
     *
     * @throws IllegalArgumentException if any placeholder key is absent from both sources.
     */
    static String resolveTemplate(String template, Map<String, Object> row,
                                  Map<String, String> fallback) {
        if (template == null || template.isBlank()) return template;
        Matcher m = Pattern.compile("\\{([^}]+)\\}").matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            Object val = row.get(key);
            if (val == null && fallback != null) val = fallback.get(key);
            if (val == null) {
                throw new IllegalArgumentException(
                        "Template references key '" + key + "' which is not present in the DataRow"
                                + (fallback != null && !fallback.isEmpty()
                                        ? " or operation properties" : "")
                                + ". Available row keys: " + row.keySet()
                                + (fallback != null && !fallback.isEmpty()
                                        ? ", properties: " + fallback.keySet() : ""));
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(val.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** Convenience overload — no operation-level property fallback. */
    static String resolveTemplate(String template, Map<String, Object> row) {
        return resolveTemplate(template, row, null);
    }

    // -------------------------------------------------------------------------
    // Resource loaders
    // -------------------------------------------------------------------------

    private String resolveJsonataExpression(String value) throws Exception {
        if (value == null || value.isBlank()) return value;
        if (value.startsWith("classpath:")) {
            String resource = value.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                if (is == null) throw new FileNotFoundException("JSONata file not found on classpath: " + resource);
                return new String(is.readAllBytes()).strip();
            }
        }
        Path path = Path.of(value);
        if (Files.exists(path)) return Files.readString(path).strip();
        return value;
    }

    private List<XPathColumn> loadXPaths(String xpathsConfig) throws Exception {
        InputStream is;
        if (xpathsConfig.startsWith("classpath:")) {
            String resource = xpathsConfig.substring("classpath:".length());
            is = getClass().getClassLoader().getResourceAsStream(resource);
            if (is == null) throw new FileNotFoundException("XPath config not found on classpath: " + resource);
        } else {
            is = new FileInputStream(xpathsConfig);
        }
        try (is) {
            return objectMapper.readValue(is, new TypeReference<List<XPathColumn>>() {});
        }
    }

    /** Loads a JSONPATH column config file (classpath or filesystem). */
    private List<JsonPathColumn> loadJsonPathColumns(String config) throws Exception {
        InputStream is;
        if (config.startsWith("classpath:")) {
            String resource = config.substring("classpath:".length());
            is = getClass().getClassLoader().getResourceAsStream(resource);
            if (is == null) throw new FileNotFoundException("JSONPATH config not found on classpath: " + resource);
        } else {
            is = new java.io.FileInputStream(config);
        }
        try (is) {
            return objectMapper.readValue(is, new TypeReference<List<JsonPathColumn>>() {});
        }
    }

    /** Loads XPath columns and returns them as a {@code columnName → xpath} map. */
    private Map<String, String> loadXPathMap(String xpathsConfig) throws Exception {
        return loadXPaths(xpathsConfig).stream()
                .collect(Collectors.toMap(XPathColumn::getColumnName, XPathColumn::getXpath,
                        (a, b) -> a, LinkedHashMap::new));
    }
}
