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
import com.mycompany.batch.config.ServerPropertiesLoader;
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
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final Map<String, HttpAuthProvider> authProviders = new LinkedHashMap<>();

    @org.springframework.beans.factory.annotation.Value("${server.port:8080}")
    private int serverPort;

    public BatchService(ObjectMapper objectMapper, XPathExtractor xpathExtractor,
                        JsonPathExtractor jsonPathExtractor, CacheFactory cacheFactory,
                        EnricherService enricherService, BatchProperties batchProperties,
                        ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.xpathExtractor = xpathExtractor;
        this.jsonPathExtractor = jsonPathExtractor;
        this.cacheFactory = cacheFactory;
        this.enricherService = enricherService;
        this.batchProperties = batchProperties;
        this.serverPropertiesLoader = serverPropertiesLoader;
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
                              double responseSizeKb,
                              Map<String, String> operationProperties) {}
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

        // When inputSource=ALIAS treat it as blank so the alias's inputSource wins
        String effectiveInputSource = "ALIAS".equalsIgnoreCase(req.inputSource()) ? null : req.inputSource();

        return new RunRequest(
                req.operation(),
                mergeStr(effectiveInputSource,        a.inputSource()),
                mergeStr(req.inputFilePath(),         a.inputFilePath()),
                mergeStr(req.inputHttpUrl(),          a.inputHttpUrl()),
                mergeList(req.ids(),                  a.ids()),
                mergeList(req.raw(),                  a.raw()),
                req.inputCount()      != null ? req.inputCount()      : a.inputCount(),
                mergeStr(req.outputData(),            a.outputData()),
                mergeStr(req.outputFilePath(),        a.outputFilePath()),
                req.debugMode()       != null ? req.debugMode()       : a.debugMode(),
                req.httpThreadCount() != null ? req.httpThreadCount() : a.httpThreadCount(),
                req.httpTimeoutMs()   != null ? req.httpTimeoutMs()   : a.httpTimeoutMs(),
                mergeList(req.filterInput(),          a.filterInput()),
                mergeList(req.filterOutput(),         a.filterOutput()),
                mergeStr(req.executionMode(),         a.executionMode()),
                req.alias(),
                mergeStr(req.responseProcessor(),     a.responseProcessor()),
                req.appendOutput() != null ? req.appendOutput() : a.appendOutput(),
                mergeStr(req.inputJsonPath(),         a.inputJsonPath()),
                mergeMap(req.properties(),            a.properties()));
    }

    /** Returns {@code a} if non-null and non-blank, otherwise {@code b}. */
    private static String mergeStr(String a, String b) {
        return (a != null && !a.isBlank()) ? a : b;
    }

    /** Returns {@code a} if non-null and non-empty, otherwise {@code b}. */
    private static <T> List<T> mergeList(List<T> a, List<T> b) {
        return (a != null && !a.isEmpty()) ? a : b;
    }

    /** Returns merge of {@code b} overlaid by {@code a}; null-safe. */
    private static Map<String, String> mergeMap(Map<String, String> a, Map<String, String> b) {
        if (a == null || a.isEmpty()) return b;
        if (b == null || b.isEmpty()) return a;
        Map<String, String> merged = new LinkedHashMap<>(b);
        merged.putAll(a);
        return merged;
    }

    // -------------------------------------------------------------------------
    // Public API — unified RunRequest entry point
    // -------------------------------------------------------------------------

    public BatchResult run(RunRequest request) throws Exception {
        if ("ALIAS".equalsIgnoreCase(request.inputSource()) && (request.alias() == null || request.alias().isBlank()))
            throw new IllegalArgumentException("alias is required when inputSource=ALIAS");
        String rawAlias = request.alias();
        RunRequest rawRequest = request;
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        if (rawAlias == null || rawAlias.isBlank()) checkMandatoryProperties(request, op);

        Map<String, String> opProperties = loadOperationProperties(op, request, rawRequest);

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;

        // Mode 1: return properties as key/value data rows
        if (debugMode == 1) return buildPropertiesResult(opProperties);

        List<DataRow> rows = buildInputRows(request, opProperties);

        applyPreEnricher(rows, op.getEnricher());
        rows = applyFilterInput(rows, request.filterInput());
        if (request.inputCount() != null && request.inputCount() > 0)
            rows = rows.stream().limit(request.inputCount()).collect(Collectors.toList());
        checkMandatoryAttributes(rows, op);

        // Mode 2: return after input source + pre-enricher (no activities)
        if (debugMode == 2) return debugResult(rows);

        BatchProperties.ResponseProcessorProperties responseProc =
                resolveResponseProcessor(op, request.responseProcessor());
        BatchResult result = runCore(rows, op, operation,
                request.httpThreadCount(), request.httpTimeoutMs(), debugMode, responseProc, opProperties);

        // Mode 3: return after activities, skip post-enricher and responseProcessor
        if (debugMode == 3) return result;

        applyPostEnricher(result.results(), op.getEnricher());

        // Rebuild columns after post-enrichment so enriched attributes appear in the grid
        List<ColumnDef> enrichedColumns = buildColumnDefsFromResults(result.results());
        enrichedColumns = applyColumnTemplate(enrichedColumns, op.getColumnTemplate());
        result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                result.httpStats(), enrichedColumns, result.results(),
                result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb(), null);

        // Mode 4: return after post-enrichment, responseProcessor already skipped inside runCore
        // Mode 0: full pipeline — apply output filter
        List<Map<String, Object>> filtered = applyFilterOutput(result.results(), request.filterOutput());
        if (filtered != result.results()) {
            result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                    result.httpStats(), result.columns(), filtered,
                    result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb(), null);
        }
        // Attach opProperties so mode=0 response can include them
        return new BatchResult(result.processed(), result.succeeded(), result.failed(),
                result.httpStats(), result.columns(), result.results(),
                result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb(),
                opProperties);
    }

    /**
     * Parses the input rows from a {@link RunRequest}.
     * Used by the ASYNC WebSocket handler to obtain the row count for the ACK message
     * before kicking off background processing. filterInput is applied separately after
     * the pre-enricher runs.
     */
    public List<DataRow> buildInputRows(RunRequest request) throws Exception {
        return buildInputRows(request, Map.of());
    }

    public List<DataRow> buildInputRows(RunRequest request, Map<String, String> opProperties) throws Exception {
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        String inputSourceType = (request.inputSource() != null && !request.inputSource().isBlank())
                ? request.inputSource().trim().toUpperCase()
                : op.getInputSource().getType().trim().toUpperCase();

        List<DataRow> rows = switch (inputSourceType) {
            case "FILE" -> {
                String path = request.inputFilePath();
                if (path == null || path.isBlank())
                    throw new IllegalArgumentException("inputFilePath is required for FILE input source");
                path = resolveTemplate(path, Map.of(), opProperties);
                if (!Files.exists(Path.of(path)))
                    throw new IllegalArgumentException("inputFilePath does not exist: " + path);
                yield readDataRowsFromFile(path, null, op);
            }
            case "REQUEST", "HTTPGET", "HTTPPOST" -> {
                if (request.raw() != null && !request.raw().isEmpty()) {
                    yield readDataRowsFromRaw(request.raw(), null);
                } else if (request.ids() != null && !request.ids().isEmpty()) {
                    yield readDataRowsFromRequest(request.ids(), null);
                } else {
                    throw new IllegalArgumentException(
                            "ids or raw data is required for " + inputSourceType + " input source");
                }
            }
            case "JSON" -> {
                String path = request.inputJsonPath();
                if (path == null || path.isBlank())
                    throw new IllegalArgumentException("inputJsonPath is required for JSON input source");
                path = resolveTemplate(path, Map.of(), opProperties);
                if (!Files.exists(Path.of(path)))
                    throw new IllegalArgumentException("inputJsonPath does not exist: " + path);
                yield readDataRowsFromJson(path, null);
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
                yield readDataRowsFromHttp(httpCfg, null);
            }
            case "HTTPLOCAL" -> {
                String url = request.inputHttpUrl();
                if (url == null || url.isBlank())
                    throw new IllegalArgumentException("inputHttpUrl is required for HTTPLOCAL input source");
                yield readDataRowsFromLocal(url);
            }
            default -> throw new IllegalArgumentException("Unknown inputSource type: " + inputSourceType);
        };

        return rows;
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
        if ("ALIAS".equalsIgnoreCase(request.inputSource()) && (request.alias() == null || request.alias().isBlank()))
            throw new IllegalArgumentException("alias is required when inputSource=ALIAS");
        String rawAlias = request.alias();
        RunRequest rawRequest = request;
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        if (rawAlias == null || rawAlias.isBlank()) checkMandatoryProperties(request, op);

        Map<String, String> opProperties = loadOperationProperties(op, request, rawRequest);
        BatchProperties.EnricherProperties enricherProps = op.getEnricher();

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;

        applyPreEnricher(rows, enricherProps);
        rows = applyFilterInput(rows, request.filterInput());
        if (request.inputCount() != null && request.inputCount() > 0)
            rows = rows.stream().limit(request.inputCount()).collect(Collectors.toList());
        checkMandatoryAttributes(rows, op);

        // Mode 2: return input rows after pre-enricher
        if (debugMode == 2) {
            BatchResult r = debugResult(rows);
            for (Map<String, Object> row : r.results()) rowCallback.accept(row);
            return CompletableFuture.completedFuture(
                    new BatchResult(r.processed(), r.succeeded(), r.failed(),
                            r.httpStats(), r.columns(), List.of(),
                            r.batchUuid(), r.timestamp(), r.timeTakenMs(), r.responseSizeKb(), null));
        }

        // Mode 3: skip post-enricher; modes 3 & 4 skip responseProcessor (handled in runCoreAsync)
        Consumer<Map<String, Object>> effectiveCallback = rowCallback;
        if (debugMode == 0 && enricherProps != null && "post".equalsIgnoreCase(enricherProps.getType())) {
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
                request.filterOutput(), effectiveCallback, responseProc, opProperties);
    }

    // -------------------------------------------------------------------------
    // Public API — convenience overloads kept for backward compatibility
    // -------------------------------------------------------------------------

    /** File-based run. */
    public BatchResult run(String filePath, Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(filePath, inputCount, op);
        return runCore(rows, op, operation, null, null, 0, null, Map.of());
    }

    /** Direct list of identifiers — each ID becomes a DataRow with key "id". */
    public BatchResult run(List<String> identifiers, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromRequest(identifiers, null);
        return runCore(rows, op, operation, null, null, 0, null, Map.of());
    }

    /** Runs from file and writes PSV output; returns summary metadata. */
    public PsvResult runToPsv(String inputFilePath, String outputFilePath,
                              Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(inputFilePath, inputCount, op);
        BatchResult batch = runCore(rows, op, operation, null, null, 0, null, Map.of());
        writeToPsv(batch, outputFilePath, false);
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
        if (filePath == null || filePath.isBlank())
            throw new IllegalArgumentException("inputFilePath must not be blank");
        if (!Files.exists(Path.of(filePath)))
            throw new IllegalArgumentException("inputFilePath does not exist: " + filePath);

        List<String> allLines = Files.readAllLines(Path.of(filePath));
        if (allLines.isEmpty()) return List.of();

        String headerLine      = allLines.get(0);
        String[] commaTokens   = headerLine.split(",", -1);
        String   delimiter     = commaTokens.length > 1 ? "," : "|";
        String[] headers       = delimiter.equals(",") ? commaTokens
                                  : headerLine.split(Pattern.quote("|"), -1);
        headers = Arrays.stream(headers).map(s -> s.trim().toUpperCase()).toArray(String[]::new);

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
            throw new IllegalArgumentException(
                    "Input source HTTP call failed with status " + response.statusCode()
                    + " for URL: " + config.getUrl());
        }

        Map<String, Object> parsed;
        try {
            parsed = objectMapper.readValue(response.body(), new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Input source HTTP response is not valid JSON from URL: " + config.getUrl()
                    + " — " + e.getMessage());
        }
        Object dataObj = parsed.get("data");
        if (!(dataObj instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "Input source HTTP response must contain a 'data' array. "
                    + "Keys found: " + parsed.keySet() + " from URL: " + config.getUrl());
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

    /**
     * Executes an inner batch run in-process, using a relative URL of the form
     * {@code /batch/run?operation=X&inputFilePath=Y&...} to supply parameters.
     * The inner run's result rows become the input DataRows for the outer pipeline.
     */
    private List<DataRow> readDataRowsFromLocal(String url) throws Exception {
        Map<String, String> params = parseQueryString(url);
        String operation = params.get("operation");
        if (operation == null || operation.isBlank())
            throw new IllegalArgumentException(
                    "HTTPLOCAL inputHttpUrl must include an 'operation' query parameter");

        String idsParam = params.get("ids");
        List<String> ids = null;
        if (idsParam != null && !idsParam.isBlank()) {
            ids = Arrays.stream(idsParam.split(","))
                    .map(String::trim).filter(s -> !s.isBlank()).toList();
        }

        RunRequest innerRequest = new RunRequest(
                operation,
                params.get("inputSource"),
                params.get("inputFilePath"),
                params.get("inputHttpUrl"),
                ids,
                null,   // raw — not supported via query string
                parseLocalInt(params.get("inputCount")),
                null,   // outputData — always HTTP for inner calls
                null,   // outputFilePath
                parseLocalInt(params.get("debugMode")),
                parseLocalInt(params.get("httpThreadCount")),
                parseLocalInt(params.get("httpTimeoutMs")),
                null,   // filterInput
                null,   // filterOutput
                null,   // executionMode
                params.get("alias"),
                params.get("responseProcessor"),
                null,   // appendOutput
                params.get("inputJsonPath"),
                null    // properties
        );

        BatchResult innerResult = run(innerRequest);

        List<DataRow> rows = new ArrayList<>(innerResult.results().size());
        int seq = 1;
        for (Map<String, Object> resultRow : innerResult.results()) {
            DataRow row = new DataRow(new LinkedHashMap<>(resultRow));
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    private static Map<String, String> parseQueryString(String url) {
        Map<String, String> params = new LinkedHashMap<>();
        int q = url.indexOf('?');
        if (q < 0) return params;
        for (String pair : url.substring(q + 1).split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String key = pair.substring(0, eq);
                String val = pair.substring(eq + 1);
                try { val = java.net.URLDecoder.decode(val, java.nio.charset.StandardCharsets.UTF_8); }
                catch (Exception ignored) {}
                params.put(key, val);
            }
        }
        return params;
    }

    private static Integer parseLocalInt(String value) {
        if (value == null) return null;
        try { return Integer.parseInt(value.trim()); } catch (NumberFormatException e) { return null; }
    }

    /**
     * Reads a JSON file and returns its content as DataRows without any data-array unwrapping.
     * If the root is an array, each element becomes a DataRow.
     * If the root is an object, it becomes a single DataRow.
     */
    @SuppressWarnings("unchecked")
    public List<DataRow> readDataRowsFromJson(String filePath, Integer inputCount) throws Exception {
        Object parsed = objectMapper.readValue(java.nio.file.Path.of(filePath).toFile(), Object.class);
        List<Map<String, Object>> rawRows;
        if (parsed instanceof List<?> list) {
            rawRows = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) rawRows.add((Map<String, Object>) m);
            }
        } else if (parsed instanceof Map<?, ?> m) {
            rawRows = List.of((Map<String, Object>) m);
        } else {
            throw new IllegalArgumentException(
                    "inputJsonPath must contain a JSON object or array: " + filePath);
        }
        if (inputCount != null) rawRows = rawRows.stream().limit(inputCount).toList();
        List<DataRow> rows = new ArrayList<>(rawRows.size());
        int seq = 1;
        for (Map<String, Object> raw : rawRows) {
            DataRow row = new DataRow();
            row.getData().putAll(raw);
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // File output helper
    // -------------------------------------------------------------------------

    public void writeToPsv(BatchResult batch, String outputFilePath, boolean append) throws Exception {
        Path outputPath = Path.of(outputFilePath);
        if (batch.results().isEmpty()) {
            if (!append) Files.writeString(outputPath, "");
            return;
        }

        SequencedSet<String> columns = new LinkedHashSet<>();
        batch.results().get(0).keySet().forEach(columns::add);
        for (Map<String, Object> row : batch.results()) {
            if (!row.containsKey("errorMessage")) {
                row.keySet().forEach(columns::add);
                break;
            }
        }
        columns.add("errorMessage");

        java.nio.file.OpenOption[] options = append
                ? new java.nio.file.OpenOption[]{java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND}
                : new java.nio.file.OpenOption[]{java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING};
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, options)) {
            writer.write(String.join("|", columns));
            writer.newLine();
            for (Map<String, Object> row : batch.results()) {
                List<String> line = new ArrayList<>();
                for (String col : columns) {
                    Object val = row.get(col);
                    line.add(val != null ? val.toString().replaceAll("[\\r\\n]", " ") : "");
                }
                writer.write(String.join("|", line));
                writer.newLine();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Debug mode
    // -------------------------------------------------------------------------

    private BatchResult buildPropertiesResult(Map<String, String> opProperties) {
        List<Map<String, Object>> results = new ArrayList<>(opProperties.size());
        opProperties.forEach((k, v) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", k);
            row.put("value", v);
            results.add(row);
        });
        String batchUuid = UUID.randomUUID().toString();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        List<ColumnDef> columns = results.isEmpty() ? List.of() : buildColumnDefsFromResults(results);
        return new BatchResult(results.size(), results.size(), 0,
                new HttpStats(0, 0, 0), columns, results, batchUuid, timestamp, 0L, 0.0, null);
    }

    private BatchResult debugResult(List<DataRow> rows) {
        List<Map<String, Object>> results = rows.stream()
                .map(row -> {
                    Map<String, Object> m = new LinkedHashMap<>(row.getData());
                    m.remove("SEQUENCE_NUMBER");
                    return m;
                })
                .collect(Collectors.toList());

        String batchUuid  = UUID.randomUUID().toString();
        String timestamp  = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        List<ColumnDef> columns = results.isEmpty() ? List.of() : buildColumnDefsFromResults(results);

        return new BatchResult(rows.size(), rows.size(), 0,
                new HttpStats(0, 0, 0), columns, results, batchUuid, timestamp, 0L, 0.0, null);
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
                                BatchProperties.ResponseProcessorProperties responseProc,
                                Map<String, String> opProperties) throws Exception {

        List<BatchProperties.ActivityProperties> activities =
                op.getActivity() != null ? op.getActivity() : List.of();
        boolean useActivities = !activities.isEmpty();
        boolean useLegacy     = !useActivities
                && op.getHttp().getUrl() != null && !op.getHttp().getUrl().isBlank();

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

            boolean includeMetadata = false;

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

            List<Map<String, Object>> rawResults = (debugMode == 3 || debugMode == 4)
                    ? new ArrayList<>(results)
                    : applyResponseProcessor(new ArrayList<>(results), responseProc);
            List<Map<String, Object>> sanitizedResults = sanitizeKeys(rawResults);
            if (debugMode < 3) sanitizedResults.forEach(r -> r.remove("operationStatus"));
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());

            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            double responseSizeKb = totalResponseBytes.get() / 1024.0;

            return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb, null);

        } else if (!useLegacy) {
            // --- Pass-through: no activities, no HTTP — return input rows as results ---
            long timeTakenMs = System.currentTimeMillis() - batchStart;
            for (DataRow row : rows) {
                Map<String, Object> resultMap = row.toResponseMap(false);
                resultMap.put("operationStatus", "SUCCESS");
                results.add(resultMap);
                succeeded.incrementAndGet();
            }
            List<Map<String, Object>> rawResults = (debugMode == 3 || debugMode == 4)
                    ? new ArrayList<>(results)
                    : applyResponseProcessor(new ArrayList<>(results), responseProc);
            List<Map<String, Object>> sanitizedResults = sanitizeKeys(rawResults);
            if (debugMode < 3) sanitizedResults.forEach(r -> r.remove("operationStatus"));
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());
            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            return new BatchResult(rows.size(), succeeded.get(), 0,
                    new HttpStats(0, 0, 0), columns, sanitizedResults,
                    batchUuid, timestamp, timeTakenMs, 0, null);

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
            if (debugMode < 3) sanitizedResults.forEach(r -> r.remove("operationStatus"));
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());

            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            double responseSizeKb = totalResponseBytes.get() / 1024.0;

            return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb, null);
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
            BatchProperties.ResponseProcessorProperties responseProc,
            Map<String, String> opProperties) throws Exception {

        List<BatchProperties.ActivityProperties> activities =
                op.getActivity() != null ? op.getActivity() : List.of();
        boolean useLegacyAsync = activities.isEmpty()
                && op.getHttp().getUrl() != null && !op.getHttp().getUrl().isBlank();

        // Pass-through: no activities, no HTTP — stream rows directly
        if (activities.isEmpty() && !useLegacyAsync) {
            long batchStartPT = System.currentTimeMillis();
            for (DataRow row : rows) {
                Map<String, Object> resultMap = row.toResponseMap(false);
                if (debugMode >= 3) resultMap.put("operationStatus", "SUCCESS");
                if (filterOutput == null || matchesAll(resultMap, filterOutput)) rowCallback.accept(resultMap);
            }
            long timeTakenMs = System.currentTimeMillis() - batchStartPT;
            List<ColumnDef> columns;
            try { columns = applyColumnTemplate(buildColumnDefsFromResults(
                    rows.stream().map(r -> r.toResponseMap(false)).collect(Collectors.toList())),
                    op.getColumnTemplate()); } catch (Exception e) { columns = List.of(); }
            return CompletableFuture.completedFuture(new BatchResult(
                    rows.size(), rows.size(), 0, new HttpStats(0, 0, 0),
                    columns, List.of(), UUID.randomUUID().toString(),
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")),
                    timeTakenMs, 0, null));
        }

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

        ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
        ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
        HttpClient httpClient     = HttpClient.newBuilder().build();

        List<ResolvedActivity> resolvedActivities = preloadActivities(activities, effectiveTimeoutMs);

        boolean includeMetadata = false;
        Consumer<Map<String, Object>> effectiveCallback = debugMode < 3
                ? row -> { row.remove("operationStatus"); rowCallback.accept(row); }
                : rowCallback;

        List<CompletableFuture<Void>> futures = rows.stream()
                .map(row -> processOneRowWithActivities(
                        row, resolvedActivities, httpClient, httpPool, xpathPool,
                        succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                        authHeader, includeMetadata, opProperties,
                        filterOutput, effectiveCallback))
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

                    List<Map<String, Object>> processed;
                    try {
                        processed = (debugMode == 3) ? new ArrayList<>(results)
                                : applyResponseProcessor(new ArrayList<>(results), responseProc);
                    } catch (Exception e) { processed = new ArrayList<>(results); }
                    List<Map<String, Object>> sanitizedForCols = sanitizeKeys(processed);

                    // Apply post-enrichment so enriched attributes appear in column defs
                    try { applyPostEnricher(sanitizedForCols, op.getEnricher()); } catch (Exception ignored) {}

                    List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedForCols);
                    try { columns = applyColumnTemplate(columns, op.getColumnTemplate()); }
                    catch (Exception ignored) {}

                    String batchUuid = UUID.randomUUID().toString();
                    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                    double responseSizeKb = totalResponseBytes.get() / 1024.0;

                    return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                            httpStats, columns, List.of(), batchUuid, timestamp, timeTakenMs, responseSizeKb, null);
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
        row.forEach((k, v) -> {
            if (!"SEQUENCE_NUMBER".equals(k)) sanitized.put(k.replace('.', '_'), v);
        });
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
        Map<String, String> effectiveProps = new LinkedHashMap<>(opProperties);
        if (activity.config().getProperties() != null) effectiveProps.putAll(activity.config().getProperties());
        String resolvedUrl;
        String resolvedBody;
        try {
            resolvedUrl  = resolveTemplate(httpConfig.getUrl(), row.getData(), effectiveProps);
            resolvedBody = activity.resolvedBodyTemplate() != null
                    ? resolveTemplate(activity.resolvedBodyTemplate(), row.getData(), effectiveProps)
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
                resolvedCacheKey = resolveTemplate(cacheConfig.getKey(), row.getData(), effectiveProps);
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
            if (!row.containsKey("errorMessage")) {
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

    private List<Map<String, Object>> applyResponseProcessor(
            List<Map<String, Object>> rows,
            BatchProperties.ResponseProcessorProperties proc) throws Exception {

        if (proc == null) return rows;
        String rpType = proc.getType().trim().toUpperCase();

        if ("XML2JSON".equals(rpType)) {
            List<Map<String, Object>> out = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) out.add(convertXmlToJson(row));
            return out;
        }

        if ("JSONATA".equals(rpType)) {
            return applyJsonataToResults(rows, proc.getJsonataTransform());
        }

        return rows;
    }

    private Map<String, Object> convertXmlToJson(Map<String, Object> row) {
        Object xml = row.get("responseBody");
        if (xml == null) return row;
        try {
            javax.xml.parsers.DocumentBuilderFactory factory =
                    javax.xml.parsers.DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            org.w3c.dom.Document doc = factory.newDocumentBuilder()
                    .parse(new org.xml.sax.InputSource(new java.io.StringReader(xml.toString())));
            Map<String, Object> result = new LinkedHashMap<>(row);
            result.remove("responseBody");
            xmlElementIntoMap(doc.getDocumentElement(), result);
            return result;
        } catch (Exception e) {
            return row;
        }
    }

    @SuppressWarnings("unchecked")
    private void xmlElementIntoMap(org.w3c.dom.Element element, Map<String, Object> map) {
        org.w3c.dom.NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            org.w3c.dom.Node child = children.item(i);
            if (child.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) continue;
            org.w3c.dom.Element childEl = (org.w3c.dom.Element) child;
            String key = childEl.getTagName();
            boolean hasChildEls = false;
            org.w3c.dom.NodeList grandChildren = childEl.getChildNodes();
            for (int j = 0; j < grandChildren.getLength(); j++) {
                if (grandChildren.item(j).getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                    hasChildEls = true; break;
                }
            }
            Object value;
            if (hasChildEls) {
                Map<String, Object> nested = new LinkedHashMap<>();
                xmlElementIntoMap(childEl, nested);
                value = nested;
            } else {
                value = childEl.getTextContent();
            }
            Object existing = map.get(key);
            if (existing == null) {
                map.put(key, value);
            } else if (existing instanceof List) {
                ((List<Object>) existing).add(value);
            } else {
                List<Object> list = new ArrayList<>();
                list.add(existing); list.add(value);
                map.put(key, list);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> applyJsonataToResults(
            List<Map<String, Object>> rows, String transform) throws Exception {
        if (transform == null || transform.isBlank()) return rows;
        String expr = transform;
        if (transform.startsWith("classpath:")) {
            String res = transform.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(res)) {
                if (is == null) throw new FileNotFoundException("JSONata transform not found: " + res);
                expr = new String(is.readAllBytes());
            }
        }
        String json = objectMapper.writeValueAsString(rows);
        Object result = Jsonata.jsonata(expr).evaluate(objectMapper.readValue(json, Object.class));
        if (result == null) return List.of();
        Object parsed = objectMapper.readValue(objectMapper.writeValueAsString(result), Object.class);
        if (parsed instanceof List<?> list) {
            return list.stream().filter(item -> item instanceof Map)
                    .map(item -> (Map<String, Object>) item).collect(Collectors.toList());
        }
        if (parsed instanceof Map<?, ?> map) return List.of((Map<String, Object>) map);
        return rows;
    }

    /**
     * Builds the effective properties map for a request by layering (lowest → highest priority):
     * server.json → operation attributes → file → http → alias request fields
     * → raw request scalar fields → request.properties() → ${VAR} resolution
     *
     * @param op          resolved operation config
     * @param resolved    request after alias merge (used for alias lookup and final properties())
     * @param raw         original request before alias merge (its non-null scalars override alias values)
     */
    /** Public entry point used by the WebSocket handler to pre-load properties before building input rows. */
    public Map<String, String> loadRequestProperties(RunRequest request) throws Exception {
        RunRequest resolved = resolveAlias(request);
        BatchProperties.OperationProperties op = batchProperties.getOperation(resolved.operation());
        return loadOperationProperties(op, resolved, request);
    }

    private Map<String, String> loadOperationProperties(
            BatchProperties.OperationProperties op,
            RunRequest resolved,
            RunRequest raw) throws Exception {

        // 0. Runtime variables (lowest priority — overridable by every subsequent layer)
        Map<String, String> result = new LinkedHashMap<>();
        String hostname;
        try { hostname = java.net.InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { hostname = "localhost"; }
        long pid = ProcessHandle.current().pid();
        result.put("HOSTNAME",   hostname);
        result.put("PORTNUMBER", String.valueOf(serverPort));
        result.put("DATESTAMP",  java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
        result.put("DATETIME",   java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
        result.put("PROCESSID",  String.valueOf(pid));
        result.put("JVMID",      hostname + "." + pid);

        // 1. Server properties (base)
        result.putAll(serverPropertiesLoader.getProperties());

        // 2. Operation-level properties
        BatchProperties.OperationPropertiesConfig cfg = op.getProperties();
        if (cfg != null) {
            if (cfg.getAttributes() != null) result.putAll(cfg.getAttributes());

            BatchProperties.FilePropertiesSource fileSrc = cfg.getFile();
            if (fileSrc != null && !fileSrc.getPath().isBlank()) {
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(fileSrc.getPath())) {
                    props.load(fis);
                }
                props.forEach((k, v) -> result.put(k.toString(), v.toString()));
            }

            BatchProperties.HttpPropertiesSource httpSrc = cfg.getHttp();
            if (httpSrc != null && !httpSrc.getUrl().isBlank()) {
                HttpClient client = HttpClient.newBuilder().build();
                int timeout = httpSrc.getTimeoutMs() > 0 ? httpSrc.getTimeoutMs() : 5000;
                HttpRequest.Builder rb = HttpRequest.newBuilder()
                        .uri(URI.create(httpSrc.getUrl()))
                        .timeout(Duration.ofMillis(timeout));
                if ("POST".equalsIgnoreCase(httpSrc.getMethod())) {
                    String formBody = result.entrySet().stream()
                            .map(e -> java.net.URLEncoder.encode(e.getKey(), java.nio.charset.StandardCharsets.UTF_8)
                                    + "=" + java.net.URLEncoder.encode(
                                            e.getValue() != null ? e.getValue() : "",
                                            java.nio.charset.StandardCharsets.UTF_8))
                            .collect(Collectors.joining("&"));
                    rb.header("Content-Type", "application/x-www-form-urlencoded")
                      .POST(HttpRequest.BodyPublishers.ofString(formBody));
                } else {
                    rb.GET();
                }
                HttpResponse<String> resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                    Map<String, Object> parsed = objectMapper.readValue(resp.body(), new TypeReference<>() {});
                    parsed.forEach((k, v) -> {
                        String val = v != null ? v.toString() : "";
                        try { val = java.net.URLDecoder.decode(val, java.nio.charset.StandardCharsets.UTF_8); }
                        catch (Exception ignored) {}
                        result.put(k, val);
                    });
                }
            }
        }

        // 3. Alias request scalar fields (lower priority than the incoming request)
        if (resolved.alias() != null && !resolved.alias().isBlank()) {
            op.getAlias().stream()
                    .filter(a -> a.getName().equalsIgnoreCase(resolved.alias().trim()))
                    .findFirst()
                    .ifPresent(aliasProps -> {
                        RunRequest aliasReq = objectMapper.convertValue(aliasProps.getRequest(), RunRequest.class);
                        Map<String, Object> aliasMap = objectMapper.convertValue(aliasReq, new TypeReference<>() {});
                        aliasMap.forEach((k, v) -> {
                            if (v != null && !(v instanceof List) && !(v instanceof Map)) {
                                result.put(k, v.toString());
                            }
                        });
                    });
        }

        // 4. Raw request scalar fields — only those explicitly set (non-null) override alias values
        Map<String, Object> rawMap = objectMapper.convertValue(raw, new TypeReference<>() {});
        rawMap.forEach((k, v) -> {
            if (v != null && !(v instanceof List) && !(v instanceof Map)) {
                result.put(k, v.toString());
            }
        });

        // 5. Explicit inline properties (highest priority)
        if (resolved.properties() != null) result.putAll(resolved.properties());

        // 6. Resolve ${VAR} placeholders in every value using the accumulated map
        resolvePropertyVariables(result);

        return result;
    }

    /**
     * Resolves {@code ${KEY}} placeholders inside each value of {@code props} using the map
     * itself as the variable source (case-insensitive). Unknown placeholders are left as-is.
     * Runs until no further substitutions can be made (handles chained references).
     */
    private static void resolvePropertyVariables(Map<String, String> props) {
        java.util.TreeMap<String, String> ci = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ci.putAll(props);
        Pattern p = Pattern.compile("\\$\\{([^}]+)\\}");
        props.replaceAll((k, v) -> {
            if (v == null) return null;
            Matcher m = p.matcher(v);
            StringBuffer sb = new StringBuffer();
            while (m.find()) {
                String var = m.group(1);
                String replacement = ci.getOrDefault(var, m.group(0)); // leave unknown as-is
                m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
            m.appendTail(sb);
            return sb.toString();
        });
    }

    private void checkMandatoryProperties(RunRequest request,
                                           BatchProperties.OperationProperties op) {
        List<String> mandatory = op.getMandatoryPropertiesList();
        if (mandatory.isEmpty()) return;
        Map<String, Object> reqMap = objectMapper.convertValue(request, new TypeReference<>() {});
        List<String> missing = mandatory.stream()
                .filter(prop -> {
                    Object val = reqMap.get(prop);
                    return val == null || val.toString().isBlank();
                }).toList();
        if (!missing.isEmpty())
            throw new IllegalArgumentException(
                    "Request is missing mandatory field(s): " + missing);
    }

    private static void checkMandatoryAttributes(List<DataRow> rows,
                                                  BatchProperties.OperationProperties op) {
        List<String> mandatory = op.getMandatoryAttributeList();
        if (mandatory.isEmpty() || rows.isEmpty()) return;
        for (DataRow row : rows) {
            Map<String, Object> ci = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            ci.putAll(row.getData());
            List<String> missing = mandatory.stream()
                    .filter(attr -> !ci.containsKey(attr)).toList();
            if (!missing.isEmpty())
                throw new IllegalArgumentException(
                        "Row is missing mandatory attribute(s): " + missing
                                + ". Available keys: " + row.getData().keySet());
        }
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
            if (f.getColumn() == null) continue;
            Object found = data.get(f.getColumn());
            if (found == null) {
                for (Map.Entry<String, Object> e : data.entrySet()) {
                    if (e.getKey().equalsIgnoreCase(f.getColumn())) { found = e.getValue(); break; }
                }
            }
            if (found == null) continue;
            String rowValue    = Objects.toString(found, "");
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
            row.forEach((k, v) -> {
                if (!"SEQUENCE_NUMBER".equals(k)) newRow.put(k.replace('.', '_'), v);
            });
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
        Map<String, Object> ci = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ci.putAll(row);
        Map<String, String> ciFallback = null;
        if (fallback != null) {
            ciFallback = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            ciFallback.putAll(fallback);
        }
        Matcher m = Pattern.compile("\\$?\\{([^}]+)\\}").matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            Object val = ci.get(key);
            if (val == null && ciFallback != null) val = ciFallback.get(key);
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
