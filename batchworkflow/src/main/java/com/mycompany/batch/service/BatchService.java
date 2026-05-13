package com.mycompany.batch.service;

import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.enricher.EnricherConfig;
import com.mycompany.batch.enricher.EnricherService;
import com.mycompany.batch.jsonpath.JsonPathColumn;
import com.mycompany.batch.jsonpath.JsonPathExtractor;
import com.mycompany.batch.model.ActivityType;
import com.mycompany.batch.model.CacheOutput;
import com.mycompany.batch.model.DataExtractionType;
import com.mycompany.batch.model.DataRow;
import com.mycompany.batch.model.EnricherType;
import com.mycompany.batch.model.FilterRule;
import com.mycompany.batch.model.HttpMethod;
import com.mycompany.batch.model.InputSourceType;
import com.mycompany.batch.model.ProcessorType;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.model.SearchKeyword;
import com.mycompany.batch.auth.BasicAuthProvider;
import com.mycompany.batch.auth.DigestAuthProvider;
import com.mycompany.batch.auth.HttpAuthProvider;
import com.mycompany.batch.auth.JwtAuthProvider;
import com.mycompany.batch.auth.KerberosAuthProvider;
import com.mycompany.batch.auth.NtlmAuthProvider;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.xpath.XPathColumn;
import com.mycompany.batch.xpath.XPathExtractor;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
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
    /** Loaded once at startup from responseprocessors.json — name → ordered list of processor steps. */
    private final Map<String, List<BatchProperties.ResponseProcessorProperties>> responseProcessorRegistry = new LinkedHashMap<>();

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
                authProviders.put(name, buildAuthProvider(name, op.getAuth(), null));
                for (BatchProperties.ActivityProperties act : op.getActivity()) {
                    BatchProperties.AuthProperties actAuth = act.getHttp() != null ? act.getHttp().getAuth() : null;
                    if (actAuth != null && actAuth.getMethod() != com.mycompany.batch.model.AuthMethod.NONE) {
                        String actHttpUrl = act.getHttp() != null ? act.getHttp().getUrl() : null;
                        authProviders.put(name + "::" + act.getName(), buildAuthProvider(act.getName(), actAuth, actHttpUrl));
                    }
                    if (act.getHttp() != null) {
                        String bt = act.getHttp().getBodyTemplate();
                        if (bt != null && bt.startsWith("classpath:") && !bt.contains("${")) {
                            String resource = bt.substring("classpath:".length());
                            try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                                if (is == null) throw new java.io.FileNotFoundException(
                                        "bodyTemplate classpath resource not found: " + resource);
                            }
                        }
                    }
                }
                // Validate DB initialization entries
                for (BatchProperties.InitializationProperties init : op.getInitialization()) {
                    if ("DB".equalsIgnoreCase(init.getType())) {
                        if (init.getDb().getJdbcUrl().isBlank()) {
                            throw new IllegalStateException(
                                    "DB initialization '" + init.getName() + "' in operation '" + name + "' requires db.jdbcUrl");
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to initialise operation '" + name + "': " + e.getMessage(), e);
            }
        });
        loadResponseProcessorRegistry();
    }

    private void loadResponseProcessorRegistry() {
        try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream("responseprocessors.json")) {
            if (is == null) return;
            com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(is);
            com.fasterxml.jackson.databind.JsonNode list = root.get("responseProcessorList");
            if (list == null || !list.isArray()) return;
            for (com.fasterxml.jackson.databind.JsonNode entry : list) {
                String name = entry.path("name").asText("").trim();
                if (name.isBlank()) continue;
                List<BatchProperties.ResponseProcessorProperties> steps =
                        objectMapper.convertValue(entry.get("responseProcessor"),
                                new com.fasterxml.jackson.core.type.TypeReference<>() {});
                if (steps != null) responseProcessorRegistry.put(name, steps);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load responseprocessors.json: " + e.getMessage(), e);
        }
    }

    private HttpAuthProvider buildAuthProvider(String operationName,
                                               BatchProperties.AuthProperties auth,
                                               String fallbackUrl) throws Exception {
        return buildAuthProvider(operationName, auth, fallbackUrl, null);
    }

    /**
     * Builds an {@link HttpAuthProvider}, resolving any {@code ${KEY}} placeholders in credential
     * fields via {@code opProperties} when supplied (request-scoped call). At startup the param is
     * {@code null} and values are used as-is.
     */
    private HttpAuthProvider buildAuthProvider(String operationName,
                                               BatchProperties.AuthProperties auth,
                                               String fallbackUrl,
                                               Map<String, String> opProperties) throws Exception {
        return switch (auth.getMethod()) {
            case BASIC -> {
                String u = rp(auth.getBasic().getUsername(), opProperties);
                String p = rp(auth.getBasic().getPassword(), opProperties);
                if (opProperties != null) {
                    if (u.contains("${")) throw new IllegalStateException(
                            "BASIC auth for '" + operationName + "': username contains unresolved placeholder '" + u
                                    + "'. Make sure the required property is provided in the request.");
                    if (p.contains("${")) throw new IllegalStateException(
                            "BASIC auth for '" + operationName + "': password contains an unresolved placeholder."
                                    + " Make sure the required property is provided in the request.");
                }
                if (u.isBlank() || p.isBlank()) {
                    throw new IllegalStateException(
                            "BASIC auth for '" + operationName + "' requires basic.username and basic.password");
                }
                yield new BasicAuthProvider(u, p);
            }
            case JWT -> {
                String url = rp(auth.getJwt().getUrl(), opProperties);
                String u   = rp(auth.getJwt().getUsername(), opProperties);
                String p   = rp(auth.getJwt().getPassword(), opProperties);
                if (opProperties != null) {
                    if (u.contains("${")) throw new IllegalStateException(
                            "JWT auth for '" + operationName + "': username contains unresolved placeholder '" + u + "'.");
                    if (p.contains("${")) throw new IllegalStateException(
                            "JWT auth for '" + operationName + "': password contains an unresolved placeholder.");
                    if (url.contains("${")) throw new IllegalStateException(
                            "JWT auth for '" + operationName + "': url contains unresolved placeholder '" + url + "'.");
                }
                if (url.isBlank() || u.isBlank() || p.isBlank()) {
                    throw new IllegalStateException(
                            "JWT auth for '" + operationName + "' requires jwt.url, jwt.username and jwt.password");
                }
                yield new JwtAuthProvider(rp(auth.getJwt().getApplicationName(), opProperties),
                        u, p, url, objectMapper);
            }
            case KERBEROS -> {
                String u  = rp(auth.getKerberos().getUsername(), opProperties);
                String kt = rp(auth.getKerberos().getKeytab(), opProperties);
                String sp = rp(auth.getKerberos().getServicePrincipal(), opProperties);
                if (u.isBlank() || kt.isBlank() || sp.isBlank()) {
                    throw new IllegalStateException(
                            "KERBEROS auth for '" + operationName + "' requires kerberos.username, kerberos.keytab and kerberos.service-principal");
                }
                yield new KerberosAuthProvider(u, kt, sp);
            }
            case DIGEST -> {
                String u = rp(auth.getDigest().getUsername(), opProperties);
                String p = rp(auth.getDigest().getPassword(), opProperties);
                if (opProperties != null) {
                    if (u.contains("${")) throw new IllegalStateException(
                            "DIGEST auth for '" + operationName + "': username contains unresolved placeholder '" + u + "'.");
                    if (p.contains("${")) throw new IllegalStateException(
                            "DIGEST auth for '" + operationName + "': password contains an unresolved placeholder.");
                }
                if (u.isBlank() || p.isBlank()) {
                    throw new IllegalStateException(
                            "DIGEST auth for '" + operationName + "' requires digest.username and digest.password");
                }
                String digestUrl = rp(auth.getDigest().getUrl(), opProperties);
                if (digestUrl.isBlank()) digestUrl = fallbackUrl != null ? fallbackUrl : "";
                yield new DigestAuthProvider(u, p, digestUrl, objectMapper);
            }
            case NTLM -> {
                String u = rp(auth.getNtlm().getUsername(), opProperties);
                String p = rp(auth.getNtlm().getPassword(), opProperties);
                if (opProperties != null) {
                    if (u.contains("${")) throw new IllegalStateException(
                            "NTLM auth for '" + operationName + "': username contains unresolved placeholder '" + u + "'.");
                    if (p.contains("${")) throw new IllegalStateException(
                            "NTLM auth for '" + operationName + "': password contains an unresolved placeholder.");
                }
                if (u.isBlank() || p.isBlank()) {
                    throw new IllegalStateException(
                            "NTLM auth for '" + operationName + "' requires ntlm.username and ntlm.password");
                }
                yield new NtlmAuthProvider(u, p);
            }
            default -> () -> null;
        };
    }

    private String buildRequestAuthHeader(BatchProperties.AuthProperties auth,
                                          String operation,
                                          Map<String, String> opProperties) throws Exception {
        if (auth == null) return null;
        return buildAuthProvider(operation, auth, null, opProperties).getAuthorizationHeader();
    }

    /** Resolves a single {@code ${KEY}} placeholder string via opProperties; returns the value as-is when no placeholders or props are absent. */
    private static String rp(String val, Map<String, String> props) {
        if (val == null || props == null || !val.contains("${")) return val != null ? val : "";
        try { return resolveTemplate(val, Map.of(), props); } catch (Exception e) { return val; }
    }

    /**
     * Resolves {@code ${VAR}} placeholders in a cache maxRetentionTime string and parses it to an Integer.
     * Returns {@code null} when the raw value is null/blank (no TTL). Throws {@link IllegalArgumentException}
     * when a placeholder cannot be resolved or the result is not a valid integer.
     */
    private Integer resolveMaxRetentionTime(String raw, Map<String, Object> rowData, Map<String, String> props) {
        if (raw == null || raw.isBlank()) return null;
        String resolved = raw;
        if (raw.contains("${") || raw.contains("{")) {
            try { resolved = resolveTemplate(raw, rowData != null ? rowData : Map.of(), props); }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Cache maxRetentionTime placeholder not resolved: " + raw
                        + ". " + e.getMessage());
            }
        }
        try { return Integer.parseInt(resolved.trim()); }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cache maxRetentionTime is not a valid integer: '" + resolved + "'");
        }
    }

    /** Builds an {@link HttpClient}, adding an NTLM {@link java.net.Authenticator} when the operation uses NTLM auth. */
    private HttpClient buildHttpClient(String operationKey) {
        HttpAuthProvider provider = authProviders.get(operationKey);
        if (provider instanceof NtlmAuthProvider ntlm) {
            return HttpClient.newBuilder().authenticator(ntlm.toAuthenticator()).build();
        }
        return HttpClient.newBuilder().build();
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
    private record ResolvedTransformStep(
            String source,           // "$", "$VAR", "$activityName.key"
            boolean isNone,          // true → pass source through without transformation
            String jsonataExpression, // pre-loaded JSONATA expression (null when isNone)
            String onError,          // "SKIP" | "$.key" | null (rethrow)
            String output            // optional variable name to capture this step's result
    ) {}

    private static class ValidationException extends RuntimeException {
        private final String activityName;
        ValidationException(String activityName, String message) {
            super(message);
            this.activityName = activityName;
        }
        String getActivityName() { return activityName; }
    }

    public static class ValidationFailureException extends RuntimeException {
        private final Map<String, Object> body;
        public ValidationFailureException(Map<String, Object> body) {
            super("Validation failed");
            this.body = body;
        }
        public Map<String, Object> getBody() { return body; }
    }

    private record ResolvedActivity(
            BatchProperties.ActivityProperties config,
            String resolvedBodyTemplate,        // for HTTP activities
            Map<String, String> xpathMap,       // XPATH extraction (null otherwise)
            String jsonataTransform,            // JSON/JSONATA extraction (null otherwise)
            List<JsonPathColumn> jsonPathColumns, // JSONPATH extraction (null otherwise)
            String activityAuthHeader,          // non-null when activity has its own auth
            List<ResolvedTransformStep> transformSteps, // TRANSFORM activities (null otherwise)
            EnricherConfig enricherConfig,      // DATAENRICHER activities (null otherwise)
            Map<String, Map<String, Map<String, Object>>> enricherStaticDatasets // pre-loaded per batch
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

        // When inputSource=ALIAS treat it as absent so the alias's inputSource wins
        InputSourceType effectiveInputSource =
                req.inputSource() == InputSourceType.ALIAS ? null : req.inputSource();

        return new RunRequest(
                req.operation(),
                mergeObj(effectiveInputSource,        a.inputSource()),
                mergeStr(req.inputFilePath(),         a.inputFilePath()),
                mergeStr(req.inputHttpUrl(),          a.inputHttpUrl()),
                mergeMap(req.inputHttpHeader(),       a.inputHttpHeader()),
                mergeStr(req.inputHttpBody(),         a.inputHttpBody()),
                mergeList(req.ids(),                  a.ids()),
                mergeList(req.raw(),                  a.raw()),
                req.inputCount()      != null ? req.inputCount()      : a.inputCount(),
                mergeObj(req.outputData(),            a.outputData()),
                mergeStr(req.outputFilePath(),        a.outputFilePath()),
                req.debugMode()       != null ? req.debugMode()       : a.debugMode(),
                req.httpThreadCount() != null ? req.httpThreadCount() : a.httpThreadCount(),
                req.httpTimeoutMs()   != null ? req.httpTimeoutMs()   : a.httpTimeoutMs(),
                mergeList(req.filterInput(),          a.filterInput()),
                mergeList(req.filterOutput(),         a.filterOutput()),
                req.searchKeyword() != null ? req.searchKeyword() : a.searchKeyword(),
                req.cache()         != null ? req.cache()         : a.cache(),
                mergeObj(req.executionMode(),         a.executionMode()),
                req.alias(),
                mergeStr(req.responseProcessor(),     a.responseProcessor()),
                req.appendOutput() != null ? req.appendOutput() : a.appendOutput(),
                mergeStr(req.inputJsonPath(),         a.inputJsonPath()),
                mergeStr(req.cacheName(),             a.cacheName()),
                mergeMap(req.properties(),            a.properties()),
                mergeObj(req.jsonataTransform(),      a.jsonataTransform()),
                mergeStr(req.templateName(),          a.templateName()),
                mergeObj(req.auth(),                  a.auth()),
                mergeStr(req.operationType(),         a.operationType()));
    }

    /** Returns {@code a} if non-null and non-blank, otherwise {@code b}. */
    private static String mergeStr(String a, String b) {
        return (a != null && !a.isBlank()) ? a : b;
    }

    /** Returns {@code a} if non-null, otherwise {@code b}. Used for enum fields. */
    private static <T> T mergeObj(T a, T b) {
        return a != null ? a : b;
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
    // Runtime operation registration — registers an operation without restart
    // -------------------------------------------------------------------------

    public synchronized void registerOperation(BatchProperties.OperationProperties op) throws Exception {
        String name = op.getName();
        if (name == null || name.isBlank()) throw new IllegalArgumentException("operation name is required");
        op.validate(name);
        batchProperties.getOperations().put(name, op);
        authProviders.put(name, buildAuthProvider(name, op.getAuth(), null));
        for (BatchProperties.ActivityProperties act : op.getActivity()) {
            BatchProperties.AuthProperties actAuth = act.getHttp() != null ? act.getHttp().getAuth() : null;
            if (actAuth != null && actAuth.getMethod() != com.mycompany.batch.model.AuthMethod.NONE) {
                String actHttpUrl = act.getHttp() != null ? act.getHttp().getUrl() : null;
                authProviders.put(name + "::" + act.getName(), buildAuthProvider(act.getName(), actAuth, actHttpUrl));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Public API — unified RunRequest entry point
    // -------------------------------------------------------------------------

    public BatchResult run(RunRequest request) throws Exception {
        if (request.inputSource() == InputSourceType.ALIAS && (request.alias() == null || request.alias().isBlank()))
            throw new IllegalArgumentException("alias is required when inputSource=ALIAS");
        String rawAlias = request.alias();
        RunRequest rawRequest = request;
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        boolean isSpecific = request.inputSource() == InputSourceType.SPECIFIC;

        Map<String, String> opProperties = loadOperationProperties(op, request, rawRequest);

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;

        // Mode 1: return properties as key/value data rows
        if (debugMode == 1) return buildPropertiesResult(opProperties);

        // Inject mandatory-property defaults early so buildInputRows can resolve inputSource
        // from opProperties when the request field is absent.
        injectMandatoryDefaults(rawRequest, rawAlias, op, opProperties);

        List<DataRow> rows = buildInputRows(request, opProperties);

        if (!isSpecific) {
            if (rawAlias != null && !rawAlias.isBlank())
                checkAliasMandatoryProperties(rawRequest, rawAlias, op, opProperties);
            else
                checkMandatoryProperties(request, op, opProperties);
        }

        applyPreEnricher(rows, op.getEnricher());
        if (request.inputCount() != null && request.inputCount() > 0)
            rows = rows.stream().limit(request.inputCount()).collect(Collectors.toList());
        rows = applySearchKeyword(rows, request.searchKeyword());
        rows = applyFilterInput(rows, request.filterInput());
        if (!isSpecific) checkMandatoryAttributes(rows, op);

        // Mode 2: return after input source + pre-enricher (no activities)
        if (debugMode == 2) return debugResult(rows);

        String reqAuthHeader = buildRequestAuthHeader(request.auth(), operation, opProperties);
        // When no per-request auth override is supplied, try re-building the operation-level auth
        // at request time so ${VAR} placeholders in credentials are resolved from opProperties.
        if (reqAuthHeader == null && op.getAuth() != null
                && op.getAuth().getMethod() != com.mycompany.batch.model.AuthMethod.NONE) {
            try {
                reqAuthHeader = buildAuthProvider(operation, op.getAuth(), null, opProperties)
                        .getAuthorizationHeader();
            } catch (IllegalStateException e) {
                throw new IllegalArgumentException("Operation '" + operation + "' auth configuration error: " + e.getMessage());
            } catch (Exception ignored) {}
        }
        BatchResult result = runCore(rows, op, operation,
                request.httpThreadCount(), request.httpTimeoutMs(), debugMode, opProperties,
                request.inputHttpHeader() != null ? request.inputHttpHeader() : Map.of(), reqAuthHeader);

        // Mode 3: return after activities
        if (debugMode == 3) return result;

        applyPostEnricher(result.results(), op.getEnricher());

        // Rebuild columns after post-enrichment so enriched attributes appear in the grid
        List<ColumnDef> enrichedColumns = buildColumnDefsFromResults(result.results());
        enrichedColumns = applyColumnTemplate(enrichedColumns, op.getColumnTemplate());
        result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                result.httpStats(), enrichedColumns, result.results(),
                result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb(), null);

        // Mode 4: return after post-enrichment. Mode 0: full pipeline — apply output filter
        List<Map<String, Object>> filtered = applyFilterOutput(result.results(), request.filterOutput());
        if (filtered != result.results()) {
            result = new BatchResult(result.processed(), result.succeeded(), result.failed(),
                    result.httpStats(), result.columns(), filtered,
                    result.batchUuid(), result.timestamp(), result.timeTakenMs(), result.responseSizeKb(), null);
        }
        final CacheOutput cacheOut = request.cache();
        if (cacheOut != null) {
            result.results().forEach(row -> saveSingleRowToCache(row, cacheOut));
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

        InputSourceType inputSourceType;
        if (request.inputSource() != null) {
            inputSourceType = request.inputSource();
        } else {
            // Fall back to opProperties (populated from request fields + alias defaults),
            // then operation-level config attributes, then the operation default (FILE).
            String fromProps = opProperties.get("inputSource");
            // ALIAS and SPECIFIC can leak into opProperties from the raw request via
            // loadOperationProperties step 4, but must never be used as a real source here.
            if ("ALIAS".equalsIgnoreCase(fromProps) || "SPECIFIC".equalsIgnoreCase(fromProps)) fromProps = null;
            String fromAttrs = fromProps != null && !fromProps.isBlank() ? fromProps
                    : op.getProperties().getAttributes().get("inputSource");
            inputSourceType = (fromAttrs != null && !fromAttrs.isBlank())
                    ? InputSourceType.from(fromAttrs)
                    : op.getInputSource().getType();
        }

        List<DataRow> rows = switch (inputSourceType) {
            case FILE -> {
                String path = request.inputFilePath();
                if (path == null || path.isBlank()) path = opProperties.get("inputFilePath");
                if (path == null || path.isBlank())
                    throw new IllegalArgumentException("inputFilePath is required for FILE input source");
                path = resolveTemplate(path, Map.of(), opProperties);
                if (!Files.exists(Path.of(path)))
                    throw new IllegalArgumentException("inputFilePath does not exist: " + path);
                yield readDataRowsFromFile(path, null, op, request.searchKeyword());
            }
            case REQUEST, HTTPGET, HTTPPOST -> {
                if (request.raw() != null && !request.raw().isEmpty()) {
                    yield readDataRowsFromRaw(request.raw(), null, opProperties);
                } else if (request.ids() != null && !request.ids().isEmpty()) {
                    yield readDataRowsFromRequest(request.ids(), null);
                } else {
                    throw new IllegalArgumentException(
                            "ids or raw data is required for " + inputSourceType + " input source");
                }
            }
            case JSON -> {
                String path = request.inputJsonPath();
                if (path == null || path.isBlank()) path = opProperties.get("inputJsonPath");
                if (path == null || path.isBlank())
                    throw new IllegalArgumentException("inputJsonPath is required for JSON input source");
                path = resolveTemplate(path, Map.of(), opProperties);
                if (!Files.exists(Path.of(path)))
                    throw new IllegalArgumentException("inputJsonPath does not exist: " + path);
                yield readDataRowsFromJson(path, null);
            }
            case HTTP, HTTPCONFIG -> {
                BatchProperties.HttpConfigSourceProperties httpCfg = op.getInputSource().getHttpConfig();
                String httpUrl = request.inputHttpUrl();
                if (httpUrl == null || httpUrl.isBlank()) httpUrl = opProperties.get("inputHttpUrl");
                if (httpUrl != null && !httpUrl.isBlank()) {
                    Map<String, String> urlProps = new LinkedHashMap<>(opProperties);
                    System.getenv().forEach(urlProps::putIfAbsent);
                    httpUrl = resolveTemplate(httpUrl.trim(), Map.of(), urlProps);
                    if (!httpUrl.startsWith("http://") && !httpUrl.startsWith("https://"))
                        yield readDataRowsFromLocal(httpUrl);
                    BatchProperties.HttpConfigSourceProperties override =
                            new BatchProperties.HttpConfigSourceProperties();
                    override.setUrl(httpUrl);
                    // if the request has a body, force POST; otherwise keep the configured method
                    override.setMethod(request.inputHttpBody() != null && !request.inputHttpBody().isBlank()
                            ? com.mycompany.batch.model.HttpMethod.POST : httpCfg.getMethod());
                    override.setJsonataTransform(httpCfg.getJsonataTransform());
                    httpCfg = override;
                } else if (httpCfg.getUrl() == null || httpCfg.getUrl().isBlank()) {
                    throw new IllegalArgumentException(
                            "inputHttpUrl is required in the request when inputSource=HTTPCONFIG "
                                    + "and no url is configured in operations.json");
                }
                yield readDataRowsFromHttp(httpCfg, null, request.inputHttpBody(), request.inputHttpHeader());
            }
            case HTTPLOCAL -> {
                String url = request.inputHttpUrl();
                if (url == null || url.isBlank()) url = opProperties.get("inputHttpUrl");
                if (url == null || url.isBlank())
                    throw new IllegalArgumentException("inputHttpUrl is required for HTTPLOCAL input source");
                yield readDataRowsFromLocal(url);
            }
            case CACHE -> {
                String cn = request.cacheName();
                if (cn == null || cn.isBlank()) cn = opProperties.get("cacheName");
                if (cn == null || cn.isBlank())
                    throw new IllegalArgumentException("cacheName is required for CACHE input source");
                yield readDataRowsFromCache(cn);
            }
            case SPECIFIC -> List.of(new DataRow(new java.util.LinkedHashMap<>()));
            case TEMPLATE -> {
                String tmplName = request.templateName();
                if (tmplName == null || tmplName.isBlank()) tmplName = opProperties.get("templateName");
                if (tmplName == null || tmplName.isBlank()) tmplName = op.getInputSource().getTemplateName();
                if (tmplName == null || tmplName.isBlank())
                    throw new IllegalArgumentException("templateName is required for TEMPLATE input source");
                yield readDataRowsFromTemplate(tmplName);
            }
            case DB -> {
                BatchProperties.DbConfigSourceProperties dbCfg = op.getInputSource().getDbConfig();
                String dbUrl  = resolveTemplate(dbCfg.getJdbcUrl(),  Map.of(), opProperties);
                String dbUser = resolveTemplate(dbCfg.getUserName(), Map.of(), opProperties);
                String dbPass = resolveTemplate(dbCfg.getPassword(), Map.of(), opProperties);
                String dbSql  = resolveTemplate(dbCfg.getSql(),      Map.of(), opProperties);
                if (dbUrl.isBlank())
                    throw new IllegalArgumentException("inputSource.dbConfig.jdbcUrl is required for DB input source");
                if (dbSql.isBlank())
                    throw new IllegalArgumentException("inputSource.dbConfig.sql is required for DB input source");
                yield readDataRowsFromDb(dbUrl, dbUser, dbPass, dbSql, request.inputCount(), dbCfg.getTimeoutMs());
            }
            default -> throw new IllegalArgumentException("Unknown inputSource type: " + inputSourceType);
        };

        return rows;
    }

    public List<DataRow> readDataRowsFromDb(String jdbcUrl, String userName, String password,
                                             String sql, Integer inputCount, int timeoutMs) throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, userName, password);
             Statement  stmt = conn.createStatement()) {
            stmt.setQueryTimeout(Math.max(1, timeoutMs / 1000));
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<DataRow> rows = new ArrayList<>();
            int seq = 1;
            while (rs.next()) {
                if (inputCount != null && rows.size() >= inputCount) break;
                DataRow row = new DataRow();
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    row.getData().put(meta.getColumnLabel(i), val != null ? val : "");
                }
                row.getData().put("SEQUENCE_NUMBER", seq++);
                rows.add(row);
            }
            return rows;
        } catch (Exception e) {
            throw new IllegalArgumentException("DB input source query failed: " + e.getMessage(), e);
        }
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
        if (request.inputSource() == InputSourceType.ALIAS && (request.alias() == null || request.alias().isBlank()))
            throw new IllegalArgumentException("alias is required when inputSource=ALIAS");
        String rawAlias = request.alias();
        RunRequest rawRequest = request;
        request = resolveAlias(request);
        String operation = request.operation();
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);

        boolean isSpecific = request.inputSource() == InputSourceType.SPECIFIC;

        Map<String, String> opProperties = loadOperationProperties(op, request, rawRequest);
        BatchProperties.EnricherProperties enricherProps = op.getEnricher();

        int debugMode = request.debugMode() != null ? request.debugMode() : 0;

        injectMandatoryDefaults(rawRequest, rawAlias, op, opProperties);

        if (!isSpecific) {
            if (rawAlias != null && !rawAlias.isBlank())
                checkAliasMandatoryProperties(rawRequest, rawAlias, op, opProperties);
            else
                checkMandatoryProperties(request, op, opProperties);
        }

        applyPreEnricher(rows, enricherProps);
        if (request.inputCount() != null && request.inputCount() > 0)
            rows = rows.stream().limit(request.inputCount()).collect(Collectors.toList());
        rows = applySearchKeyword(rows, request.searchKeyword());
        rows = applyFilterInput(rows, request.filterInput());
        if (!isSpecific) checkMandatoryAttributes(rows, op);

        // Mode 2: return input rows after pre-enricher
        if (debugMode == 2) {
            BatchResult r = debugResult(rows);
            for (Map<String, Object> row : r.results()) rowCallback.accept(row);
            return CompletableFuture.completedFuture(
                    new BatchResult(r.processed(), r.succeeded(), r.failed(),
                            r.httpStats(), r.columns(), List.of(),
                            r.batchUuid(), r.timestamp(), r.timeTakenMs(), r.responseSizeKb(), opProperties));
        }

        // Mode 3: skip post-enricher
        Consumer<Map<String, Object>> effectiveCallback = rowCallback;
        if (debugMode == 0 && enricherProps != null && enricherProps.getType() == EnricherType.POST) {
            EnricherConfig cfg = enricherService.loadConfig(enricherProps.getEnhancer());
            Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
            effectiveCallback = row -> {
                enricherService.enrichRow(row, cfg.getData(), datasets);
                rowCallback.accept(row);
            };
        }
        if (request.cache() != null) {
            final CacheOutput cacheOut = request.cache();
            final Consumer<Map<String, Object>> prev = effectiveCallback;
            effectiveCallback = row -> { saveSingleRowToCache(row, cacheOut); prev.accept(row); };
        }

        String reqAuthHeaderAsync = buildRequestAuthHeader(request.auth(), operation, opProperties);
        return runCoreAsync(rows, op, operation,
                request.httpThreadCount(), request.httpTimeoutMs(), debugMode,
                request.filterOutput(), effectiveCallback, opProperties,
                request.inputHttpHeader() != null ? request.inputHttpHeader() : Map.of(), reqAuthHeaderAsync);
    }

    // -------------------------------------------------------------------------
    // Public API — convenience overloads kept for backward compatibility
    // -------------------------------------------------------------------------

    /** File-based run. */
    public BatchResult run(String filePath, Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(filePath, inputCount, op);
        return runCore(rows, op, operation, null, null, 0, Map.of(), Map.of(), null);
    }

    /** Direct list of identifiers — each ID becomes a DataRow with key "id". */
    public BatchResult run(List<String> identifiers, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromRequest(identifiers, null);
        return runCore(rows, op, operation, null, null, 0, Map.of(), Map.of(), null);
    }

    /** Runs from file and writes PSV output; returns summary metadata. */
    public PsvResult runToPsv(String inputFilePath, String outputFilePath,
                              Integer inputCount, String operation) throws Exception {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operation);
        List<DataRow> rows = readDataRowsFromFile(inputFilePath, inputCount, op);
        BatchResult batch = runCore(rows, op, operation, null, null, 0, Map.of(), Map.of(), null);
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
        return readDataRowsFromFile(filePath, inputCount, op, null);
    }

    public List<DataRow> readDataRowsFromFile(String filePath, Integer inputCount,
                                              BatchProperties.OperationProperties op,
                                              SearchKeyword keyword) throws Exception {
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
        headers = Arrays.stream(headers).map(s -> {
            String h = s.trim().toUpperCase();
            int bracket = h.indexOf('[');
            if (bracket >= 0) h = h.substring(0, bracket).trim();
            return h.replace(' ', '_');
        }).toArray(String[]::new);

        String delimPattern = Pattern.quote(delimiter);
        List<String> dataLines = allLines.stream()
                .skip(1).map(String::trim).filter(s -> !s.isBlank() && !s.startsWith("#"))
                .collect(Collectors.toList());

        if (keyword != null && keyword.getValue() != null && !keyword.getValue().isBlank()) {
            String kw   = keyword.getValue();
            String type = keyword.getType() != null ? keyword.getType().trim().toLowerCase() : "contains";
            dataLines = dataLines.stream().filter(line -> {
                String lower = line.toLowerCase();
                return switch (type) {
                    case "startswith" -> lower.startsWith(kw.toLowerCase());
                    case "endswith"   -> lower.endsWith(kw.toLowerCase());
                    case "regex"      -> line.matches(kw);
                    case "eq"         -> lower.equalsIgnoreCase(kw);
                    default           -> lower.contains(kw.toLowerCase());
                };
            }).collect(Collectors.toList());
        }

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

    /** Reads all entries from a named cache and produces one DataRow per entry. */
    public List<DataRow> readDataRowsFromCache(String cacheName) throws Exception {
        Map<String, CacheFactory.CacheEntry> entries = cacheFactory.getEntries(cacheName);
        if (entries.isEmpty()) return List.of();
        List<DataRow> rows = new ArrayList<>(entries.size());
        int seq = 1;
        for (CacheFactory.CacheEntry entry : entries.values()) {
            if (entry.value() == null) continue;
            Map<String, Object> parsed = objectMapper.readValue(
                    entry.value(), new TypeReference<Map<String, Object>>() {});
            DataRow row = new DataRow();
            row.getData().putAll(parsed);
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Loads an inputSource template from the inputSourceTemplate directory and returns DataRows.
     * The template file format is: {"name":"X","inputSource":{"type":"HTTPCONFIG","httpConfig":{...}}}
     */
    private List<DataRow> readDataRowsFromTemplate(String templateName) throws Exception {
        Path templateFile = inputSourceTemplateDir().resolve(templateName + ".json");
        if (!Files.exists(templateFile))
            throw new IllegalArgumentException("inputSource template not found: " + templateName);

        @SuppressWarnings("unchecked")
        Map<String, Object> templateMap = objectMapper.readValue(templateFile.toFile(), Map.class);
        Object inputSourceRaw = templateMap.get("inputSource");
        if (inputSourceRaw == null)
            throw new IllegalArgumentException("inputSource template '" + templateName + "' has no 'inputSource' field");

        BatchProperties.InputSourceProperties inputSource =
                objectMapper.convertValue(inputSourceRaw, BatchProperties.InputSourceProperties.class);
        return readDataRowsFromInputSource(inputSource);
    }

    /** Executes an inputSource configuration and returns the resulting DataRows. */
    public List<DataRow> readDataRowsFromInputSource(BatchProperties.InputSourceProperties inputSource) throws Exception {
        if (inputSource == null || inputSource.getType() == null)
            throw new IllegalArgumentException("inputSource type is required");
        return switch (inputSource.getType()) {
            case HTTPCONFIG, HTTP -> readDataRowsFromHttp(inputSource.getHttpConfig(), null);
            case DB -> {
                BatchProperties.DbConfigSourceProperties db = inputSource.getDbConfig();
                yield readDataRowsFromDb(db.getJdbcUrl(), db.getUserName(), db.getPassword(),
                        db.getSql(), null, db.getTimeoutMs());
            }
            default -> throw new IllegalArgumentException(
                    "inputSource type '" + inputSource.getType() + "' is not supported in standalone template test");
        };
    }

    private Path inputSourceTemplateDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("inputSourceTemplate");
    }

    private Path templateDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("operationTemplate");
    }

    /**
     * Applies an optional JSONata transform to a response object just before it is returned to the client.
     * Loads the expression from {@code classpath:transforms/{key}.jsonata} when {@code key} is set,
     * or uses {@code value} directly as the JSONata expression.
     * When {@code source} is set it is evaluated as JSONata against {@code response} to extract the
     * actual transform input; if the extracted value is a JSON string it is parsed first.
     */
    public Object applyJsonataTransform(Object response,
                                        com.mycompany.batch.model.JsonataTransform transform) throws Exception {
        if (transform == null) return response;

        Object jsonataInput = response;
        if (transform.source() != null && !transform.source().isBlank()) {
            String responseJson = objectMapper.writeValueAsString(response);
            Object extracted = Jsonata.jsonata(transform.source())
                    .evaluate(objectMapper.readValue(responseJson, Object.class));
            if (extracted instanceof String s) {
                try { jsonataInput = objectMapper.readValue(s, Object.class); }
                catch (Exception e) { jsonataInput = s; }
            } else {
                jsonataInput = extracted != null ? extracted : response;
            }
        }

        String jsonataExpr;
        if (transform.key() != null && !transform.key().isBlank()) {
            String resourcePath = "transforms/" + transform.key().trim() + ".jsonata";
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
                if (is == null)
                    throw new IllegalArgumentException("jsonata transform not found: classpath:" + resourcePath);
                jsonataExpr = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
            }
        } else if (transform.value() != null && !transform.value().isBlank()) {
            jsonataExpr = transform.value();
        } else {
            return jsonataInput;
        }

        String json = objectMapper.writeValueAsString(jsonataInput);
        Object result = Jsonata.jsonata(jsonataExpr).evaluate(objectMapper.readValue(json, Object.class));
        return result != null ? result : jsonataInput;
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
        return readDataRowsFromRaw(rawData, inputCount, null);
    }

    /**
     * Same as {@link #readDataRowsFromRaw(List, Integer)} but resolves {@code ${key}} / {@code {key}}
     * placeholders in string values against {@code opProperties} so that raw rows can reference
     * operation-level properties (e.g. {@code "id": "${zipCode}"} resolved from the request
     * {@code properties} map).
     */
    public List<DataRow> readDataRowsFromRaw(List<Map<String, Object>> rawData, Integer inputCount,
                                             Map<String, String> opProperties) {
        List<Map<String, Object>> list = inputCount != null
                ? rawData.stream().limit(inputCount).toList()
                : rawData;
        List<DataRow> rows = new ArrayList<>(list.size());
        int seq = 1;
        for (Map<String, Object> rawRow : list) {
            DataRow row = new DataRow();
            for (Map.Entry<String, Object> entry : rawRow.entrySet()) {
                Object val = entry.getValue();
                if (val instanceof String s && opProperties != null && !opProperties.isEmpty()
                        && (s.contains("${") || (s.contains("{") && s.contains("}")))) {
                    try { val = resolveTemplate(s, Map.of(), opProperties); } catch (Exception ignored) {}
                }
                row.getData().put(entry.getKey(), val);
            }
            row.getData().put("SEQUENCE_NUMBER", seq++);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Calls the configured HTTP endpoint and returns one DataRow per element in the
     * {@code "data"} array of the JSON response. Each row additionally contains {@code SEQUENCE_NUMBER}.
     */
    public List<DataRow> readDataRowsFromHttp(BatchProperties.HttpConfigSourceProperties config,
                                              Integer inputCount) throws Exception {
        return readDataRowsFromHttp(config, inputCount, null, null);
    }

    public List<DataRow> readDataRowsFromHttp(BatchProperties.HttpConfigSourceProperties config,
                                              Integer inputCount,
                                              String body,
                                              Map<String, String> extraHeaders) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(config.getUrl()));
        if (config.getMethod() == com.mycompany.batch.model.HttpMethod.POST) {
            String postBody = (body != null && !body.isBlank()) ? body : "";
            builder.POST(HttpRequest.BodyPublishers.ofString(postBody));
        } else {
            builder.GET();
        }
        if (extraHeaders != null) extraHeaders.forEach(builder::header);

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
                InputSourceType.from(params.get("inputSource")),
                params.get("inputFilePath"),
                params.get("inputHttpUrl"),
                null,   // inputHttpHeader — not supported via query string
                null,   // inputHttpBody   — not supported via query string
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
                null,   // searchKeyword
                null,   // cache
                null,   // executionMode
                params.get("alias"),
                params.get("responseProcessor"),
                null,   // appendOutput
                params.get("inputJsonPath"),
                params.get("cacheName"),
                null,   // properties
                null,   // jsonataTransform
                null,   // templateName
                null,   // auth
                null    // operationType
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

    /**
     * Derives the PSV column order from the first row (same logic as {@link #writeToPsv})
     * and writes the header line to the file, truncating any existing content.
     * Returns the ordered column list to pass to {@link #appendPsvRow}.
     */
    public List<String> initPsvStream(Map<String, Object> firstRow, String outputFilePath,
                                      boolean append) throws Exception {
        SequencedSet<String> cols = new LinkedHashSet<>(firstRow.keySet());
        cols.add("errorMessage");
        java.nio.file.OpenOption[] options = append
                ? new java.nio.file.OpenOption[]{java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND}
                : new java.nio.file.OpenOption[]{java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING};
        try (BufferedWriter w = Files.newBufferedWriter(Path.of(outputFilePath), options)) {
            w.write(String.join("|", cols));
            w.newLine();
        }
        return new ArrayList<>(cols);
    }

    /** Appends a single data row to a PSV file that was already initialised with {@link #initPsvStream}. */
    public void appendPsvRow(Map<String, Object> row, List<String> columns,
                             String outputFilePath) throws Exception {
        List<String> line = new ArrayList<>(columns.size());
        for (String col : columns) {
            Object val = row.get(col);
            line.add(val != null ? val.toString().replaceAll("[\\r\\n]", " ") : "");
        }
        try (BufferedWriter w = Files.newBufferedWriter(Path.of(outputFilePath),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
            w.write(String.join("|", line));
            w.newLine();
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
                                Map<String, String> opProperties,
                                Map<String, String> extraHeaders,
                                String authHeaderOverride) throws Exception {

        List<BatchProperties.ActivityProperties> allActivities =
                op.getActivity() != null ? op.getActivity() : List.of();
        List<BatchProperties.ActivityProperties> initActConfigs = allActivities.stream()
                .filter(a -> a.getType() == ActivityType.INITIALIZE)
                .collect(Collectors.toList());
        List<BatchProperties.ActivityProperties> activities = allActivities.stream()
                .filter(a -> a.getType() != ActivityType.INITIALIZE)
                .collect(Collectors.toList());
        boolean useActivities = !activities.isEmpty() || !initActConfigs.isEmpty();
        boolean useLegacy     = !useActivities
                && op.getHttp().getUrl() != null && !op.getHttp().getUrl().isBlank();

        AtomicInteger succeeded       = new AtomicInteger();
        AtomicInteger failed          = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();
        List<Long> httpDurationsMs    = new CopyOnWriteArrayList<>();
        AtomicLong totalResponseBytes = new AtomicLong();

        String authHeader = authHeaderOverride != null ? authHeaderOverride : authProviders.get(operation).getAuthorizationHeader();
        long batchStart   = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures;

        if (useActivities) {
            // --- Activity-based path ---

            // Determine effective thread count and timeout from the HTTP activity
            int activityHttpThreads = activities.stream()
                    .filter(a -> a.getType() == ActivityType.HTTP)
                    .findFirst()
                    .map(a -> a.getHttp().getThreadCount())
                    .orElse(op.getHttp().getThreadCount());
            int sshThreadCount = activities.stream()
                    .filter(a -> a.getType() == ActivityType.SSH)
                    .findFirst()
                    .map(a -> resolveSshThreadCount(a.getSsh(), opProperties))
                    .orElse(0);
            int effectiveThreadCount = threadCountOverride != null ? threadCountOverride
                    : Math.max(activityHttpThreads, sshThreadCount > 0 ? sshThreadCount : activityHttpThreads);

            int activityTimeoutMs = activities.stream()
                    .filter(a -> a.getType() == ActivityType.HTTP)
                    .findFirst()
                    .map(a -> a.getHttp().getTimeoutMs())
                    .orElse(op.getHttp().getTimeoutMs());
            int effectiveTimeoutMs = timeoutMsOverride != null ? timeoutMsOverride : activityTimeoutMs;

            int xpathThreadCount = activities.stream()
                    .filter(a -> a.getType() == ActivityType.DATAEXTRACTION)
                    .filter(a -> a.getDataExtraction().getType() == DataExtractionType.XPATH)
                    .findFirst()
                    .map(a -> a.getDataExtraction().getThreadCount())
                    .orElse(op.getXpath().getThreadCount());

            ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
            ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
            HttpClient httpClient     = buildHttpClient(operation);

            // Run INITIALIZE activities once before any per-row processing; errors propagate to the controller
            if (!initActConfigs.isEmpty()) {
                List<ResolvedActivity> resolvedInits = preloadActivities(initActConfigs, effectiveTimeoutMs, operation, opProperties);
                executeInitializeActivities(resolvedInits, httpClient, httpPool, authHeader, opProperties);
            }

            // Pre-load activity resources (classpath files, etc.) once for all rows
            List<ResolvedActivity> resolvedActivities = preloadActivities(activities, effectiveTimeoutMs, operation, opProperties);

            // Validate DB initialization connections once before row processing
            validateSharedDbConnections(op, opProperties);

            // Create shared SSH sessions (one per initialization reference) for SSH activities
            Map<String, Session> sshSessions = buildSharedSshSessions(activities, op, opProperties);

            boolean includeMetadata = false;

            futures = rows.stream()
                    .map(row -> processOneRowWithActivities(
                            row, resolvedActivities, httpClient, httpPool, xpathPool,
                            succeeded, failed, results, extraHeaders,
                            httpDurationsMs, totalResponseBytes,
                            authHeader, includeMetadata, opProperties, op, null, null, sshSessions))
                    .collect(Collectors.toList());

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long timeTakenMs = System.currentTimeMillis() - batchStart;
            httpPool.shutdown();
            xpathPool.shutdown();
            sshSessions.values().forEach(s -> { if (s != null && s.isConnected()) s.disconnect(); });

            LongSummaryStatistics stats = httpDurationsMs.stream().mapToLong(Long::longValue).summaryStatistics();
            HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                    : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

            List<Map<String, Object>> rawResults = new ArrayList<>(results);
            List<Map<String, Object>> sanitizedResults = sanitizeKeys(rawResults);
            if (debugMode < 3) sanitizedResults.forEach(r -> r.remove("operationStatus"));

            List<Object> validationErrors = sanitizedResults.stream()
                    .filter(r -> r.containsKey("Errors"))
                    .flatMap(r -> ((List<?>) r.get("Errors")).stream())
                    .collect(Collectors.toList());
            if (!validationErrors.isEmpty()) {
                Map<String, Object> errMap = new LinkedHashMap<>();
                errMap.put("Errors", validationErrors);
                throw new ValidationFailureException(errMap);
            }

            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());

            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            double responseSizeKb = totalResponseBytes.get() / 1024.0;

            return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb, opProperties);

        } else if (!useLegacy) {
            // --- Pass-through: no activities, no HTTP — return input rows as results ---
            long timeTakenMs = System.currentTimeMillis() - batchStart;
            for (DataRow row : rows) {
                Map<String, Object> resultMap = row.toResponseMap(false);
                resultMap.put("operationStatus", "SUCCESS");
                results.add(resultMap);
                succeeded.incrementAndGet();
            }
            List<Map<String, Object>> rawResults = new ArrayList<>(results);
            List<Map<String, Object>> sanitizedResults = sanitizeKeys(rawResults);
            if (debugMode < 3) sanitizedResults.forEach(r -> r.remove("operationStatus"));
            List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedResults);
            columns = applyColumnTemplate(columns, op.getColumnTemplate());
            String batchUuid = UUID.randomUUID().toString();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            return new BatchResult(rows.size(), succeeded.get(), 0,
                    new HttpStats(0, 0, 0), columns, sanitizedResults,
                    batchUuid, timestamp, timeTakenMs, 0, opProperties);

        } else {
            // --- Legacy path (no activity array defined) ---

            DataExtractionType extractionType = op.getDataExtraction().getType();

            List<XPathColumn> xpathColumns = List.of();
            Map<String, String> xpathMap   = Map.of();
            if (extractionType == DataExtractionType.XPATH) {
                xpathColumns = loadXPaths(op.getXpath().getConfig());
                xpathMap = xpathColumns.stream()
                        .collect(Collectors.toMap(XPathColumn::getColumnName, XPathColumn::getXpath,
                                (a, b) -> a, LinkedHashMap::new));
            }

            int effectiveThreadCount = threadCountOverride != null ? threadCountOverride : op.getHttp().getThreadCount();
            int effectiveTimeoutMs   = timeoutMsOverride   != null ? timeoutMsOverride   : op.getHttp().getTimeoutMs();

            ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
            ExecutorService xpathPool = Executors.newFixedThreadPool(op.getXpath().getThreadCount());
            HttpClient httpClient     = buildHttpClient(operation);

            final String resolvedBodyTemplate = resolveJsonataExpression(op.getHttp().getBodyTemplate());

            if (extractionType == DataExtractionType.JSON || extractionType == DataExtractionType.JSONATA) {
                final String jsonataTransform = resolveJsonataExpression(op.getDataExtraction().getJsonataTransform());
                futures = rows.stream()
                        .map(row -> processOneJson(row.getData(), httpClient, httpPool, jsonataTransform,
                                succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                                authHeader, op, resolvedBodyTemplate, effectiveTimeoutMs, extraHeaders))
                        .collect(Collectors.toList());
            } else {
                final Map<String, String> finalXpathMap = Map.copyOf(xpathMap);
                futures = rows.stream()
                        .map(row -> processOneXpath(row.getData(), httpClient, httpPool, finalXpathMap, xpathPool,
                                succeeded, failed, results, httpDurationsMs, totalResponseBytes,
                                authHeader, op, resolvedBodyTemplate, effectiveTimeoutMs, extraHeaders))
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
                    httpStats, columns, sanitizedResults, batchUuid, timestamp, timeTakenMs, responseSizeKb, opProperties);
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
            Map<String, String> opProperties,
            Map<String, String> extraHeaders,
            String authHeaderOverride) throws Exception {

        List<BatchProperties.ActivityProperties> allActivitiesAsync =
                op.getActivity() != null ? op.getActivity() : List.of();
        List<BatchProperties.ActivityProperties> initActConfigsAsync = allActivitiesAsync.stream()
                .filter(a -> a.getType() == ActivityType.INITIALIZE)
                .collect(Collectors.toList());
        List<BatchProperties.ActivityProperties> activities = allActivitiesAsync.stream()
                .filter(a -> a.getType() != ActivityType.INITIALIZE)
                .collect(Collectors.toList());
        boolean useLegacyAsync = activities.isEmpty()
                && op.getHttp().getUrl() != null && !op.getHttp().getUrl().isBlank();

        // Pass-through: no activities, no HTTP — stream rows directly
        if (activities.isEmpty() && !useLegacyAsync && initActConfigsAsync.isEmpty()) {
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
                    timeTakenMs, 0, opProperties));
        }

        AtomicInteger succeeded       = new AtomicInteger();
        AtomicInteger failed          = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();   // for column-def derivation
        List<Long> httpDurationsMs    = new CopyOnWriteArrayList<>();
        AtomicLong totalResponseBytes = new AtomicLong();

        String authHeader = authHeaderOverride != null ? authHeaderOverride : authProviders.get(operation).getAuthorizationHeader();
        long batchStart   = System.currentTimeMillis();

        int activityHttpThreads = activities.stream()
                .filter(a -> a.getType() == ActivityType.HTTP)
                .findFirst()
                .map(a -> a.getHttp().getThreadCount())
                .orElse(op.getHttp().getThreadCount());
        int sshThreadCountAsync = activities.stream()
                .filter(a -> a.getType() == ActivityType.SSH)
                .findFirst()
                .map(a -> resolveSshThreadCount(a.getSsh(), opProperties))
                .orElse(0);
        int effectiveThreadCount = threadCountOverride != null ? threadCountOverride
                : Math.max(activityHttpThreads, sshThreadCountAsync > 0 ? sshThreadCountAsync : activityHttpThreads);

        int activityTimeoutMs = activities.stream()
                .filter(a -> a.getType() == ActivityType.HTTP)
                .findFirst()
                .map(a -> a.getHttp().getTimeoutMs())
                .orElse(op.getHttp().getTimeoutMs());
        int effectiveTimeoutMs = timeoutMsOverride != null ? timeoutMsOverride : activityTimeoutMs;

        int xpathThreadCount = activities.stream()
                .filter(a -> a.getType() == ActivityType.DATAEXTRACTION)
                .filter(a -> a.getDataExtraction().getType() == DataExtractionType.XPATH)
                .findFirst()
                .map(a -> a.getDataExtraction().getThreadCount())
                .orElse(op.getXpath().getThreadCount());

        ExecutorService httpPool  = Executors.newFixedThreadPool(effectiveThreadCount);
        ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
        HttpClient httpClient     = HttpClient.newBuilder().build();

        // Run INITIALIZE activities once before any per-row processing; errors propagate to the controller
        if (!initActConfigsAsync.isEmpty()) {
            List<ResolvedActivity> resolvedInits = preloadActivities(initActConfigsAsync, effectiveTimeoutMs, operation, opProperties);
            executeInitializeActivities(resolvedInits, httpClient, httpPool, authHeader, opProperties);
        }

        List<ResolvedActivity> resolvedActivities = preloadActivities(activities, effectiveTimeoutMs, operation, opProperties);

        // Validate DB initialization connections once before row processing
        validateSharedDbConnections(op, opProperties);

        Map<String, Session> sshSessionsAsync = buildSharedSshSessions(activities, op, opProperties);

        boolean includeMetadata = false;
        Consumer<Map<String, Object>> effectiveCallback = debugMode < 3
                ? row -> { row.remove("operationStatus"); rowCallback.accept(row); }
                : rowCallback;

        List<CompletableFuture<Void>> futures = rows.stream()
                .map(row -> processOneRowWithActivities(
                        row, resolvedActivities, httpClient, httpPool, xpathPool,
                        succeeded, failed, results, extraHeaders,
                        httpDurationsMs, totalResponseBytes,
                        authHeader, includeMetadata, opProperties, op,
                        filterOutput, effectiveCallback, sshSessionsAsync))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    long timeTakenMs = System.currentTimeMillis() - batchStart;
                    httpPool.shutdown();
                    xpathPool.shutdown();
                    sshSessionsAsync.values().forEach(s -> { if (s != null && s.isConnected()) s.disconnect(); });

                    LongSummaryStatistics stats = httpDurationsMs.stream()
                            .mapToLong(Long::longValue).summaryStatistics();
                    HttpStats httpStats = httpDurationsMs.isEmpty() ? new HttpStats(0, 0, 0)
                            : new HttpStats(stats.getMin(), stats.getMax(), stats.getAverage());

                    List<Map<String, Object>> sanitizedForCols = sanitizeKeys(new ArrayList<>(results));

                    // Apply post-enrichment so enriched attributes appear in column defs
                    try { applyPostEnricher(sanitizedForCols, op.getEnricher()); } catch (Exception ignored) {}

                    List<ColumnDef> columns = buildColumnDefsFromResults(sanitizedForCols);
                    try { columns = applyColumnTemplate(columns, op.getColumnTemplate()); }
                    catch (Exception ignored) {}

                    String batchUuid = UUID.randomUUID().toString();
                    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                    double responseSizeKb = totalResponseBytes.get() / 1024.0;

                    return new BatchResult(rows.size(), succeeded.get(), failed.get(),
                            httpStats, columns, List.of(), batchUuid, timestamp, timeTakenMs, responseSizeKb, opProperties);
                });
    }

    // -------------------------------------------------------------------------
    // INITIALIZE activity execution
    // -------------------------------------------------------------------------

    /**
     * Executes each INITIALIZE activity once (sequentially) before per-row processing begins.
     * Extracted fields from the activity response are merged into {@code opProperties} so that
     * subsequent per-row activities can reference them via {@code ${FIELD}} placeholders.
     * Any failure throws immediately — the exception propagates to the controller, which renders
     * the standard error response.
     */
    private void executeInitializeActivities(
            List<ResolvedActivity> resolvedInits,
            HttpClient httpClient,
            ExecutorService httpPool,
            String authHeader,
            Map<String, String> opProperties) throws Exception {
        for (ResolvedActivity initAct : resolvedInits) {
            boolean isDb = initAct.config().getDb() != null
                    && !initAct.config().getDb().getJdbcUrl().isBlank();
            DataRow initRow = new DataRow();
            try {
                DataRow result;
                if (isDb) {
                    result = executeDbInitActivity(initAct, opProperties);
                } else {
                    result = executeHttpActivity(
                            initRow, initAct, httpClient, httpPool,
                            new java.util.ArrayList<>(), new AtomicLong(),
                            authHeader, opProperties, Map.of()
                    ).get();
                }
                result.getData().forEach((k, v) -> {
                    if (v != null) opProperties.put(k, v.toString());
                });
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                String msg = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                throw new RuntimeException("INITIALIZE activity '" + initAct.config().getName() + "' failed: " + msg, cause);
            }
        }
    }

    private DataRow executeDbInitActivity(ResolvedActivity activity,
                                          Map<String, String> opProperties) throws Exception {
        BatchProperties.DbProperties dbConfig = activity.config().getDb();
        Map<String, String> effectiveProps = new LinkedHashMap<>(opProperties);
        if (activity.config().getProperties() != null) effectiveProps.putAll(activity.config().getProperties());

        String resolvedUrl  = resolveTemplate(dbConfig.getJdbcUrl(),  Map.of(), effectiveProps);
        String resolvedUser = resolveTemplate(dbConfig.getUserName(), Map.of(), effectiveProps);
        String resolvedPass = resolveTemplate(dbConfig.getPassword(), Map.of(), effectiveProps);

        DataRow row = new DataRow();
        try (Connection conn = DriverManager.getConnection(resolvedUrl, resolvedUser, resolvedPass);
             Statement  stmt = conn.createStatement()) {
            stmt.setQueryTimeout(Math.max(1, dbConfig.getTimeoutMs() / 1000));
            if (!dbConfig.getSql().isBlank()) {
                String resolvedSql = resolveTemplate(dbConfig.getSql(), Map.of(), effectiveProps);
                ResultSet         rs   = stmt.executeQuery(resolvedSql);
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                if (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        Object val = rs.getObject(i);
                        row.getData().put(meta.getColumnLabel(i), val != null ? val : "");
                    }
                }
            }
        }
        return row;
    }

    // Activity pre-loading
    // -------------------------------------------------------------------------

    /**
     * Pre-loads classpath/filesystem resources for each activity so they are resolved once
     * rather than on every row. Returns one {@link ResolvedActivity} per input activity.
     */
    private List<ResolvedActivity> preloadActivities(
            List<BatchProperties.ActivityProperties> activities,
            int defaultTimeoutMs) throws Exception {
        return preloadActivities(activities, defaultTimeoutMs, null, null);
    }

    private List<ResolvedActivity> preloadActivities(
            List<BatchProperties.ActivityProperties> activities,
            int defaultTimeoutMs,
            String operationName) throws Exception {
        return preloadActivities(activities, defaultTimeoutMs, operationName, null);
    }

    private List<ResolvedActivity> preloadActivities(
            List<BatchProperties.ActivityProperties> activities,
            int defaultTimeoutMs,
            String operationName,
            Map<String, String> opProperties) throws Exception {
        List<ResolvedActivity> resolved = new ArrayList<>(activities.size());
        for (BatchProperties.ActivityProperties act : activities) {
            ActivityType type = act.getType();
            String activityAuthHeader = null;
            if ((type == ActivityType.HTTP || type == ActivityType.INITIALIZE) && operationName != null) {
                BatchProperties.AuthProperties actAuth = act.getHttp() != null ? act.getHttp().getAuth() : null;
                HttpAuthProvider actProvider = null;
                // When opProperties are available, build a fresh per-request provider so that
                // ${USERNAME}/${PASSWORD} placeholders in the activity auth config are resolved
                // from the current request's properties rather than the startup-time literals.
                if (actAuth != null && actAuth.getMethod() != com.mycompany.batch.model.AuthMethod.NONE
                        && opProperties != null) {
                    try {
                        String actUrl = act.getHttp() != null ? act.getHttp().getUrl() : null;
                        actProvider = buildAuthProvider(act.getName(), actAuth, actUrl, opProperties);
                    } catch (IllegalStateException e) {
                        // Unresolved ${VAR} or missing credentials — surface to the caller as a parameter error
                        throw new IllegalArgumentException(
                                "Activity '" + act.getName() + "' auth configuration error: " + e.getMessage());
                    } catch (Exception ignored) {
                        // Token fetch failures, etc. — fall through to the static startup provider
                    }
                }
                if (actProvider == null) {
                    actProvider = authProviders.get(operationName + "::" + act.getName());
                }
                if (actProvider != null) {
                    try {
                        activityAuthHeader = actProvider.getAuthorizationHeader();
                    } catch (Exception e) {
                        // token fetch failed at preload time
                    }
                }
            }
            if (type == ActivityType.HTTP || type == ActivityType.INITIALIZE) {
                String rawBt = act.getHttp() != null ? act.getHttp().getBodyTemplate() : null;
                // If the path itself has ${VAR} placeholders, keep it unloaded — resolved per-row at execute time
                String bodyTemplate = (rawBt != null && rawBt.contains("${"))
                        ? rawBt
                        : resolveJsonataExpression(rawBt);
                resolved.add(new ResolvedActivity(act, bodyTemplate, null, null, null, activityAuthHeader, null, null, null));
            } else if (type == ActivityType.DATAEXTRACTION) {
                DataExtractionType extractType = act.getDataExtraction().getType();
                if (extractType == DataExtractionType.XPATH) {
                    String config = act.getDataExtraction().getConfig();
                    if (config == null || config.isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "': dataExtraction.config is required for XPATH extraction");
                    }
                    Map<String, String> xpathMap = loadXPathMap(config);
                    resolved.add(new ResolvedActivity(act, null, xpathMap, null, null, null, null, null, null));
                } else if (extractType == DataExtractionType.JSONPATH) {
                    String config = act.getDataExtraction().getConfig();
                    if (config == null || config.isBlank()) {
                        throw new IllegalStateException(
                                "Activity '" + act.getName() + "': dataExtraction.config is required for JSONPATH extraction");
                    }
                    List<JsonPathColumn> cols = loadJsonPathColumns(config);
                    resolved.add(new ResolvedActivity(act, null, null, null, cols, null, null, null, null));
                } else {
                    // JSON or JSONATA — JSONata transform
                    String transform = resolveJsonataExpression(act.getDataExtraction().getJsonataTransform());
                    resolved.add(new ResolvedActivity(act, null, null, transform, null, null, null, null, null));
                }
            } else if (type == ActivityType.TRANSFORM) {
                List<BatchProperties.TransformStepProperties> stepConfigs = act.getTransforms();
                List<ResolvedTransformStep> steps = new ArrayList<>(stepConfigs.size());
                for (BatchProperties.TransformStepProperties s : stepConfigs) {
                    boolean isNone = "NONE".equalsIgnoreCase(s.getType().trim());
                    String expr = isNone ? null : resolveJsonataExpression(s.getJsonataTransform());
                    steps.add(new ResolvedTransformStep(s.getSource(), isNone, expr, s.getOnError(), s.getOutput()));
                }
                resolved.add(new ResolvedActivity(act, null, null, null, null, null, steps, null, null));
            } else if (type == ActivityType.DB) {
                resolved.add(new ResolvedActivity(act, null, null, null, null, null, null, null, null));
            } else if (type == ActivityType.DATAENRICHER) {
                String file = act.getDataEnricher().getFile();
                EnricherConfig enricherCfg = enricherService.loadConfig(file);
                Map<String, Map<String, Map<String, Object>>> staticDatasets = enricherService.loadDatasets(enricherCfg);
                resolved.add(new ResolvedActivity(act, null, null, null, null, null, null, enricherCfg, staticDatasets));
            } else {
                resolved.add(new ResolvedActivity(act, null, null, null, null, null, null, null, null));
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
            Map<String, String> extraHeaders,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader,
            boolean includeMetadata,
            Map<String, String> opProperties,
            BatchProperties.OperationProperties op,
            List<FilterRule> filterOutput,          // nullable — only used when rowCallback != null
            Consumer<Map<String, Object>> rowCallback, // nullable — ASYNC streaming
            Map<String, Session> sharedSshSessions) { // shared SSH sessions keyed by reference name

        // Group activities into sequential stages; activities sharing a sequence number run in parallel
        CompletableFuture<DataRow> chain = CompletableFuture.completedFuture(inputRow);

        for (List<ResolvedActivity> stage : buildStages(activities)) {
            if (stage.size() == 1) {
                ResolvedActivity activity = stage.get(0);
                chain = chain.thenCompose(row -> executeOneActivity(
                        activity, row, httpClient, httpPool, xpathPool,
                        httpDurationsMs, totalResponseBytes, authHeader, opProperties, extraHeaders,
                        op, sharedSshSessions));
            } else {
                List<ResolvedActivity> parallelStage = stage;
                chain = chain.thenCompose(row -> executeParallelStage(
                        row, parallelStage, httpClient, httpPool, xpathPool,
                        httpDurationsMs, totalResponseBytes, authHeader, opProperties, extraHeaders,
                        op, sharedSshSessions));
            }
        }

        return chain.thenAccept(row -> {
            int httpStatus = row.getLastHttpStatusCode();
            boolean httpSuccess = httpStatus == 0 || (httpStatus >= 200 && httpStatus < 300);
            String opStatus = httpSuccess ? "SUCCESS" : "FAILURE";
            List<Map<String, Object>> expandedRows = row.getExpandedRows();
            if (expandedRows != null && !expandedRows.isEmpty()) {
                // JSON extraction produced multiple output rows — expand them
                for (Map<String, Object> expandedData : expandedRows) {
                    Map<String, Object> resultMap = row.toResponseMap(includeMetadata);
                    resultMap.putAll(expandedData);
                    resultMap.put("operationStatus", opStatus);
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
                resultMap.put("operationStatus", opStatus);
                results.add(resultMap);
                if (rowCallback != null) {
                    Map<String, Object> sanitized = sanitizeRow(resultMap);
                    if (matchesAll(sanitized, filterOutput != null ? filterOutput : List.of())) {
                        rowCallback.accept(sanitized);
                    }
                }
            }
            if (httpSuccess) {
                succeeded.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }
        }).exceptionally(ex -> {
            failed.incrementAndGet();
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            Map<String, Object> resultMap;
            if (cause instanceof ValidationException ve) {
                Map<String, Object> errorEntry = new LinkedHashMap<>();
                errorEntry.put("activity", ve.getActivityName());
                errorEntry.put("message",  ve.getMessage());
                resultMap = new LinkedHashMap<>();
                resultMap.put("Errors", List.of(errorEntry));
            } else {
                String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                resultMap = new LinkedHashMap<>(inputRow.getData());
                resultMap.put("operationStatus", "FAILED");
                resultMap.put("errorMessage", message);
            }
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

    // -------------------------------------------------------------------------
    // Stage grouping + per-activity dispatch helpers
    // -------------------------------------------------------------------------

    /**
     * Groups resolved activities into ordered execution stages.
     * Consecutive activities that share the same non-null {@code sequence} number form a parallel stage.
     * Activities with a null sequence (or a unique sequence number) each form their own sequential stage.
     */
    private static List<List<ResolvedActivity>> buildStages(List<ResolvedActivity> activities) {
        List<List<ResolvedActivity>> stages = new ArrayList<>();
        List<ResolvedActivity> current = new ArrayList<>();
        Integer currentSeq = null;
        for (ResolvedActivity act : activities) {
            Integer seq = act.config().getSequence();
            if (seq != null && seq.equals(currentSeq)) {
                current.add(act);
            } else {
                if (!current.isEmpty()) stages.add(new ArrayList<>(current));
                current.clear();
                current.add(act);
                currentSeq = seq;
            }
        }
        if (!current.isEmpty()) stages.add(current);
        return stages;
    }

    /** Dispatches a single activity, then applies outputVar post-processing if configured. */
    private CompletableFuture<DataRow> executeOneActivity(
            ResolvedActivity activity, DataRow row,
            HttpClient httpClient, ExecutorService httpPool, ExecutorService xpathPool,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader, Map<String, String> opProperties, Map<String, String> extraHeaders,
            BatchProperties.OperationProperties op, Map<String, Session> sharedSshSessions) {

        ActivityType type = activity.config().getType();
        CompletableFuture<DataRow> future;
        if (type == ActivityType.HTTP) {
            future = executeHttpActivity(row, activity, httpClient, httpPool,
                    httpDurationsMs, totalResponseBytes, authHeader, opProperties, extraHeaders);
        } else if (type == ActivityType.DATAEXTRACTION) {
            future = executeExtractionActivity(row, activity, xpathPool);
        } else if (type == ActivityType.DB) {
            future = executeDbActivity(row, activity, httpPool, opProperties, op);
        } else if (type == ActivityType.SSH) {
            future = executeSshActivity(row, activity, httpPool, opProperties, op, sharedSshSessions);
        } else if (type == ActivityType.TRANSFORM) {
            future = executeTransformActivity(row, activity);
        } else if (type == ActivityType.VALIDATION) {
            future = executeValidationActivity(row, activity);
        } else if (type == ActivityType.DATAENRICHER) {
            future = executeDataEnricherActivity(row, activity);
        } else {
            future = CompletableFuture.completedFuture(row);
        }

        String outputVar = activity.config().getOutputVar();
        if (outputVar != null && !outputVar.isBlank()) {
            String outputKey = activity.config().getOutputKey();
            return future.thenApply(r -> storeOutputVar(r, outputVar, outputKey));
        }
        return future;
    }

    /**
     * Runs all activities in a parallel stage concurrently against the same DataRow.
     * Parallel activities should use distinct {@code outputVar} names to avoid write conflicts.
     */
    private CompletableFuture<DataRow> executeParallelStage(
            DataRow row, List<ResolvedActivity> activities,
            HttpClient httpClient, ExecutorService httpPool, ExecutorService xpathPool,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader, Map<String, String> opProperties, Map<String, String> extraHeaders,
            BatchProperties.OperationProperties op, Map<String, Session> sharedSshSessions) {

        List<CompletableFuture<DataRow>> futures = activities.stream()
                .map(act -> executeOneActivity(act, row, httpClient, httpPool, xpathPool,
                        httpDurationsMs, totalResponseBytes, authHeader, opProperties, extraHeaders,
                        op, sharedSshSessions))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> row);
    }

    /**
     * If an activity produced {@code expandedRows}, indexes them by {@code outputKey} and stores the
     * resulting dataset in {@code row.datasets} under {@code outputVar}. Clears {@code expandedRows}
     * so the outer pipeline does not expand them into separate result rows.
     */
    private static DataRow storeOutputVar(DataRow row, String outputVar, String outputKey) {
        List<Map<String, Object>> expandedRows = row.getExpandedRows();
        if (expandedRows != null && !expandedRows.isEmpty()) {
            Map<String, Map<String, Object>> indexed = new LinkedHashMap<>();
            for (Map<String, Object> r : expandedRows) {
                String keyVal = (outputKey != null && !outputKey.isBlank() && r.containsKey(outputKey))
                        ? String.valueOf(r.get(outputKey))
                        : String.valueOf(indexed.size());
                indexed.put(keyVal, r);
            }
            row.getDatasets().put(outputVar, indexed);
            row.setExpandedRows(null);
        }
        return row;
    }

    /** Enriches the current DataRow using the pre-loaded enricher config and merged datasets. */
    private CompletableFuture<DataRow> executeDataEnricherActivity(
            DataRow row, ResolvedActivity activity) {
        try {
            EnricherConfig cfg = activity.enricherConfig();
            if (cfg == null) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException(
                        "DataEnricher activity '" + activity.config().getName() + "': enricher config was not pre-loaded"));
                return f;
            }
            // Merge static (pre-loaded) datasets with per-row datasets from previous outputVar activities
            Map<String, Map<String, Map<String, Object>>> allDatasets = new LinkedHashMap<>();
            if (activity.enricherStaticDatasets() != null) allDatasets.putAll(activity.enricherStaticDatasets());
            allDatasets.putAll(row.getDatasets());

            enricherService.enrichRow(row.getData(), cfg.getData(), allDatasets);
            return CompletableFuture.completedFuture(row);
        } catch (Exception e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException(
                    "DataEnricher activity '" + activity.config().getName() + "' failed: " + e.getMessage(), e));
            return f;
        }
    }

    /** Makes the HTTP call for one DataRow, captures timing + URL in metadata, stores response body. */
    private CompletableFuture<DataRow> executeHttpActivity(
            DataRow row,
            ResolvedActivity activity,
            HttpClient httpClient,
            ExecutorService httpPool,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String authHeader,
            Map<String, String> opProperties,
            Map<String, String> extraHeaders) {

        BatchProperties.HttpProperties httpConfig = activity.config().getHttp();
        Map<String, String> effectiveProps = new LinkedHashMap<>(opProperties);
        if (activity.config().getProperties() != null) effectiveProps.putAll(activity.config().getProperties());

        // Validate mandatory activity properties against row data and effective properties;
        // inject defaultValue into effectiveProps when the property is absent
        List<BatchProperties.MandatoryPropertyDef> mandatoryActDefs = activity.config().getMandatoryProperties();
        if (!mandatoryActDefs.isEmpty()) {
            List<String> missing = new java.util.ArrayList<>();
            for (BatchProperties.MandatoryPropertyDef def : mandatoryActDefs) {
                String p = def.getProperty();
                if (p == null || p.isBlank()) continue;
                Object rowVal = row.getData().get(p);
                if (rowVal != null && !String.valueOf(rowVal).isBlank()) continue;
                String propVal = effectiveProps.get(p);
                if (propVal != null && !propVal.isBlank()) continue;
                if (def.getDefaultValue() != null) {
                    effectiveProps.put(p, resolveDefaultValue(def.getDefaultValue()));
                } else {
                    missing.add(p);
                }
            }
            if (!missing.isEmpty()) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalArgumentException(
                        "Activity '" + activity.config().getName() + "' missing mandatory properties: " + missing));
                return f;
            }
        }

        String resolvedUrl;
        String resolvedBody;
        try {
            // Two-pass: resolve ${inputHttpUrl} first, then resolve any dataRow vars embedded in that URL.
            String urlTemplate = resolveTemplate(httpConfig.getUrl(), row.getData(), effectiveProps);
            resolvedUrl = resolveTemplate(urlTemplate, row.getData(), effectiveProps);
            String bt = activity.resolvedBodyTemplate();
            if (bt == null) {
                resolvedBody = "";
            } else {
                // Resolve any ${VAR} in the path itself first, then load if still a classpath: ref
                String resolvedBt = resolveTemplate(bt, row.getData(), effectiveProps);
                if (resolvedBt.startsWith("classpath:")) {
                    String resource = resolvedBt.substring("classpath:".length());
                    try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                        if (is == null) throw new IllegalArgumentException(
                                "bodyTemplate classpath resource not found: " + resource);
                        resolvedBt = new String(is.readAllBytes()).strip();
                    }
                    // Resolve ${VAR} inside the file content
                    resolvedBody = resolveTemplate(resolvedBt, row.getData(), effectiveProps);
                } else {
                    resolvedBody = resolvedBt;
                }
            }
        } catch (IllegalArgumentException e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        } catch (Exception e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException(e.getMessage(), e));
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
                String cached = cacheFactory.get(cacheName, resolvedCacheKey,
                                                 resolveMaxRetentionTime(cacheConfig.getMaxRetentionTime(), row.getData(), effectiveProps));
                if (cached != null) {
                    totalResponseBytes.addAndGet(cached.length());
                    row.setResponseBody(cached);
                    row.putNamedOutput(activityName + ".RESPONSEBODY", cached);
                    applyHttpExtract(httpConfig.getExtract().getFields(), null, resolvedBody, null, cached, resolvedUrl, 0L, row);
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

        if (!resolvedUrl.startsWith("http://") && !resolvedUrl.startsWith("https://"))
            return executeLocalHttpActivity(row, activity, resolvedUrl, resolvedBody,
                    httpDurationsMs, totalResponseBytes, cacheName, resolvedCacheKey, httpConfig);

        String effectiveAuthHeader = activity.activityAuthHeader() != null
                ? activity.activityAuthHeader() : authHeader;

        // Support NTLM credentials injected per-row via request properties (from the HTTP page)
        String ntlmUser = effectiveProps.get("ntlmUsername");
        String ntlmPass = effectiveProps.get("ntlmPassword");
        HttpClient effectiveClient = (ntlmUser != null && !ntlmUser.isBlank()
                && ntlmPass != null && !ntlmPass.isBlank())
                ? HttpClient.newBuilder()
                        .authenticator(new NtlmAuthProvider(ntlmUser, ntlmPass).toAuthenticator())
                        .build()
                : httpClient;

        Map<String, String> resolvedExtraHeaders = (extraHeaders == null || extraHeaders.isEmpty()) ? extraHeaders
                : extraHeaders.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> { try { return resolveTemplate(e.getValue(), row.getData(), effectiveProps); } catch (Exception ex) { return e.getValue(); } },
                        (a, b) -> b, LinkedHashMap::new));
        Map<String, String> resolvedConfigHeaders = (httpConfig.getHeader() == null || httpConfig.getHeader().isEmpty()) ? httpConfig.getHeader()
                : httpConfig.getHeader().entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> { try { return resolveTemplate(e.getValue(), row.getData(), effectiveProps); } catch (Exception ex) { return e.getValue(); } },
                        (a, b) -> b, LinkedHashMap::new));
        HttpRequest request = buildRequestFromHttpConfig(resolvedUrl, effectiveAuthHeader, httpConfig, resolvedBody,
                httpConfig.getTimeoutMs(), httpConfig.getTimeoutMs(), resolvedExtraHeaders, resolvedConfigHeaders);

        final String finalCacheName        = cacheName;
        final String finalResolvedCacheKey = resolvedCacheKey;
        final String finalResolvedUrl      = resolvedUrl;
        final String finalResolvedBody     = resolvedBody;

        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            try {
                HttpResponse<String> response = effectiveClient.send(request, HttpResponse.BodyHandlers.ofString());
                long responseTimeMs = System.currentTimeMillis() - start;
                String body = response.body();
                row.setLastHttpStatusCode(response.statusCode());
                row.setResponseBody(body);
                row.putNamedOutput(activityName + ".RESPONSEBODY", body);
                BatchProperties.HttpExtractProperties extract = httpConfig.getExtract();
                applyHttpExtract(extract.getFields(), request, finalResolvedBody, response, body, finalResolvedUrl, responseTimeMs, row);
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    if (extract != null && !extract.getIfError().isEmpty()) {
                        applyHttpExtract(extract.getIfError(), request, finalResolvedBody, response, body, finalResolvedUrl, responseTimeMs, row);
                    } else if (extract == null || extract.getFields().isEmpty()) {
                        throw new RuntimeException("HTTP " + response.statusCode()
                                + (body != null && !body.isBlank() ? " — " + body : ""));
                    }
                    return row;
                }
                totalResponseBytes.addAndGet(body.length());
                if (finalCacheName != null) {
                    cacheFactory.save(finalCacheName, finalResolvedCacheKey, body, finalResolvedUrl);
                }
                return row;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while calling HTTP endpoint: " + finalResolvedUrl, e);
            } catch (Exception e) {
                String detail = e.getMessage();
                if (detail == null || detail.isBlank()) detail = e.getClass().getSimpleName();
                throw new RuntimeException("HTTP activity failed for '" + finalResolvedUrl + "': " + detail, e);
            } finally {
                long elapsed = System.currentTimeMillis() - start;
                httpDurationsMs.add(elapsed);
                row.getMetadata().put(activityName + ".timetakenmillis", elapsed);
                row.getMetadata().put(activityName + ".httpurl", finalResolvedUrl);
            }
        }, httpPool);
    }

    /** Executes a JDBC query for one DataRow and stores the result set as JSON in the response body. */
    private CompletableFuture<DataRow> executeDbActivity(
            DataRow row,
            ResolvedActivity activity,
            ExecutorService pool,
            Map<String, String> opProperties,
            BatchProperties.OperationProperties op) {

        BatchProperties.DbProperties dbConfig = activity.config().getDb();
        Map<String, String> effectiveProps = new LinkedHashMap<>(opProperties);
        if (activity.config().getProperties() != null) effectiveProps.putAll(activity.config().getProperties());

        // If the activity references a named DB initialization entry, use its credentials
        String rawJdbcUrl  = dbConfig.getJdbcUrl();
        String rawUserName = dbConfig.getUserName();
        String rawPassword = dbConfig.getPassword();
        String ref = dbConfig.getReference();
        if (ref != null && !ref.isBlank() && op != null && op.getInitialization() != null) {
            BatchProperties.InitializationProperties initEntry = op.getInitialization().stream()
                    .filter(i -> ref.equalsIgnoreCase(i.getName()) && "DB".equalsIgnoreCase(i.getType()))
                    .findFirst().orElse(null);
            if (initEntry != null) {
                if (rawJdbcUrl.isBlank())  rawJdbcUrl  = initEntry.getDb().getJdbcUrl();
                if (rawUserName.isBlank()) rawUserName = initEntry.getDb().getUserName();
                if (rawPassword.isBlank()) rawPassword = initEntry.getDb().getPassword();
            }
        }
        final String effectiveJdbcUrl  = rawJdbcUrl;
        final String effectiveUserName = rawUserName;
        final String effectivePassword = rawPassword;

        String resolvedSql;
        String resolvedUrl;
        try {
            String sqlTemplate = !dbConfig.getSql().isBlank()
                    ? dbConfig.getSql()
                    : effectiveProps.getOrDefault("sql", "");
            resolvedSql = resolveTemplate(sqlTemplate, row.getData(), effectiveProps);
            resolvedUrl = resolveTemplate(effectiveJdbcUrl, row.getData(), effectiveProps);
        } catch (IllegalArgumentException e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        if (resolvedSql.isBlank()) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException(
                    "DB activity '" + activity.config().getName() + "': 'sql' property is required"));
            return f;
        }

        String activityName = activity.config().getName();
        BatchProperties.CacheProperties cacheConfig = dbConfig.getCache();
        String cacheName        = null;
        String resolvedCacheKey = null;
        if (cacheConfig != null && !cacheConfig.getName().isBlank()) {
            try {
                cacheName        = cacheConfig.getName();
                resolvedCacheKey = resolveTemplate(cacheConfig.getKey(), row.getData(), effectiveProps);
                String cached = cacheFactory.get(cacheName, resolvedCacheKey,
                        resolveMaxRetentionTime(cacheConfig.getMaxRetentionTime(), row.getData(), effectiveProps));
                if (cached != null) {
                    row.setResponseBody(cached);
                    row.getMetadata().put(activityName + ".timetakenmillis", 0L);
                    row.getMetadata().put(activityName + ".sql", resolvedSql + " [CACHED]");
                    return CompletableFuture.completedFuture(row);
                }
            } catch (IllegalArgumentException e) {
                CompletableFuture<DataRow> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        }

        final String finalCacheName        = cacheName;
        final String finalResolvedCacheKey = resolvedCacheKey;
        final String finalResolvedSql      = resolvedSql;
        final String finalResolvedUrl      = resolvedUrl;

        BatchProperties.DbExtractProperties extractConfig = dbConfig.getExtract();

        String preResolvedUser;
        String preResolvedPass;
        try {
            preResolvedUser = resolveTemplate(effectiveUserName, row.getData(), effectiveProps);
            preResolvedPass = resolveTemplate(effectivePassword, row.getData(), effectiveProps);
        } catch (IllegalArgumentException e) {
            preResolvedUser = effectiveUserName;
            preResolvedPass = effectivePassword;
        }
        final String finalResolvedUser = preResolvedUser;
        final String finalResolvedPass = preResolvedPass;

        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(finalResolvedUrl, finalResolvedUser, finalResolvedPass);
                 Statement  stmt = conn.createStatement()) {
                stmt.setQueryTimeout(Math.max(1, dbConfig.getTimeoutMs() / 1000));
                ResultSet         rs       = stmt.executeQuery(finalResolvedSql);
                ResultSetMetaData meta     = rs.getMetaData();
                int               colCount = meta.getColumnCount();

                List<Map<String, Object>> queryRows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> qRow = new LinkedHashMap<>();
                    for (int i = 1; i <= colCount; i++) {
                        qRow.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    queryRows.add(qRow);
                }

                String body = objectMapper.writeValueAsString(queryRows);
                if (finalCacheName != null) {
                    cacheFactory.save(finalCacheName, finalResolvedCacheKey, body, finalResolvedSql);
                }
                row.setResponseBody(body);
                row.putNamedOutput(activityName + ".RESPONSEBODY", body);
                if (!queryRows.isEmpty()) {
                    row.setExpandedRows(queryRows);
                }
                return row;
            } catch (Exception e) {
                if (extractConfig != null && !extractConfig.getIfError().isEmpty()) {
                    for (Map.Entry<String, String> entry : extractConfig.getIfError().entrySet()) {
                        String key  = entry.getKey();
                        String path = entry.getValue();
                        switch (path) {
                            case "$.statusCode" -> row.getData().put(key, e instanceof java.sql.SQLException sqle
                                    ? sqle.getErrorCode() : -1);
                            case "$.error"      -> row.getData().put(key, e.getMessage() != null
                                    ? e.getMessage() : e.getClass().getSimpleName());
                            case "$.userName"   -> row.getData().put(key, dbConfig.getUserName());
                            case "$.password"   -> row.getData().put(key, dbConfig.getPassword());
                            case "$.jdbcUrl"    -> row.getData().put(key, finalResolvedUrl);
                        }
                    }
                    return row;
                }
                throw new RuntimeException("DB query failed: " + e.getMessage(), e);
            } finally {
                long elapsed = System.currentTimeMillis() - start;
                row.getMetadata().put(activityName + ".timetakenmillis", elapsed);
                row.getMetadata().put(activityName + ".sql", finalResolvedSql);
            }
        }, pool);
    }

    private CompletableFuture<DataRow> executeSshActivity(
            DataRow row,
            ResolvedActivity activity,
            ExecutorService pool,
            Map<String, String> opProperties,
            BatchProperties.OperationProperties op,
            Map<String, Session> sharedSessions) {

        BatchProperties.SshProperties sshConfig = activity.config().getSsh();
        Map<String, String> effectiveProps = new LinkedHashMap<>(opProperties);
        if (activity.config().getProperties() != null) effectiveProps.putAll(activity.config().getProperties());

        String resolvedFile;
        String host = null;
        String portStr = null;
        String username = null;
        String privateKey = null;

        try {
            resolvedFile = resolveTemplate(sshConfig.getFile(), row.getData(), effectiveProps);
            // Second pass: resolve any ${VAR} that appeared inside the first-pass result
            if (resolvedFile.contains("${") || resolvedFile.contains("{")) {
                resolvedFile = resolveTemplate(resolvedFile, row.getData(), effectiveProps);
            }

            String ref = sshConfig.getReference();
            // Always load connection details so we can reconnect if the shared session drops
            BatchProperties.InitializationProperties initEntry = op.getInitialization() == null ? null
                    : op.getInitialization().stream()
                            .filter(ip -> ip.getName().equalsIgnoreCase(ref))
                            .findFirst().orElse(null);
            if (initEntry == null && (sharedSessions == null || !sharedSessions.containsKey(ref))) {
                throw new IllegalArgumentException(
                        "SSH activity '" + activity.config().getName() + "': initialization reference '"
                                + ref + "' not found");
            }
            if (initEntry != null && initEntry.getSsh() != null) {
                BatchProperties.SshConnectionProperties conn = initEntry.getSsh();
                host       = resolveTemplate(conn.getHost(),       row.getData(), effectiveProps);
                portStr    = resolveTemplate(conn.getPort(),       row.getData(), effectiveProps);
                username   = resolveTemplate(conn.getUsername(),   row.getData(), effectiveProps);
                privateKey = resolveTemplate(conn.getPrivateKey(), row.getData(), effectiveProps);
            }
        } catch (IllegalArgumentException e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        final String ref = sshConfig.getReference();
        // Use shared session only if it is still connected; otherwise fall back to per-row session
        final Session sharedSession = (sharedSessions != null && sharedSessions.containsKey(ref)
                && sharedSessions.get(ref) != null && sharedSessions.get(ref).isConnected())
                ? sharedSessions.get(ref) : null;

        int port = 22;
        if (portStr != null) {
            try { port = Integer.parseInt(portStr.trim()); } catch (NumberFormatException ignored) {}
        }

        final int    finalPort       = port;
        final String finalHost       = host;
        final String finalUsername   = username;
        final String finalPrivateKey = privateKey;
        final String finalFile       = resolvedFile;
        final String activityName    = activity.config().getName();
        final int    timeoutMs       = sshConfig.getTimeoutMs();

        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            Session session = sharedSession;
            // Re-check connectivity inside the async block; the session may have dropped
            // between buildSharedSshSessions() and this thread's execution
            boolean ownSession = session == null || !session.isConnected();
            if (ownSession) session = null;
            try {
                if (ownSession) {
                    if (finalHost == null) {
                        throw new IllegalStateException(
                                "Shared SSH session '" + ref + "' is disconnected and no init config is available to reconnect");
                    }
                    JSch jsch = new JSch();
                    jsch.addIdentity(finalPrivateKey);
                    session = jsch.getSession(finalUsername, finalHost, finalPort);
                    session.setConfig("StrictHostKeyChecking", "no");
                    session.setConfig("PreferredAuthentications", "publickey");
                    session.connect(timeoutMs);
                }

                String safeFile = finalFile.replace("'", "'\"'\"'");
                String cmd = "P='" + safeFile + "'; "
                        + "if [ -e \"$P\" ]; then "
                        + "if [ -L \"$P\" ]; then FT=symlink; "
                        + "elif [ -f \"$P\" ]; then FT=file; "
                        + "elif [ -d \"$P\" ]; then FT=directory; "
                        + "else FT=other; fi; "
                        + "MT=$(stat -c%Y \"$P\" 2>/dev/null || stat -f%m \"$P\" 2>/dev/null || echo 0); "
                        + "HR=$(date -d \"@${MT}\" '+%Y-%m-%d %H:%M:%S' 2>/dev/null "
                        +     "|| date -r \"${MT}\" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo ''); "
                        + "SZ=0; RC=0; "
                        + "if [ \"$FT\" = file ]; then "
                        + "SZ=$(wc -c < \"$P\" 2>/dev/null || echo 0); "
                        + "RC=$(wc -l < \"$P\" 2>/dev/null || echo 0); fi; "
                        + "printf '{\"fileExists\":true,\"fileType\":\"%s\",\"fileSize\":%d,"
                        + "\"fileLastModifiedTime\":%d,\"fileLastModifiedTimeHumanReadable\":\"%s\","
                        + "\"fileRowCount\":%d}\\n' \"$FT\" $SZ $MT \"$HR\" $RC; "
                        + "else "
                        + "printf '{\"fileExists\":false,\"fileType\":\"unknown\",\"fileSize\":0,"
                        + "\"fileLastModifiedTime\":0,\"fileLastModifiedTimeHumanReadable\":\"\","
                        + "\"fileRowCount\":0}\\n'; fi";

                ChannelExec channel = (ChannelExec) session.openChannel("exec");
                channel.setCommand(cmd);
                channel.setInputStream(null);
                java.io.InputStream in = channel.getInputStream();
                try {
                    channel.connect(timeoutMs);
                } catch (com.jcraft.jsch.JSchException channelEx) {
                    // Shared session may be half-open (JSch still reports isConnected=true but
                    // the server has silently dropped it). Reconnect with a fresh per-row session.
                    try { channel.disconnect(); } catch (Exception ignored) {}
                    if (!ownSession && finalHost != null) {
                        if (session != null && session.isConnected()) {
                            try { session.disconnect(); } catch (Exception ignored) {}
                        }
                        JSch retryJsch = new JSch();
                        retryJsch.addIdentity(finalPrivateKey);
                        session = retryJsch.getSession(finalUsername, finalHost, finalPort);
                        session.setConfig("StrictHostKeyChecking", "no");
                        session.setConfig("PreferredAuthentications", "publickey");
                        session.connect(timeoutMs);
                        ownSession = true;
                        channel = (ChannelExec) session.openChannel("exec");
                        channel.setCommand(cmd);
                        channel.setInputStream(null);
                        in = channel.getInputStream();
                        channel.connect(timeoutMs);
                    } else {
                        throw channelEx;
                    }
                }

                StringBuilder sb = new StringBuilder();
                byte[] buf = new byte[1024];
                int n;
                while ((n = in.read(buf)) != -1) sb.append(new String(buf, 0, n, java.nio.charset.StandardCharsets.UTF_8));
                channel.disconnect();

                String body = sb.toString().trim();
                row.setResponseBody(body);

                Map<String, String> extractFields = sshConfig.getExtract().getFields();
                for (Map.Entry<String, String> entry : extractFields.entrySet()) {
                    String key  = entry.getKey();
                    String path = entry.getValue();
                    if (path == null) continue;
                    try {
                        Object val = com.jayway.jsonpath.JsonPath.read(body, path);
                        row.getData().put(key, val);
                    } catch (Exception ignored) {}
                }

                return row;
            } catch (Exception e) {
                throw new RuntimeException("SSH activity '" + activityName + "' failed: " + e.getMessage(), e);
            } finally {
                long elapsed = System.currentTimeMillis() - start;
                row.getMetadata().put(activityName + ".timetakenmillis", elapsed);
                if (finalHost != null) row.getMetadata().put(activityName + ".sshhost", finalHost);
                row.getMetadata().put(activityName + ".file", finalFile);
                row.getData().put(activityName + ".file", finalFile);
                // Only disconnect if we own this session (not a shared one)
                if (ownSession && session != null && session.isConnected()) session.disconnect();
            }
        }, pool);
    }

    /** Resolves the SSH threadCount string (may contain ${VAR}) to an integer. */
    private int resolveSshThreadCount(BatchProperties.SshProperties sshConfig,
                                      Map<String, String> opProperties) {
        String raw = sshConfig.getThreadCountRaw();
        if (raw == null || raw.isBlank()) return 5;
        if (raw.contains("$") || raw.contains("{")) {
            try {
                raw = resolveTemplate(raw, Map.of(), opProperties);
            } catch (Exception ignored) {}
        }
        try { return Integer.parseInt(raw.trim()); }
        catch (NumberFormatException e) { return 5; }
    }

    /** Creates one shared JSch Session per SSH initialization reference found in the activity list. */
    private Map<String, Session> buildSharedSshSessions(
            List<BatchProperties.ActivityProperties> activities,
            BatchProperties.OperationProperties op,
            Map<String, String> opProperties) {
        Map<String, Session> sessions = new java.util.LinkedHashMap<>();
        for (BatchProperties.ActivityProperties act : activities) {
            if (act.getType() != ActivityType.SSH) continue;
            String ref = act.getSsh().getReference();
            if (ref == null || ref.isBlank() || sessions.containsKey(ref)) continue;
            BatchProperties.InitializationProperties initEntry = op.getInitialization() == null ? null
                    : op.getInitialization().stream()
                            .filter(ip -> ip.getName().equalsIgnoreCase(ref))
                            .findFirst().orElse(null);
            if (initEntry == null || initEntry.getSsh() == null) continue;
            BatchProperties.SshConnectionProperties conn = initEntry.getSsh();
            try {
                String host       = resolveTemplate(conn.getHost(),       Map.of(), opProperties);
                String portStr    = resolveTemplate(conn.getPort(),       Map.of(), opProperties);
                String username   = resolveTemplate(conn.getUsername(),   Map.of(), opProperties);
                String privateKey = resolveTemplate(conn.getPrivateKey(), Map.of(), opProperties);
                int port = 22;
                try { port = Integer.parseInt(portStr.trim()); } catch (NumberFormatException ignored) {}
                int timeout = act.getSsh().getTimeoutMs();
                JSch jsch = new JSch();
                jsch.addIdentity(privateKey);
                Session s = jsch.getSession(username, host, port);
                s.setConfig("StrictHostKeyChecking", "no");
                s.setConfig("PreferredAuthentications", "publickey");
                s.connect(timeout);
                sessions.put(ref, s);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create shared SSH session for '" + ref + "': " + e.getMessage(), e);
            }
        }
        return sessions;
    }

    /**
     * Opens and immediately closes one JDBC connection per DB initialization entry to verify
     * connectivity before row processing begins. Throws on the first unreachable database.
     */
    private void validateSharedDbConnections(
            BatchProperties.OperationProperties op,
            Map<String, String> opProperties) throws Exception {
        if (op.getInitialization() == null) return;
        for (BatchProperties.InitializationProperties init : op.getInitialization()) {
            if (!"DB".equalsIgnoreCase(init.getType())) continue;
            BatchProperties.DbConnectionProperties dbConn = init.getDb();
            String url  = resolveTemplate(dbConn.getJdbcUrl(),  Map.of(), opProperties);
            String user = resolveTemplate(dbConn.getUserName(), Map.of(), opProperties);
            String pass = resolveTemplate(dbConn.getPassword(), Map.of(), opProperties);
            if (url.isBlank()) throw new IllegalStateException(
                    "DB initialization '" + init.getName() + "': jdbcUrl is required");
            try (Connection conn = DriverManager.getConnection(url, user, pass)) {
                // connection established successfully — close it immediately
            } catch (Exception e) {
                throw new RuntimeException(
                        "DB initialization '" + init.getName() + "' failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Applies the http.extract map to the DataRow.
     * Special paths: $.statusCode, $.body/$.responseBody, $.requestBody, $.url,
     *                $.requestHeaders, $.responseHeaders, $.headers (alias for $.responseHeaders).
     * Any other path is evaluated as JSONPath against the response body.
     */

    private CompletableFuture<DataRow> executeLocalHttpActivity(
            DataRow row, ResolvedActivity activity,
            String localUrl, String requestBody,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes,
            String cacheName, String resolvedCacheKey,
            BatchProperties.HttpProperties httpConfig) {
        long start = System.currentTimeMillis();
        try {
            String body = callLocalEndpointToJson(localUrl);
            long responseTimeMs = System.currentTimeMillis() - start;
            row.setLastHttpStatusCode(200);
            row.setResponseBody(body);
            totalResponseBytes.addAndGet(body.length());
            String activityName = activity.config().getName();
            applyHttpExtract(httpConfig.getExtract().getFields(), null, requestBody, null, body, localUrl, responseTimeMs, row);
            httpDurationsMs.add(responseTimeMs);
            row.getMetadata().put(activityName + ".timetakenmillis", responseTimeMs);
            row.getMetadata().put(activityName + ".httpurl", localUrl + " [LOCAL]");
            if (cacheName != null)
                cacheFactory.save(cacheName, resolvedCacheKey, body, localUrl);
            return CompletableFuture.completedFuture(row);
        } catch (Exception e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException(
                    "Local endpoint call failed for '" + localUrl + "': " + e.getMessage(), e));
            return f;
        }
    }

    private String callLocalEndpointToJson(String url) throws Exception {
        int q = url.indexOf('?');
        String path = (q >= 0 ? url.substring(0, q) : url).trim();
        Map<String, String> params = parseQueryString(url);
        if (path.endsWith("/run")) {
            String operation = params.get("operation");
            if (operation == null || operation.isBlank())
                throw new IllegalArgumentException(
                        "Local URL must include an 'operation' query parameter: " + url);
            String idsParam = params.get("ids");
            List<String> ids = null;
            if (idsParam != null && !idsParam.isBlank()) {
                ids = Arrays.stream(idsParam.split(","))
                        .map(String::trim).filter(s -> !s.isBlank()).toList();
            }
            RunRequest inner = new RunRequest(
                    operation,
                    InputSourceType.from(params.get("inputSource")),
                    params.get("inputFilePath"),
                    params.get("inputHttpUrl"),
                    null, null,
                    ids,
                    null,
                    parseLocalInt(params.get("inputCount")),
                    null, null,
                    parseLocalInt(params.get("debugMode")),
                    parseLocalInt(params.get("httpThreadCount")),
                    parseLocalInt(params.get("httpTimeoutMs")),
                    null, null, null, null, null,
                    params.get("alias"),
                    params.get("responseProcessor"),
                    null,
                    params.get("inputJsonPath"),
                    params.get("cacheName"),
                    null, null, null,
                    null,   // auth
                    null    // operationType
            );
            BatchResult result = run(inner);
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("data", result.results());
            return objectMapper.writeValueAsString(resp);
        }
        // Template endpoints: /batch/operationTemplate, /batch/operationTemplate/{name}, /batch/operationTemplate/{name}/run
        int templateIdx = path.indexOf("/operationTemplate");
        if (templateIdx >= 0) {
            String afterTemplate = path.substring(templateIdx + "/operationTemplate".length());
            if (afterTemplate.isEmpty() || afterTemplate.equals("/")) {
                String name = params.get("name");
                if (name != null && !name.isBlank()) {
                    Map<String, String> extraProps = new LinkedHashMap<>(params);
                    extraProps.remove("name");
                    return runLocalTemplate(name.trim(), extraProps);
                }
                return listLocalTemplates();
            } else if (afterTemplate.endsWith("/run")) {
                String name = afterTemplate.substring(1, afterTemplate.length() - 4);
                return runLocalTemplate(name, new LinkedHashMap<>(params));
            } else {
                String name = afterTemplate.substring(1);
                return getLocalTemplateContent(name);
            }
        }

        throw new IllegalArgumentException(
                "Unsupported local URL path '" + path + "' — supported: /batch/run?operation=..., /batch/operationTemplate, /batch/operationTemplate/{name}, /batch/operationTemplate/{name}/run");
    }

    private Path localTemplateDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", "");
        String templateDir = serverPropertiesLoader.getProperties().getOrDefault("TEMPLATEDIR",
                dataDir.isBlank() ? "." : Path.of(dataDir).resolve("operationTemplate").toString());
        return Path.of(templateDir);
    }

    private String listLocalTemplates() throws Exception {
        Path dir = localTemplateDir();
        List<Map<String, Object>> templates = new ArrayList<>();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                List<Path> files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
                for (Path p : files) {
                    String templateName = p.getFileName().toString().replaceAll("\\.json$", "");
                    Object templateBody = objectMapper.readValue(p.toFile(), Object.class);
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("templateName", templateName);
                    entry.put("templateBody", templateBody);
                    templates.add(entry);
                }
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", templates);
        return objectMapper.writeValueAsString(response);
    }

    private String getLocalTemplateContent(String name) throws Exception {
        if (!name.matches("[\\w\\-.]+")) throw new IllegalArgumentException("invalid template name: " + name);
        Path file = localTemplateDir().resolve(name + ".json");
        if (!Files.exists(file)) throw new IllegalArgumentException("template not found: " + name);
        Object content = objectMapper.readValue(file.toFile(), Object.class);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name", name);
        response.put("content", content);
        return objectMapper.writeValueAsString(response);
    }

    private String runLocalTemplate(String name, Map<String, String> extraProps) throws Exception {
        if (!name.matches("[\\w\\-.]+")) throw new IllegalArgumentException("invalid template name: " + name);
        Path file = localTemplateDir().resolve(name + ".json");
        if (!Files.exists(file)) throw new IllegalArgumentException("template not found: " + name);
        Map<String, Object> templateMap = objectMapper.readValue(file.toFile(),
                new TypeReference<Map<String, Object>>() {});
        extraProps.forEach(templateMap::put);
        if (!extraProps.isEmpty()) {
            Object rawBody = templateMap.get("inputHttpBody");
            if (rawBody != null) {
                String bodyStr = rawBody instanceof String s ? s : objectMapper.writeValueAsString(rawBody);
                try {
                    Map<String, Object> bodyMap = objectMapper.readValue(bodyStr,
                            new TypeReference<Map<String, Object>>() {});
                    extraProps.forEach(bodyMap::put);
                    templateMap.put("inputHttpBody", objectMapper.writeValueAsString(bodyMap));
                } catch (Exception ignored) {}
            }
        }
        RunRequest request = resolveAlias(objectMapper.convertValue(templateMap, RunRequest.class));
        BatchResult result = run(request);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("data", result.results());
        return objectMapper.writeValueAsString(resp);
    }

    private void applyHttpExtract(Map<String, String> extractMap,
                                  HttpRequest httpRequest,
                                  String requestBody,
                                  HttpResponse<String> response,
                                  String body,
                                  String resolvedUrl,
                                  long responseTimeMs,
                                  DataRow row) {
        if (extractMap == null || extractMap.isEmpty()) return;
        for (Map.Entry<String, String> entry : extractMap.entrySet()) {
            String key  = entry.getKey();
            String path = entry.getValue();
            if (path == null) continue;
            switch (path) {
                case "$.statusCode"                  -> row.getData().put(key, response != null ? response.statusCode() : 200);
                case "$.body", "$.responseBody"      -> row.getData().put(key, body);
                case "$.requestBody"                 -> row.getData().put(key, requestBody);
                case "$.url"                         -> row.getData().put(key, resolvedUrl);
                case "$.requestHeaders"              -> row.getData().put(key, httpRequest != null ? httpRequest.headers().map().toString() : "");
                case "$.responseHeaders", "$.headers"-> row.getData().put(key, response != null ? response.headers().map().toString() : "");
                case "$.responseTimeMs"              -> row.getData().put(key, responseTimeMs);
                default -> {
                    try {
                        Object val = com.jayway.jsonpath.JsonPath.read(body, path);
                        row.getData().put(key, val);
                    } catch (Exception ignored) {}
                }
            }
        }
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

        DataExtractionType extractionType = activity.config().getDataExtraction().getType();

        if (extractionType == DataExtractionType.XPATH) {
            return xpathExtractor.extractAsync(responseBody, activity.xpathMap(), xpathPool)
                    .thenApply(attributes -> {
                        row.getData().putAll(attributes);
                        return row;
                    });
        }

        if (extractionType == DataExtractionType.JSON || extractionType == DataExtractionType.JSONATA) {
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

        if (extractionType == DataExtractionType.JSONPATH) {
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
                "Unknown extraction type: " + extractionType));
        return f;
    }

    /** Executes a TRANSFORM activity: runs a chain of steps, each optionally capturing its result as a named variable. */
    private CompletableFuture<DataRow> executeTransformActivity(DataRow row, ResolvedActivity activity) {
        String activityName = activity.config().getName();
        List<ResolvedTransformStep> steps = activity.transformSteps();
        if (steps == null || steps.isEmpty()) {
            return CompletableFuture.completedFuture(row);
        }
        try {
            Object current = null;
            Map<String, Object> localVars = new LinkedHashMap<>();

            for (ResolvedTransformStep step : steps) {
                Object sourceValue = resolveTransformSource(step.source(), current, localVars, row);

                Object result;
                if (step.isNone() || step.jsonataExpression() == null) {
                    result = sourceValue;
                } else {
                    result = applyTransformStep(sourceValue, step.jsonataExpression(), step.onError(), sourceValue);
                }

                current = result;

                if (step.output() != null && !step.output().isBlank()) {
                    localVars.put(step.output(), result);
                    row.putNamedOutput(activityName + "." + step.output(), result);
                }
            }

            // Store the final result as responseBody so a following DATAEXTRACTION activity can consume it
            if (current != null) {
                String finalBody = current instanceof String s ? s : objectMapper.writeValueAsString(current);
                row.setResponseBody(finalBody);
                row.putNamedOutput(activityName + ".RESPONSEBODY", finalBody);
            }

            return CompletableFuture.completedFuture(row);
        } catch (Exception e) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException(
                    "TRANSFORM activity '" + activityName + "' failed: " + e.getMessage(), e));
            return f;
        }
    }

    private CompletableFuture<DataRow> executeValidationActivity(DataRow row, ResolvedActivity activity) {
        BatchProperties.ValidationProperties validation = activity.config().getValidation();
        if (validation == null) return CompletableFuture.completedFuture(row);
        BatchProperties.ConditionGroupProperties group = validation.getConditions();
        if (group == null || group.getCondition() == null || group.getCondition().isEmpty()) {
            return CompletableFuture.completedFuture(row);
        }
        boolean matched = evaluateConditionGroup(group, row);
        if (matched) {
            CompletableFuture<DataRow> f = new CompletableFuture<>();
            f.completeExceptionally(new ValidationException(
                    activity.config().getName(), validation.getErrorMessage()));
            return f;
        }
        return CompletableFuture.completedFuture(row);
    }

    private boolean evaluateConditionGroup(BatchProperties.ConditionGroupProperties group, DataRow row) {
        String groupType = group.getType() != null ? group.getType().toUpperCase() : "AND";
        for (BatchProperties.ConditionProperties cond : group.getCondition()) {
            boolean result = evaluateSingleCondition(cond, row);
            if ("OR".equals(groupType) && result) return true;
            if ("AND".equals(groupType) && !result) return false;
        }
        return "AND".equals(groupType);
    }

    private boolean evaluateSingleCondition(BatchProperties.ConditionProperties cond, DataRow row) {
        if (cond.getColumn() == null) return false;
        String columnRef = cond.getColumn();
        if (columnRef.startsWith("$fe.")) columnRef = columnRef.substring(4);
        Object found = row.getData().get(columnRef);
        if (found == null) {
            for (Map.Entry<String, Object> e : row.getData().entrySet()) {
                if (e.getKey().equalsIgnoreCase(columnRef)) { found = e.getValue(); break; }
            }
        }
        if (found == null) return false;
        String rowValue  = Objects.toString(found, "");
        String condValue = cond.getValue() != null ? cond.getValue() : "";
        String op        = cond.getOp() != null ? cond.getOp().toLowerCase() : "eq";
        return switch (op) {
            case "like"      -> rowValue.matches(condValue);
            case "neq", "ne" -> !rowValue.equals(condValue);
            default          -> rowValue.equals(condValue);
        };
    }

    /**
     * Resolves the {@code source} reference for a transform step.
     * <ul>
     *   <li>{@code null} or {@code "$"} — result of the previous step.</li>
     *   <li>{@code "$varName"} (no dot) — variable captured by a previous step's {@code output} in this transform.</li>
     *   <li>{@code "$activityName.key"} — named output stored by another activity (response body or output variable).</li>
     * </ul>
     */
    private Object resolveTransformSource(String source, Object previousResult,
                                          Map<String, Object> localVars, DataRow row) {
        if (source == null || source.equals("$")) return previousResult;
        if (!source.startsWith("$")) return source;
        String ref = source.substring(1); // strip leading $
        int dot = ref.indexOf('.');
        if (dot >= 0) {
            // $activityName.key  →  look up "activityName.key" in DataRow named outputs
            return row.getNamedOutput(ref);
        }
        // $varName  →  local transform variable, then DataRow named outputs
        Object local = localVars.get(ref);
        return local != null ? local : row.getNamedOutput(ref);
    }

    /** Applies a pre-loaded JSONATA expression to {@code sourceValue}, honouring {@code onError}. */
    private Object applyTransformStep(Object sourceValue, String jsonataExpr,
                                      String onError, Object fallback) throws Exception {
        try {
            String json = objectMapper.writeValueAsString(sourceValue);
            Object result = Jsonata.jsonata(jsonataExpr).evaluate(objectMapper.readValue(json, Object.class));
            return result != null ? result : sourceValue;
        } catch (Exception e) {
            if (onError == null) throw e;
            if ("SKIP".equalsIgnoreCase(onError.trim())) return fallback;
            // "$.key" — write the error message into a map under that key
            String key = onError.trim().startsWith("$.") ? onError.trim().substring(2) : onError.trim();
            Map<String, Object> errMap = new LinkedHashMap<>();
            errMap.put(key, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return errMap;
        }
    }

    // -------------------------------------------------------------------------
    // Legacy per-record processing (used when no activity array is defined)
    // -------------------------------------------------------------------------

    private CompletableFuture<Void> processOneXpath(
            Map<String, Object> row, HttpClient httpClient, ExecutorService httpPool,
            Map<String, String> xpaths, ExecutorService xpathPool,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results,
            List<Long> httpDurationsMs, AtomicLong totalResponseBytes, String authHeader,
            BatchProperties.OperationProperties op, String resolvedBodyTemplate, int timeoutMs,
            Map<String, String> extraHeaders) {

        HttpRequest request;
        try {
            request = buildRequest(row, authHeader, op, resolvedBodyTemplate, timeoutMs, extraHeaders);
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
            BatchProperties.OperationProperties op, String resolvedBodyTemplate, int timeoutMs,
            Map<String, String> extraHeaders) {

        HttpRequest request;
        try {
            request = buildRequest(row, authHeader, op, resolvedBodyTemplate, timeoutMs, extraHeaders);
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
                                     String resolvedBodyTemplate, int timeoutMs,
                                     Map<String, String> extraHeaders) {
        BatchProperties.HttpProperties http = op.getHttp();
        String resolvedUrl  = resolveTemplate(http.getUrl(), row);
        String resolvedBody = resolvedBodyTemplate != null ? resolveTemplate(resolvedBodyTemplate, row) : "";
        Map<String, String> resolvedExtraHeaders = (extraHeaders == null || extraHeaders.isEmpty()) ? extraHeaders
                : extraHeaders.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> { try { return resolveTemplate(e.getValue(), row); } catch (Exception ex) { return e.getValue(); } },
                        (a, b) -> b, LinkedHashMap::new));
        return buildRequestFromHttpConfig(resolvedUrl, authHeader, http, resolvedBody, timeoutMs,
                http.getTimeoutMs(), resolvedExtraHeaders);
    }

    private HttpRequest buildRequestFromHttpConfig(
            String resolvedUrl, String authHeader,
            BatchProperties.HttpProperties http, String resolvedBody,
            int timeoutMs, int ignored) {
        return buildRequestFromHttpConfig(resolvedUrl, authHeader, http, resolvedBody, timeoutMs, ignored, null);
    }

    private HttpRequest buildRequestFromHttpConfig(
            String resolvedUrl, String authHeader,
            BatchProperties.HttpProperties http, String resolvedBody,
            int timeoutMs, int ignored,
            Map<String, String> extraHeaders) {
        return buildRequestFromHttpConfig(resolvedUrl, authHeader, http, resolvedBody, timeoutMs, ignored, extraHeaders, http.getHeader());
    }

    private HttpRequest buildRequestFromHttpConfig(
            String resolvedUrl, String authHeader,
            BatchProperties.HttpProperties http, String resolvedBody,
            int timeoutMs, int ignored,
            Map<String, String> extraHeaders, Map<String, String> resolvedConfigHeaders) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .timeout(Duration.ofMillis(timeoutMs))
                .uri(URI.create(resolvedUrl));

        if (http.getMethod() == HttpMethod.GET) {
            builder.GET();
        } else {
            boolean ctInHeaders = (resolvedConfigHeaders != null && resolvedConfigHeaders.keySet().stream()
                        .anyMatch(k -> k.equalsIgnoreCase("content-type")))
                    || (extraHeaders != null && extraHeaders.keySet().stream()
                        .anyMatch(k -> k.equalsIgnoreCase("content-type")));
            String ct = http.getContentType();
            if (ct != null && !ctInHeaders) {
                builder.header("Content-Type", ct);
            }
            builder.POST(HttpRequest.BodyPublishers.ofString(resolvedBody != null ? resolvedBody : ""));
        }
        if (resolvedConfigHeaders != null) resolvedConfigHeaders.forEach(builder::header);
        // extraHeaders override activity-level headers
        if (extraHeaders != null) extraHeaders.forEach(builder::header);
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
        if (props == null || props.getType() != EnricherType.PRE) return;
        EnricherConfig cfg = enricherService.loadConfig(props.getEnhancer());
        Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
        rows.forEach(row -> enricherService.enrichRow(row.getData(), cfg.getData(), datasets));
    }

    private void applyPostEnricher(List<Map<String, Object>> results,
                                    BatchProperties.EnricherProperties props) throws Exception {
        if (props == null || props.getType() != EnricherType.POST) return;
        EnricherConfig cfg = enricherService.loadConfig(props.getEnhancer());
        Map<String, Map<String, Map<String, Object>>> datasets = enricherService.loadDatasets(cfg);
        results.forEach(row -> enricherService.enrichRow(row, cfg.getData(), datasets));
    }

    // -------------------------------------------------------------------------
    // Response processor
    // -------------------------------------------------------------------------

    public List<String> getResponseProcessorNames() {
        return new ArrayList<>(responseProcessorRegistry.keySet());
    }

    private List<BatchProperties.ResponseProcessorProperties> resolveResponseProcessor(String name, String operationName) {
        if (name == null || name.isBlank()) return null;
        // Global registry takes priority
        List<BatchProperties.ResponseProcessorProperties> procs = responseProcessorRegistry.get(name.trim());
        if (procs != null) return procs;
        // Fall back to operation-level processor definitions
        if (operationName != null && !operationName.isBlank()) {
            try {
                BatchProperties.OperationProperties op = batchProperties.getOperation(operationName);
                return op.getResponseProcessor().stream()
                        .filter(rp -> name.trim().equalsIgnoreCase(rp.getName()))
                        .findFirst()
                        .map(BatchProperties.ResponseProcessorEntryProperties::getResponseProcessor)
                        .orElse(null);
            } catch (Exception ignored) {}
        }
        return null;
    }

    /**
     * Looks up the named processor chain and applies each step sequentially to the full response.
     * Returns whatever the final step produces — may be a Map, List, String, etc.
     * Called by the controller/WebSocket handler after the complete response object is assembled.
     */
    public Object applyResponseProcessor(Object response, String name, String operationName) throws Exception {
        if (name == null || name.isBlank()) return response;
        List<BatchProperties.ResponseProcessorProperties> procs = resolveResponseProcessor(name, operationName);
        if (procs == null) {
            List<String> available = new ArrayList<>(responseProcessorRegistry.keySet());
            throw new IllegalArgumentException("Response processor not found: '" + name.trim()
                    + "'. Available: " + available);
        }
        if (procs.isEmpty()) return response;
        Object current = response;
        for (BatchProperties.ResponseProcessorProperties proc : procs) {
            current = applySingleResponseProcessor(current, proc);
        }
        return current;
    }

    public Object applyResponseProcessor(Object response, String name) throws Exception {
        return applyResponseProcessor(response, name, null);
    }

    @SuppressWarnings("unchecked")
    private Object applySingleResponseProcessor(
            Object response,
            BatchProperties.ResponseProcessorProperties proc) throws Exception {

        if (proc == null) return response;
        ProcessorType rpType = proc.getType();
        try {
            return runProcessorStep(response, rpType, proc);
        } catch (Exception e) {
            String onError = proc.getOnError();
            if (onError == null) throw e;
            if ("SKIP".equalsIgnoreCase(onError.trim())) return response;
            if ("THROW".equalsIgnoreCase(onError.trim()))
                throw new IllegalArgumentException(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), e);
            // $.key — write error message into response map at that key
            String key = onError.trim().startsWith("$.") ? onError.trim().substring(2) : onError.trim();
            if (response instanceof Map<?, ?> m) {
                Map<String, Object> out = new LinkedHashMap<>((Map<String, Object>) m);
                out.put(key, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                return out;
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private Object runProcessorStep(Object response, ProcessorType rpType,
                                    BatchProperties.ResponseProcessorProperties proc) throws Exception {

        if (rpType == ProcessorType.XML2JSON) {
            if (!(response instanceof Map<?, ?> responseMap)) return response;
            Object dataObj = ((Map<String, Object>) responseMap).get("data");
            if (dataObj instanceof List<?> dataList) {
                List<Map<String, Object>> converted = new ArrayList<>(dataList.size());
                for (Object item : dataList) {
                    converted.add(item instanceof Map ? convertXmlToJson((Map<String, Object>) item) : (Map<String, Object>) item);
                }
                Map<String, Object> out = new LinkedHashMap<>((Map<String, Object>) responseMap);
                out.put("data", converted);
                return out;
            }
            return response;
        }

        if (rpType == ProcessorType.JSONATA) {
            String transform = proc.getJsonataTransform();
            if (transform == null || transform.isBlank()) return response;
            if (transform.startsWith("classpath:")) {
                String res = transform.substring("classpath:".length());
                try (InputStream is = getClass().getClassLoader().getResourceAsStream(res)) {
                    if (is == null) throw new java.io.FileNotFoundException("JSONata transform not found: " + res);
                    transform = new String(is.readAllBytes());
                }
            }
            String json = objectMapper.writeValueAsString(response);
            Object result = Jsonata.jsonata(transform).evaluate(objectMapper.readValue(json, Object.class));
            if (result == null) return response;
            // If the result is a String, try to parse it as JSON so it isn't double-encoded
            // (e.g. when the transform extracts a field that holds a JSON string value)
            if (result instanceof String s) {
                String trimmed = s.trim();
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    try { return objectMapper.readValue(trimmed, Object.class); } catch (Exception ignored) {}
                }
                return s;
            }
            return objectMapper.readValue(objectMapper.writeValueAsString(result), Object.class);
        }

        return response;
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
        java.time.LocalDate today = java.time.LocalDate.now();
        result.put("DATESTAMP",  today.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
        result.put("DATETIME",   java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
        java.time.LocalDate prevBizDate = today.minusDays(1);
        while (prevBizDate.getDayOfWeek() == java.time.DayOfWeek.SATURDAY
                || prevBizDate.getDayOfWeek() == java.time.DayOfWeek.SUNDAY) {
            prevBizDate = prevBizDate.minusDays(1);
        }
        result.put("PREVDATESTAMP", prevBizDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
        java.time.LocalDate nextBizDate = today.plusDays(1);
        while (nextBizDate.getDayOfWeek() == java.time.DayOfWeek.SATURDAY
                || nextBizDate.getDayOfWeek() == java.time.DayOfWeek.SUNDAY) {
            nextBizDate = nextBizDate.plusDays(1);
        }
        result.put("NEXTDATESTAMP", nextBizDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
        result.put("PROCESSID",  String.valueOf(pid));
        result.put("JVMID",      hostname + "." + pid);

        // 1. Server properties (base)
        result.putAll(serverPropertiesLoader.getProperties());

        // 2. Operation-level properties
        BatchProperties.OperationPropertiesConfig cfg = op.getProperties();
        if (cfg != null) {
            if (cfg.getAttributes() != null) result.putAll(cfg.getAttributes());

            List<BatchProperties.FilePropertiesSource> fileSrcs = cfg.getFile();
            if (fileSrcs != null) {
                for (BatchProperties.FilePropertiesSource fileSrc : fileSrcs) {
                    if (fileSrc != null && !fileSrc.getPath().isBlank()) {
                        java.util.Properties props = new java.util.Properties();
                        try (FileInputStream fis = new FileInputStream(fileSrc.getPath())) {
                            props.load(fis);
                        }
                        props.forEach((k, v) -> result.put(k.toString(), v.toString()));
                    }
                }
            }

            List<BatchProperties.HttpPropertiesSource> httpSrcs = cfg.getHttp();
            if (httpSrcs != null && !httpSrcs.isEmpty()) {
                HttpClient client = HttpClient.newBuilder().build();
                for (BatchProperties.HttpPropertiesSource httpSrc : httpSrcs) {
                    if (httpSrc != null && !httpSrc.getUrl().isBlank()) {
                        int timeout = httpSrc.getTimeoutMs() > 0 ? httpSrc.getTimeoutMs() : 5000;
                        HttpRequest.Builder rb = HttpRequest.newBuilder()
                                .uri(URI.create(httpSrc.getUrl()))
                                .timeout(Duration.ofMillis(timeout));
                        if (httpSrc.getMethod() == HttpMethod.POST) {
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
                                           BatchProperties.OperationProperties op,
                                           Map<String, String> opProperties) {
        List<BatchProperties.MandatoryPropertyDef> defs = op.getMandatoryProperties();
        if (defs.isEmpty()) return;
        Map<String, Object> reqMap = objectMapper.convertValue(request, new TypeReference<>() {});
        List<String> missing = new java.util.ArrayList<>();
        for (BatchProperties.MandatoryPropertyDef def : defs) {
            String prop = def.getProperty();
            if (prop == null || prop.isBlank()) continue;
            if (!def.isRequired()) continue;
            Object val = reqMap.get(prop);
            if (val != null && !val.toString().isBlank()) continue;
            String opVal = opProperties.get(prop);
            if (opVal != null && !opVal.isBlank()) continue;
            // Property absent — apply defaultValue if present (empty string is valid), otherwise it is missing
            if (def.getDefaultValue() != null) {
                opProperties.put(prop, resolveDefaultValue(def.getDefaultValue()));
            } else {
                missing.add(prop);
            }
        }
        if (!missing.isEmpty())
            throw new IllegalArgumentException(
                    "Request is missing mandatory field(s): " + missing);
    }

    private void checkAliasMandatoryProperties(RunRequest request, String aliasName,
                                                BatchProperties.OperationProperties op,
                                                Map<String, String> opProperties) {
        BatchProperties.AliasProperties alias = op.getAlias().stream()
                .filter(a -> a.getName().equalsIgnoreCase(aliasName.trim()))
                .findFirst().orElse(null);
        if (alias == null) return;
        List<BatchProperties.MandatoryPropertyDef> defs = alias.getMandatoryProperties();
        if (defs.isEmpty()) return;
        Map<String, Object> reqMap = objectMapper.convertValue(request, new TypeReference<>() {});
        List<String> missing = new java.util.ArrayList<>();
        for (BatchProperties.MandatoryPropertyDef def : defs) {
            String prop = def.getProperty();
            if (prop == null || prop.isBlank()) continue;
            if (!def.isRequired()) continue;
            Object val = reqMap.get(prop);
            if (val != null && !val.toString().isBlank()) continue;
            String opVal = opProperties.get(prop);
            if (opVal != null && !opVal.isBlank()) continue;
            if (def.getDefaultValue() != null) {
                opProperties.put(prop, resolveDefaultValue(def.getDefaultValue()));
            } else {
                missing.add(prop);
            }
        }
        if (!missing.isEmpty())
            throw new IllegalArgumentException(
                    "Alias '" + aliasName + "' requires mandatory field(s): " + missing);
    }

    /** Resolves a defaultValue — if it starts with {@code classpath:}, the resource is read from the classpath. */
    String resolveDefaultValue(String value) {
        if (value == null || !value.startsWith("classpath:")) return value;
        String resourcePath = value.substring("classpath:".length()).trim();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Failed to read classpath resource '" + resourcePath + "': " + e.getMessage());
        }
    }

    /** Injects defaultValues from mandatory properties into opProperties without throwing for missing fields. */
    private void injectMandatoryDefaults(RunRequest request, String aliasName,
                                         BatchProperties.OperationProperties op,
                                         Map<String, String> opProperties) {
        List<BatchProperties.MandatoryPropertyDef> defs;
        if (aliasName != null && !aliasName.isBlank()) {
            defs = op.getAlias().stream()
                    .filter(a -> a.getName().equalsIgnoreCase(aliasName.trim()))
                    .findFirst()
                    .map(BatchProperties.AliasProperties::getMandatoryProperties)
                    .orElse(op.getMandatoryProperties());
        } else {
            defs = op.getMandatoryProperties();
        }
        if (defs.isEmpty()) return;
        Map<String, Object> reqMap = objectMapper.convertValue(request, new TypeReference<>() {});
        boolean anyDefaultsInjected = false;
        for (BatchProperties.MandatoryPropertyDef def : defs) {
            String prop = def.getProperty();
            if (prop == null || prop.isBlank()) continue;
            Object val = reqMap.get(prop);
            if (val != null && !val.toString().isBlank()) continue;
            if (opProperties.containsKey(prop) && !opProperties.get(prop).isBlank()) continue;
            if (def.getDefaultValue() != null) {
                opProperties.put(prop, resolveDefaultValue(def.getDefaultValue()));
                anyDefaultsInjected = true;
            }
        }
        // Resolve ${VAR} placeholders in any newly injected default values (e.g. ${DATADIR}/file.txt)
        if (anyDefaultsInjected) resolvePropertyVariables(opProperties);
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
    private static List<DataRow> applySearchKeyword(List<DataRow> rows, SearchKeyword keyword) {
        if (keyword == null || keyword.getValue() == null || keyword.getValue().isBlank()) return rows;
        String val  = keyword.getValue();
        String type = keyword.getType() != null ? keyword.getType().trim().toLowerCase() : "contains";
        return rows.stream().filter(row ->
            row.getData().values().stream().anyMatch(v -> {
                if (v == null) return false;
                String s = v.toString();
                return switch (type) {
                    case "startswith" -> s.toLowerCase().startsWith(val.toLowerCase());
                    case "endswith"   -> s.toLowerCase().endsWith(val.toLowerCase());
                    case "regex"      -> s.matches(val);
                    case "eq"         -> s.equalsIgnoreCase(val);
                    default           -> s.toLowerCase().contains(val.toLowerCase());
                };
            })
        ).collect(Collectors.toList());
    }

    private void saveSingleRowToCache(Map<String, Object> row, CacheOutput cacheOut) {
        try {
            String keyCol = cacheOut.getKey();
            Object keyVal = row.get(keyCol);
            if (keyVal == null) {
                for (Map.Entry<String, Object> e : row.entrySet()) {
                    if (e.getKey().equalsIgnoreCase(keyCol)) { keyVal = e.getValue(); break; }
                }
            }
            if (keyVal != null) {
                cacheFactory.save(cacheOut.getName(), keyVal.toString(),
                        objectMapper.writeValueAsString(row), keyCol + "=" + keyVal);
            }
        } catch (Exception ignored) {}
    }

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
        Matcher m = Pattern.compile("\\$?\\{(\\w+)\\}").matcher(template);
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

    /** Resolves {@code ${KEY}} / {@code {KEY}} placeholders in a path string using operation properties. */
    public String resolvePath(String path, Map<String, String> opProperties) {
        return resolveTemplate(path, Map.of(), opProperties);
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
