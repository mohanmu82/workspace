package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.ActivityType;
import com.mycompany.batch.model.AuthMethod;
import com.mycompany.batch.model.DataExtractionType;
import com.mycompany.batch.model.ExecutionMode;
import com.mycompany.batch.model.InputSourceType;
import com.mycompany.batch.model.OutputDataType;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/batch")
public class BatchController {

    private final BatchService batchService;
    private final BatchProperties batchProperties;
    private final CacheFactory cacheFactory;
    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;

    public BatchController(BatchService batchService, BatchProperties batchProperties,
                           CacheFactory cacheFactory, ObjectMapper objectMapper,
                           ServerPropertiesLoader serverPropertiesLoader) {
        this.batchService = batchService;
        this.batchProperties = batchProperties;
        this.cacheFactory = cacheFactory;
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    // -------------------------------------------------------------------------
    // GET /batch/operations  — flat key-value listing of all configured operations
    // -------------------------------------------------------------------------

    @GetMapping("/operations")
    public ResponseEntity<Map<String, Object>> listOperations() {
        List<Map<String, Object>> data = new ArrayList<>();

        batchProperties.getOperations().forEach((name, op) -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", name);

            // Activities (if defined)
            if (!op.getActivity().isEmpty()) {
                entry.put("activity_count", op.getActivity().size());
                for (int i = 0; i < op.getActivity().size(); i++) {
                    var act = op.getActivity().get(i);
                    String pfx = "activity_" + i + "_";
                    entry.put(pfx + "name", act.getName());
                    entry.put(pfx + "type", act.getType());
                    if (act.getType() == ActivityType.HTTP) {
                        entry.put(pfx + "http_url",         act.getHttp().getUrl());
                        entry.put(pfx + "http_method",      act.getHttp().getMethod());
                        entry.put(pfx + "http_threadCount", act.getHttp().getThreadCount());
                        entry.put(pfx + "http_timeoutMs",   act.getHttp().getTimeoutMs());
                        act.getHttp().getHeader().forEach((k, v) -> entry.put(pfx + "http_header_" + k, v));
                    } else if (act.getType() == ActivityType.DATAEXTRACTION) {
                        entry.put(pfx + "dataExtraction_type",  act.getDataExtraction().getType());
                        entry.put(pfx + "dataExtraction_config",act.getDataExtraction().getConfig());
                        entry.put(pfx + "dataExtraction_threadCount", act.getDataExtraction().getThreadCount());
                    }
                }
            }

            // Effective HTTP properties (from activity if present, otherwise legacy flat)
            BatchProperties.HttpProperties http = op.getEffectiveHttp();
            entry.put("http_url",          http.getUrl());
            entry.put("http_method",        http.getMethod());
            entry.put("http_contentType",   http.getContentType());
            entry.put("http_bodyTemplate",  http.getBodyTemplate());
            entry.put("http_threadCount",   http.getThreadCount());
            entry.put("http_timeoutMs",     http.getTimeoutMs());

            // Custom HTTP headers (flat)
            http.getHeader().forEach((k, v) -> entry.put("http_header_" + k, v));

            // XPath properties (flat) — from legacy config
            entry.put("xpath_config",       op.getXpath().getConfig());
            entry.put("xpath_threadCount",  op.getXpath().getThreadCount());

            // Auth properties (flat)
            AuthMethod authMethod = op.getAuth().getMethod();
            entry.put("auth_method", authMethod);
            switch (authMethod) {
                case BASIC -> {
                    entry.put("auth_basic_username", op.getAuth().getBasic().getUsername());
                    entry.put("auth_basic_password", "***");
                }
                case JWT -> {
                    entry.put("auth_jwt_url",             op.getAuth().getJwt().getUrl());
                    entry.put("auth_jwt_applicationName", op.getAuth().getJwt().getApplicationName());
                    entry.put("auth_jwt_username",        op.getAuth().getJwt().getUsername());
                    entry.put("auth_jwt_password",        "***");
                }
                case KERBEROS -> {
                    entry.put("auth_kerberos_username",         op.getAuth().getKerberos().getUsername());
                    entry.put("auth_kerberos_keytab",           op.getAuth().getKerberos().getKeytab());
                    entry.put("auth_kerberos_servicePrincipal", op.getAuth().getKerberos().getServicePrincipal());
                }
                default -> {}
            }

            // Input source properties (flat)
            InputSourceType inputType = op.getInputSource().getType();
            entry.put("inputSource_type", inputType);
            if (inputType == InputSourceType.HTTPCONFIG) {
                entry.put("inputSource_httpConfig_url",             op.getInputSource().getHttpConfig().getUrl());
                entry.put("inputSource_httpConfig_method",          op.getInputSource().getHttpConfig().getMethod());
                entry.put("inputSource_httpConfig_jsonataTransform",op.getInputSource().getHttpConfig().getJsonataTransform());
            }

            // Output data properties (flat)
            entry.put("outputData_type", op.getOutputData().getType());

            // Data extraction properties (flat)
            DataExtractionType extractType = op.getDataExtraction().getType();
            entry.put("dataExtraction_type", extractType);
            if (extractType == DataExtractionType.JSON || extractType == DataExtractionType.JSONATA) {
                entry.put("dataExtraction_jsonataTransform", op.getDataExtraction().getJsonataTransform());
            }

            // Mandatory properties (full definitions for dynamic form rendering)
            if (!op.getMandatoryProperties().isEmpty()) {
                entry.put("mandatoryProperties", op.getMandatoryProperties());
            }

            // Alias names (kept as string list for backward compat)
            List<String> aliasNames = op.getAlias().stream()
                    .map(BatchProperties.AliasProperties::getName)
                    .filter(n -> n != null && !n.isBlank())
                    .collect(Collectors.toList());
            if (!aliasNames.isEmpty()) {
                entry.put("aliases", aliasNames);
                // Full alias detail — name + optional mandatoryProperties for dynamic forms
                List<Map<String, Object>> aliasDetails = op.getAlias().stream()
                        .filter(a -> a.getName() != null && !a.getName().isBlank())
                        .map(a -> {
                            Map<String, Object> ad = new LinkedHashMap<>();
                            ad.put("name", a.getName());
                            if (!a.getMandatoryProperties().isEmpty()) {
                                ad.put("mandatoryProperties", a.getMandatoryProperties());
                            }
                            return ad;
                        })
                        .collect(Collectors.toList());
                entry.put("aliasDetails", aliasDetails);
            }

            data.add(entry);
        });

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", data);
        List<String> rpNames = batchService.getResponseProcessorNames();
        if (!rpNames.isEmpty()) {
            response.put("responseProcessors", rpNames);
        }
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // GET  /batch/cache          — list all cache names + entry counts
    // GET  /batch/cache/{name}   — list all entries in one cache
    // DELETE /batch/cache/{name} — clear one cache
    // -------------------------------------------------------------------------

    @GetMapping("/cache")
    public ResponseEntity<Map<String, Object>> listCaches() {
        List<Map<String, Object>> data = new ArrayList<>();
        cacheFactory.getAll().forEach((name, entries) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("cacheName",  name);
            row.put("cacheCount", entries.size());
            long totalBytes = entries.values().stream()
                    .mapToLong(e -> e.value() != null ? e.value().length() : 0L)
                    .sum();
            row.put("cacheSizeMb", Math.round(totalBytes / 1024.0 / 1024.0 * 100.0) / 100.0);
            data.add(row);
        });
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", data);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/cache/{name}")
    public ResponseEntity<Map<String, Object>> listCacheEntries(@PathVariable String name) {
        List<Map<String, Object>> data = new ArrayList<>();
        cacheFactory.getEntries(name).forEach((key, entry) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key",   key);
            row.put("value", entry.value());
            if ("runtime".equals(name) && entry.url() != null) {
                String[] parts = entry.url().split(" \\| ", 2);
                row.put("operation", parts[0]);
                row.put("timestamp", parts.length > 1 ? parts[1] : "");
            } else {
                row.put("url", entry.url());
            }
            data.add(row);
        });
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", data);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/cache/{name}")
    public ResponseEntity<Map<String, Object>> clearCache(@PathVariable String name) {
        cacheFactory.clear(name);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("status",  "CLEARED");
        entry.put("message", "Cache '" + name + "' has been cleared");
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", List.of(entry));
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // GET /batch/run  — run via query parameters
    //
    // Supported inputSource values:
    //   FILE (default)  — ?operationType=X&inputFilePath=/path/to/file.csv
    //   HTTPGET         — ?operationType=X&inputSource=HTTPGET&ids=123,456,789
    //   HTTPCONFIG      — ?operationType=X&inputSource=HTTPCONFIG
    //
    // Optional:
    //   inputCount=N    — limit to first N identifiers
    //   outputData=FILE — write PSV; requires outputFilePath=...
    // -------------------------------------------------------------------------

    @GetMapping("/run")
    public ResponseEntity<?> runGet(
            @RequestParam Map<String, String> params) throws Exception {
        return executeRun(buildRunRequestFromParams(params));
    }

    // -------------------------------------------------------------------------
    // POST /batch/run (form / query params) — mirrors the GET endpoint
    // -------------------------------------------------------------------------

    @PostMapping(value = "/run",
            consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE,
                        MediaType.MULTIPART_FORM_DATA_VALUE})
    public ResponseEntity<?> runPostForm(
            @RequestParam Map<String, String> params) throws Exception {
        return executeRun(buildRunRequestFromParams(params));
    }

    // -------------------------------------------------------------------------
    // POST /batch/run (JSON body) — supports all inputSource types including HTTPPOST
    //
    // Example bodies:
    //
    //   FILE:       { "operationType":"pubmed", "inputFilePath":"/data/ids.csv" }
    //   HTTPGET:    { "operationType":"pubmed", "inputSource":"HTTPGET", "ids":["123","456"] }
    //   HTTPPOST:   { "operationType":"pubmed", "inputSource":"HTTPPOST","ids":["123","456"] }
    //   HTTPCONFIG: { "operationType":"pubmed", "inputSource":"HTTPCONFIG" }
    //   FILE→FILE:  { "operationType":"pubmed", "inputFilePath":"/in.csv",
    //                 "outputData":"FILE", "outputFilePath":"/out.psv" }
    // -------------------------------------------------------------------------

    @PostMapping(value = "/run", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> runPostJson(
            @RequestBody Map<String, Object> body) throws Exception {
        return executeRun(deserializeRunRequest(body));
    }

    // -------------------------------------------------------------------------
    // Legacy PSV endpoints (kept for backward compatibility)
    // -------------------------------------------------------------------------

    @GetMapping("/run/psv")
    public ResponseEntity<?> runPsvGet(
            @RequestParam Map<String, String> params) throws Exception {
        return runPsv(params);
    }

    @PostMapping("/run/psv")
    public ResponseEntity<?> runPsvPost(
            @RequestParam Map<String, String> params) throws Exception {
        return runPsv(params);
    }

    // -------------------------------------------------------------------------
    // Shared execution logic
    // -------------------------------------------------------------------------

    /** Shared entry point for all run endpoints. */
    ResponseEntity<?> executeRun(RunRequest request) throws Exception {
        if (request.operation() == null || request.operation().isBlank()) {
            return badRequest("operation is required");
        }

        BatchService.BatchResult result;
        try {
            request = batchService.resolveAlias(request);
            result = batchService.run(request);
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return errorsResponse("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }

        OutputDataType outputData = resolveOutputData(request);

        if (outputData == OutputDataType.FILE) {
            String outputFilePath = request.outputFilePath();
            if (outputFilePath == null || outputFilePath.isBlank()) {
                try {
                    outputFilePath = batchProperties.getOperation(request.operation())
                            .getOutputData().getOutputFilePath();
                } catch (Exception ignored) {}
            }
            if (outputFilePath == null || outputFilePath.isBlank()) {
                return badRequest("outputFilePath is required when outputData=FILE");
            }
            outputFilePath = batchService.resolvePath(outputFilePath, result.operationProperties());
            try {
                batchService.writeToPsv(result, outputFilePath,
                        Boolean.TRUE.equals(request.appendOutput()));
            } catch (Exception e) {
                return badRequest("Failed to write output file: " + e.getMessage());
            }
            saveToRuntimeCache(request, result.batchUuid(), result.timestamp());
            Object fileResp = buildFileResponse(request.operation(), result,
                    Path.of(outputFilePath).toAbsolutePath().toString());
            if (request.jsonataTransform() != null) {
                fileResp = batchService.applyJsonataTransform(fileResp, request.jsonataTransform());
            }
            return ResponseEntity.ok(fileResp);
        }

        saveToRuntimeCache(request, result.batchUuid(), result.timestamp());
        Object httpResponse = buildHttpResponse(request.operation(), result, request.httpThreadCount());
        if (request.responseProcessor() != null && !request.responseProcessor().isBlank()) {
            httpResponse = batchService.applyResponseProcessor(httpResponse, request.responseProcessor());
        }
        if (request.jsonataTransform() != null) {
            httpResponse = batchService.applyJsonataTransform(httpResponse, request.jsonataTransform());
        }
        return ResponseEntity.ok(httpResponse);
    }

    private OutputDataType resolveOutputData(RunRequest request) {
        if (request.outputData() != null) return request.outputData();
        try {
            OutputDataType t = batchProperties.getOperation(request.operation()).getOutputData().getType();
            return t != null ? t : OutputDataType.HTTP;
        } catch (Exception e) {
            return OutputDataType.HTTP;
        }
    }

    /** Builds the full JSON response returned to HTTP clients and the WebSocket. */
    Map<String, Object> buildHttpResponse(String operationType, BatchService.BatchResult result,
                                          Integer threadCountOverride) {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);
        BatchProperties.HttpProperties http = op.getEffectiveHttp();
        Map<String, Object> httpStats = new LinkedHashMap<>();
        httpStats.put("method",      http.getMethod());
        httpStats.put("threadCount", threadCountOverride != null ? threadCountOverride : http.getThreadCount());
        httpStats.put("minMs",       result.httpStats().minMs());
        httpStats.put("maxMs",       result.httpStats().maxMs());
        httpStats.put("avgMs",       result.httpStats().avgMs());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("processed",      result.processed());
        summary.put("succeeded",      result.succeeded());
        summary.put("failed",         result.failed());
        summary.put("timeTakenMs",    result.timeTakenMs());
        summary.put("responseSizeKb", Math.round(result.responseSizeKb() * 100.0) / 100.0);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("batchUuid",    result.batchUuid());
        metadata.put("timestamp",    result.timestamp());
        metadata.put("operation", operationType);
        metadata.put("httpStats",    httpStats);
        metadata.put("summary",      summary);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata", metadata);
        if (result.operationProperties() != null) {
            response.put("properties", result.operationProperties());
        }
        response.put("columns",  result.columns());
        response.put("data",     result.results());
        return response;
    }

    /** Builds the summary response used when results were written to a file. */
    Map<String, Object> buildFileResponse(String operationType, BatchService.BatchResult result,
                                                  String outputFilePath) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("processed",      result.processed());
        summary.put("succeeded",      result.succeeded());
        summary.put("failed",         result.failed());
        summary.put("timeTakenMs",    result.timeTakenMs());
        summary.put("responseSizeKb", Math.round(result.responseSizeKb() * 100.0) / 100.0);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("batchUuid",    result.batchUuid());
        metadata.put("timestamp",    result.timestamp());
        metadata.put("operation", operationType);
        metadata.put("summary",      summary);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata",   metadata);
        response.put("outputFile", outputFilePath);
        return response;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Converts a flat {@code Map<String,String>} of request params into a {@link RunRequest}. */
    private RunRequest buildRunRequestFromParams(Map<String, String> params) {
        String idsParam = params.get("ids");
        List<String> ids = null;
        if (idsParam != null && !idsParam.isBlank()) {
            ids = Arrays.stream(idsParam.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .toList();
        }
        return new RunRequest(
                params.get("operation"),
                InputSourceType.from(params.get("inputSource")),
                params.get("inputFilePath"),
                params.get("inputHttpUrl"),  // mandatory when inputSource=HTTPCONFIG
                null, // inputHttpHeader — not supported via query params
                null, // inputHttpBody   — not supported via query params
                ids,
                null, // raw — not supported via query params
                parseInputCount(params.get("inputCount")),
                OutputDataType.from(params.get("outputData")),
                params.get("outputFilePath"),
                parseInputCount(params.get("debugMode")),
                parseInputCount(params.get("httpThreadCount")),
                parseInputCount(params.get("httpTimeoutMs")),
                null, // filterInput — not supported via query params
                null, // filterOutput — not supported via query params
                null, // searchKeyword
                null, // cache
                ExecutionMode.from(params.get("executionMode")),
                params.get("alias"),
                params.get("responseProcessor"),
                "true".equalsIgnoreCase(params.get("appendOutput")) ? Boolean.TRUE : null,
                params.get("inputJsonPath"),
                params.get("cacheName"),
                null,  // properties — not supported via query params
                null,  // jsonataTransform
                null   // templateName
        );
    }

    void saveToRuntimeCache(RunRequest request, String batchUuid, String timestamp) {
        try {
            Map<String, Object> reqMap = objectMapper.convertValue(request,
                    new com.fasterxml.jackson.core.type.TypeReference<>() {});
            reqMap.values().removeIf(java.util.Objects::isNull);
            cacheFactory.save("runtime", batchUuid, objectMapper.writeValueAsString(reqMap),
                    request.operation() + " | " + timestamp);
        } catch (Exception ignored) {}
    }

    private ResponseEntity<Map<String, Object>> runPsv(Map<String, String> params) throws Exception {
        String operationType = params.get("operationType");
        if (operationType == null || operationType.isBlank()) {
            return badRequest("operationType is required");
        }

        String inputFilePath = params.get("inputFilePath");
        if (inputFilePath == null || inputFilePath.isBlank()) {
            return badRequest("inputFilePath is required");
        }
        if (!Files.exists(Path.of(inputFilePath))) {
            return badRequest("inputFilePath does not exist: " + inputFilePath);
        }

        String outputFilePath = params.get("outputFilePath");
        if (outputFilePath == null || outputFilePath.isBlank()) {
            return badRequest("outputFilePath is required");
        }

        Integer inputCount = parseInputCount(params.get("inputCount"));
        if (inputCount instanceof Integer ic && ic <= 0) {
            return badRequest("inputCount must be a positive integer");
        }

        BatchService.PsvResult result;
        try {
            result = batchService.runToPsv(inputFilePath, outputFilePath, inputCount, operationType);
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("processed",  result.processed());
        summary.put("succeeded",  result.succeeded());
        summary.put("failed",     result.failed());

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("batchUuid",     result.batchUuid());
        metadata.put("timestamp",     result.timestamp());
        metadata.put("operation", operationType);
        metadata.put("summary",       summary);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata",   metadata);
        response.put("outputFile", result.outputFile());
        return ResponseEntity.ok(response);
    }

    private static final java.util.Set<String> KNOWN_RUN_REQUEST_FIELDS = java.util.Set.of(
            "operation", "inputSource", "inputFilePath", "inputHttpUrl", "inputHttpHeader",
            "inputHttpBody", "ids", "raw", "inputCount", "outputData", "outputFilePath",
            "debugMode", "httpThreadCount", "httpTimeoutMs", "filterInput", "filterOutput",
            "searchKeyword", "cache", "executionMode", "alias", "responseProcessor",
            "appendOutput", "inputJsonPath", "cacheName", "properties",
            "jsonataTransform", "templateName");

    /**
     * Converts a raw JSON body map to a {@link RunRequest}, capturing any unrecognised
     * top-level keys as entries in {@code properties} so callers can pass custom key-value
     * pairs (e.g. {@code {"operation":"jwt","username":"x","password":"y"}}) without
     * having to nest them inside a {@code "properties"} object.
     * Explicitly set {@code properties} values take precedence over top-level unknowns.
     */
    RunRequest deserializeRunRequest(Map<String, Object> body) {
        Map<String, Object> extra = new LinkedHashMap<>();
        body.forEach((k, v) -> { if (!KNOWN_RUN_REQUEST_FIELDS.contains(k)) extra.put(k, v); });
        if (extra.isEmpty()) return objectMapper.convertValue(body, RunRequest.class);

        Map<String, Object> merged = new LinkedHashMap<>(body);
        Map<String, String> props = new LinkedHashMap<>();
        // Unknown top-level keys go in first (lowest priority)
        extra.forEach((k, v) -> props.put(k, v != null ? v.toString() : ""));
        // Explicitly provided properties override them
        if (body.get("properties") instanceof Map<?, ?> existing) {
            existing.forEach((k, v) -> props.put(k.toString(), v != null ? v.toString() : ""));
        }
        merged.put("properties", props);
        return objectMapper.convertValue(merged, RunRequest.class);
    }

    /**
     * Returns null if the string is null, -1 if unparseable, or the parsed value.
     * Callers check for <= 0 to catch both the -1 sentinel and genuine non-positive values.
     */
    private Integer parseInputCount(String value) {
        if (value == null) return null;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // -------------------------------------------------------------------------
    // GET /batch/resource?classpath=path/to/resource  — read a classpath resource as text
    // -------------------------------------------------------------------------

    @GetMapping("/resource")
    public ResponseEntity<?> getClasspathResource(@RequestParam String classpath) throws Exception {
        // Strip leading slashes and block path traversal
        String path = classpath.replace('\\', '/').replaceAll("(^/+|\\.\\./)", "").trim();
        if (path.isBlank()) return badRequest("classpath parameter is required");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) return badRequest("Resource not found: " + path);
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("content", content);
            return ResponseEntity.ok(response);
        }
    }

    // -------------------------------------------------------------------------
    // Template endpoints
    //
    // GET  /batch/template                  — list saved templates
    // GET  /batch/template?name=X&k=v       — run template X with extra props {k:v}
    // GET  /batch/template/{name}           — fetch template content as JSON
    // POST /batch/template  {"name","content"} — save template
    // POST /batch/template  {"name","k":"v"} — run template with extra props
    // -------------------------------------------------------------------------

    @GetMapping("/template")
    public ResponseEntity<?> getTemplates(@RequestParam Map<String, String> params) throws Exception {
        String name = params.get("name");
        if (name != null && !name.isBlank()) {
            Map<String, String> extraProps = new LinkedHashMap<>(params);
            extraProps.remove("name");
            return executeTemplateRun(name.trim(), extraProps);
        }
        // List mode
        Path dir = templateDir();
        List<String> names = new ArrayList<>();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                stream.filter(p -> p.toString().endsWith(".json"))
                      .map(p -> p.getFileName().toString().replaceAll("\\.json$", ""))
                      .sorted()
                      .forEach(names::add);
            } catch (Exception ignored) {}
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", names);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/template/{name}")
    public ResponseEntity<?> getTemplateContent(@PathVariable String name) throws Exception {
        Path file = templateDir().resolve(name + ".json");
        if (!Files.exists(file)) return badRequest("template not found: " + name);
        Object content = objectMapper.readValue(file.toFile(), Object.class);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name",    name);
        response.put("content", content);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/template")
    public ResponseEntity<?> postTemplate(
            @RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");

        if (body.containsKey("content")) {
            // Save mode
            if (!name.matches("[\\w\\-. ]+")) return badRequest("name contains invalid characters");
            Object content = body.get("content");
            Path dir = templateDir();
            Files.createDirectories(dir);
            Path file = dir.resolve(name + ".json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "saved");
            response.put("path",   file.toAbsolutePath().toString());
            return ResponseEntity.ok(response);
        }

        // Run mode — all keys except "name" become extra properties
        Map<String, String> extraProps = new LinkedHashMap<>();
        body.forEach((k, v) -> {
            if (!"name".equals(k) && v != null) extraProps.put(k, v.toString());
        });
        return executeTemplateRun(name, extraProps);
    }

    private ResponseEntity<?> executeTemplateRun(String name, Map<String, String> extraProps) throws Exception {
        Path file = templateDir().resolve(name + ".json");
        if (!Files.exists(file)) return badRequest("template not found: " + name);
        RunRequest template;
        try {
            template = objectMapper.readValue(file.toFile(), RunRequest.class);
        } catch (Exception e) {
            return badRequest("failed to parse template '" + name + "': " + e.getMessage());
        }
        RunRequest request = template;
        if (!extraProps.isEmpty()) {
            Map<String, String> mergedProps = new LinkedHashMap<>();
            if (template.properties() != null) mergedProps.putAll(template.properties());
            mergedProps.putAll(extraProps);
            request = new RunRequest(
                    template.operation(), template.inputSource(), template.inputFilePath(),
                    template.inputHttpUrl(), template.inputHttpHeader(), template.inputHttpBody(),
                    template.ids(), template.raw(), template.inputCount(), template.outputData(),
                    template.outputFilePath(), template.debugMode(), template.httpThreadCount(),
                    template.httpTimeoutMs(), template.filterInput(), template.filterOutput(),
                    template.searchKeyword(), template.cache(), template.executionMode(),
                    template.alias(), template.responseProcessor(), template.appendOutput(),
                    template.inputJsonPath(), template.cacheName(), mergedProps,
                    template.jsonataTransform(), template.templateName());
        }
        return executeRun(request);
    }

    private Path templateDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("operationTemplate");
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return errorsResponse("validation", message);
    }

    ResponseEntity<Map<String, Object>> errorsResponse(String activity, String message) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("activity", activity);
        err.put("message",  message != null ? message : "Unknown error");
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("Errors", List.of(err));
        return ResponseEntity.badRequest().body(body);
    }
}
