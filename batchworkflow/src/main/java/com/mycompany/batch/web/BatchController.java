package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.ActivityType;
import com.mycompany.batch.model.AuthMethod;
import com.mycompany.batch.model.DataExtractionType;
import com.mycompany.batch.model.ExecutionMode;
import com.mycompany.batch.model.DataRow;
import com.mycompany.batch.model.InputSourceType;
import com.mycompany.batch.model.OutputDataType;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
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

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

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
@RequestMapping(value = "/batch", produces = MediaType.APPLICATION_JSON_VALUE)
public class BatchController {

    private final BatchService batchService;
    private final BatchProperties batchProperties;
    private final CacheFactory cacheFactory;
    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final BatchWebSocketHandler batchWebSocketHandler;

    @org.springframework.beans.factory.annotation.Value("${server.port:8080}")
    private int serverPort;

    @org.springframework.beans.factory.annotation.Value("${app.version:unknown}")
    private String appVersion;

    @Autowired
    public BatchController(BatchService batchService, BatchProperties batchProperties,
                           CacheFactory cacheFactory, ObjectMapper objectMapper,
                           ServerPropertiesLoader serverPropertiesLoader,
                           @Lazy BatchWebSocketHandler batchWebSocketHandler) {
        this.batchService = batchService;
        this.batchProperties = batchProperties;
        this.cacheFactory = cacheFactory;
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
        this.batchWebSocketHandler = batchWebSocketHandler;
    }

    // -------------------------------------------------------------------------
    // POST /batch/operation  — save an operation JSON to DATADIR/operations/ and register it live
    // -------------------------------------------------------------------------

    @PostMapping("/operation")
    public ResponseEntity<?> saveOperation(@RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");
        if (!name.matches("[\\w\\-.]+")) return badRequest("name contains invalid characters (use letters, digits, _ - .)");

        Object content = body.get("content");
        if (content == null) return badRequest("content is required");

        BatchProperties.OperationProperties op;
        try {
            op = objectMapper.convertValue(content, BatchProperties.OperationProperties.class);
        } catch (Exception e) {
            return badRequest("invalid operation JSON: " + e.getMessage());
        }
        if (op.getName() == null || op.getName().isBlank()) op.setName(name);

        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", "");
        String filePath = null;
        if (!dataDir.isBlank()) {
            Path dir = Path.of(dataDir).resolve("operations");
            Files.createDirectories(dir);
            Path file = dir.resolve(name + ".json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
            filePath = file.toAbsolutePath().toString();
        }

        try {
            batchService.registerOperation(op);
        } catch (Exception e) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", filePath != null ? "saved_with_errors" : "error");
            response.put("error", e.getMessage());
            if (filePath != null) response.put("path", filePath);
            return ResponseEntity.ok(response);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "saved");
        response.put("name", op.getName());
        if (filePath != null) response.put("path", filePath);
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // GET /batch/operations  — flat key-value listing of all configured operations
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // POST /batch/operations/reload  — rescan classpath + DATADIR and reload all operations
    // -------------------------------------------------------------------------

    @PostMapping("/operations/reload")
    public ResponseEntity<Map<String, Object>> reloadOperations() {
        try {
            batchProperties.loadFromJson();
        } catch (Exception e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("status",  "error");
            err.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return ResponseEntity.badRequest().body(err);
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "reloaded");
        response.put("count",  batchProperties.getOperations().size());
        response.put("operations", batchProperties.getOperations().keySet());
        return ResponseEntity.ok(response);
    }

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
        RunRequest request;
        try {
            request = deserializeRunRequest(body);
        } catch (Exception e) {
            return badRequest(e.getMessage() != null ? e.getMessage() : "invalid request body");
        }
        return executeRun(request);
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

        // ASYNC via WebSocket: return 202 immediately; results stream over the caller's WS session.
        // The caller must supply wsSessionId (as a top-level property or inside "properties") matching
        // the session ID received in the {"type":"connected","sessionId":"..."} handshake message.
        if (request.executionMode() == ExecutionMode.ASYNC) {
            String wsSessionId = request.properties() != null ? request.properties().get("wsSessionId") : null;
            if (wsSessionId == null || wsSessionId.isBlank()) {
                return badRequest("wsSessionId is required when executionMode=ASYNC");
            }
            try {
                RunRequest resolvedReq = batchService.resolveAlias(request);
                batchWebSocketHandler.runAsyncOnSession(wsSessionId, resolvedReq);
            } catch (IllegalArgumentException e) {
                return badRequest(e.getMessage());
            } catch (Exception e) {
                return errorsResponse("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            }
            Map<String, Object> ack = new LinkedHashMap<>();
            ack.put("status",      "accepted");
            ack.put("wsSessionId", wsSessionId);
            ack.put("message",     "batch processing started asynchronously; results will stream to WebSocket session " + wsSessionId);
            return ResponseEntity.accepted().body(ack);
        }

        BatchService.BatchResult result;
        try {
            request = batchService.resolveAlias(request);
            result = batchService.run(request);
        } catch (BatchService.ValidationFailureException e) {
            return ResponseEntity.badRequest().body(e.getBody());
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return internalServerError(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
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

        // SINGLE mode: return the first result row as a flat JSON object so JSONata can use $.RESPONSEBODY
        if ("SINGLE".equalsIgnoreCase(request.operationType())) {
            List<Map<String, Object>> rows = result.results();
            Object singleResponse = rows.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(rows.get(0));
            if (request.jsonataTransform() != null) {
                // When source is set, evaluate it against the full response so paths like
                // "data[0].RESPONSEBODY" resolve correctly.
                boolean hasSource = request.jsonataTransform().source() != null
                        && !request.jsonataTransform().source().isBlank();
                Object transformInput = hasSource
                        ? buildHttpResponse(request.operation(), result, request.httpThreadCount())
                        : singleResponse;
                singleResponse = batchService.applyJsonataTransform(transformInput, request.jsonataTransform());
            }
            return ResponseEntity.ok(singleResponse);
        }

        Object httpResponse = buildHttpResponse(request.operation(), result, request.httpThreadCount());
        if (request.responseProcessor() != null && !request.responseProcessor().isBlank()) {
            try {
                httpResponse = batchService.applyResponseProcessor(httpResponse, request.responseProcessor(), request.operation());
            } catch (IllegalArgumentException e) {
                return badRequest(e.getMessage());
            } catch (Exception e) {
                return errorsResponse("responseProcessor", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            }
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
                null,  // templateName
                null,  // auth
                null,  // operationType
                null   // operationConfig
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
            "jsonataTransform", "templateName", "auth", "operationType");

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
    // -------------------------------------------------------------------------
    // GET /batch/joindataset        — list all joindataset config names
    // GET /batch/joindataset/{name} — get a specific joindataset config
    // -------------------------------------------------------------------------

    @GetMapping("/joindataset")
    public ResponseEntity<?> listJoinDatasets() {
        List<String> names = new ArrayList<>();
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath:joindataset/*.json");
            for (Resource r : resources) {
                String filename = r.getFilename();
                if (filename != null && filename.endsWith(".json")) {
                    names.add(filename.substring(0, filename.length() - 5));
                }
            }
            names.sort(String::compareTo);
        } catch (Exception ignored) {}
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", names);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/joindataset/{name}")
    public ResponseEntity<?> getJoinDataset(@PathVariable String name) throws Exception {
        if (!name.matches("[\\w\\-]+")) return badRequest("invalid joindataset name");
        String path = "joindataset/" + name + ".json";
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) return badRequest("joindataset not found: " + name);
            @SuppressWarnings("unchecked")
            Map<String, Object> content = objectMapper.readValue(is, Map.class);
            applyJoinDatasetDefaults(content);
            return ResponseEntity.ok(content);
        }
    }

    @SuppressWarnings("unchecked")
    private void applyJoinDatasetDefaults(Map<String, Object> config) {
        if (!(config.get("datasets") instanceof List<?> rawList)) return;
        for (Object rawDs : rawList) {
            if (!(rawDs instanceof Map<?, ?> dsRaw)) continue;
            Map<String, Object> ds = (Map<String, Object>) dsRaw;
            Object uk = ds.get("uniqueKey");
            boolean missingKey = !(uk instanceof List<?> l) || ((List<?>) l).isEmpty();
            if (missingKey) {
                Object cols = ds.get("columns");
                if (cols instanceof List<?> cl && !cl.isEmpty() && cl.get(0) instanceof Map<?, ?> cm
                        && cm.get("name") instanceof String firstCol) {
                    ds.put("uniqueKey", List.of(firstCol));
                }
            }
        }
    }

    RunRequest resolveTemplateToRequest(String name) throws Exception {
        Path file = templateDir().resolve(name + ".json");
        if (!Files.exists(file)) throw new IllegalArgumentException("template not found: " + name);
        Map<String, Object> templateMap = objectMapper.readValue(file.toFile(),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        return deserializeRunRequest(templateMap);
    }

    // -------------------------------------------------------------------------
    // GET /batch/resource?classpath=path/to/resource  — read a classpath resource as text
    // -------------------------------------------------------------------------

    @GetMapping("/resource")
    public ResponseEntity<?> getClasspathResource(@RequestParam String classpath) throws Exception {
        // Strip leading slashes and block path traversal
        String path = classpath.replace('\\', '/').replaceAll("(^/+|\\.\\./)", "").trim();
        if (path.isBlank()) return badRequest("classpath parameter is required");
        // Search all classpath locations (classpath* scans every JAR/directory on the path)
        try {
            org.springframework.core.io.support.PathMatchingResourcePatternResolver resolver =
                    new org.springframework.core.io.support.PathMatchingResourcePatternResolver();
            org.springframework.core.io.Resource[] resources = resolver.getResources("classpath*:" + path);
            if (resources.length > 0) {
                try (InputStream is = resources[0].getInputStream()) {
                    String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("content", content);
                    return ResponseEntity.ok(response);
                }
            }
        } catch (Exception ignored) {}
        // Also check DATADIR filesystem for runtime-generated resources not on the classpath
        try {
            String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", "");
            if (!dataDir.isBlank()) {
                java.nio.file.Path filePath = java.nio.file.Path.of(dataDir).resolve(path);
                if (java.nio.file.Files.exists(filePath)) {
                    String content = java.nio.file.Files.readString(filePath, StandardCharsets.UTF_8);
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("content", content);
                    return ResponseEntity.ok(response);
                }
            }
        } catch (Exception ignored) {}
        return badRequest("Resource not found: " + path);
    }

    // -------------------------------------------------------------------------
    // Template endpoints
    //
    // GET  /batch/operationTemplate                      — list saved templates
    // GET  /batch/operationTemplate?name=X&k=v           — run template X; extra params merged into top-level fields and inputHttpBody
    // GET  /batch/operationTemplate/{name}               — fetch template content as JSON
    // GET  /batch/operationTemplate/{name}/run?k=v       — run template X; extra params merged into top-level fields and inputHttpBody
    // POST /batch/operationTemplate  {"name","content"}  — save template
    // POST /batch/operationTemplate  {"name","k":"v"}    — run template with extra props
    // -------------------------------------------------------------------------

    @GetMapping("/operationTemplate")
    public ResponseEntity<?> getTemplates(@RequestParam Map<String, String> params) throws Exception {
        String name = params.get("name");
        if (name != null && !name.isBlank()) {
            Map<String, String> extraProps = new LinkedHashMap<>(params);
            extraProps.remove("name");
            return executeTemplateRun(name.trim(), extraProps);
        }
        // List mode — optional ?templateType=COMPARE filter
        String typeFilter = params.get("templateType");
        Path dir = templateDir();
        List<Map<String, Object>> templates = new ArrayList<>();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                List<Path> files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
                for (Path p : files) {
                    String templateName = p.getFileName().toString().replaceAll("\\.json$", "");
                    Object templateBody = objectMapper.readValue(p.toFile(), Object.class);
                    String templateType = "OPERATION";
                    if (templateBody instanceof Map<?,?> m && m.get("templateType") instanceof String t) {
                        templateType = t;
                    }
                    if (typeFilter != null && !typeFilter.equalsIgnoreCase(templateType)) continue;
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("templateName", templateName);
                    entry.put("templateBody", templateBody);
                    entry.put("templateType", templateType);
                    templates.add(entry);
                }
            } catch (Exception ignored) {}
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", templates);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/operationTemplate/{name}")
    public ResponseEntity<?> getTemplateContent(@PathVariable String name) throws Exception {
        Path file = templateDir().resolve(name + ".json");
        if (!Files.exists(file)) return badRequest("template not found: " + name);
        Object content = objectMapper.readValue(file.toFile(), Object.class);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name",    name);
        response.put("content", content);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/operationTemplate/{name}/run")
    public ResponseEntity<?> runTemplateByName(
            @PathVariable String name,
            @RequestParam Map<String, String> params) throws Exception {
        return executeTemplateRun(name.trim(), new LinkedHashMap<>(params));
    }

    @PostMapping("/operationTemplate")
    public ResponseEntity<?> postTemplate(
            @RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");

        if (body.containsKey("content")) {
            // Save mode
            if (!name.matches("[\\w\\-. ]+")) return badRequest("name contains invalid characters");
            Object content = body.get("content");
            String templateType = body.get("templateType") instanceof String t ? t.trim() : "OPERATION";
            // Embed templateType inside the stored content so it can be read back when listing
            Map<String, Object> storedContent;
            if (content instanceof Map<?,?> rawMap) {
                storedContent = new LinkedHashMap<>();
                rawMap.forEach((k, v) -> storedContent.put(String.valueOf(k), v));
            } else {
                storedContent = objectMapper.convertValue(content,
                        new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
            }
            storedContent.put("templateType", templateType);
            Path dir = templateDir();
            Files.createDirectories(dir);
            Path file = dir.resolve(name + ".json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), storedContent);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "saved");
            response.put("path",   file.toAbsolutePath().toString());
            return ResponseEntity.ok(response);
        }

        // Run mode — all keys except "name" become extra properties
        Map<String, String> extraProps = new LinkedHashMap<>();
        body.forEach((k, v) -> {
            if (!"name".equals(k) && !"templateType".equals(k) && v != null) extraProps.put(k, v.toString());
        });
        return executeTemplateRun(name, extraProps);
    }

    private ResponseEntity<?> executeTemplateRun(String name, Map<String, String> extraProps) throws Exception {
        Path file = templateDir().resolve(name + ".json");
        if (!Files.exists(file)) return badRequest("template not found: " + name);
        Map<String, Object> templateMap;
        try {
            templateMap = objectMapper.readValue(file.toFile(),
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return badRequest("failed to parse template '" + name + "': " + e.getMessage());
        }
        // Merge extra params into the template map so known RunRequest fields (responseProcessor,
        // debugMode, inputFilePath, etc.) are routed correctly, not buried in properties.
        Map<String, Object> mergedMap = new LinkedHashMap<>(templateMap);
        extraProps.forEach(mergedMap::put);
        // If the template declares a JSON-object inputHttpBody, also inject the extra params into
        // it so callers can supply dynamic API request parameters via query string.
        if (!extraProps.isEmpty()) {
            Object rawBody = templateMap.get("inputHttpBody");
            if (rawBody != null) {
                String bodyStr = rawBody instanceof String s ? s : objectMapper.writeValueAsString(rawBody);
                try {
                    Map<String, Object> bodyMap = objectMapper.readValue(bodyStr,
                            new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                    extraProps.forEach(bodyMap::put);
                    mergedMap.put("inputHttpBody", objectMapper.writeValueAsString(bodyMap));
                } catch (Exception ignored) {
                    // not a JSON-object body — leave it as-is
                }
            }
        }
        RunRequest request;
        try {
            request = deserializeRunRequest(mergedMap);
        } catch (Exception e) {
            return badRequest("failed to build request from template '" + name + "': "
                    + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
        return executeRun(request);
    }

    private Path templateDir() {
        String templateDir = serverPropertiesLoader.getProperties().getOrDefault("TEMPLATEDIR", ".");
        return Path.of(templateDir);
    }

    // -------------------------------------------------------------------------
    // Input Source Template endpoints
    //
    // GET  /batch/inputsourcetemplate          — list saved inputSource templates
    // GET  /batch/inputsourcetemplate/{name}   — get template content
    // POST /batch/inputsourcetemplate          — save template {"name","content"}
    // POST /batch/inputsourcetemplate/test     — test inputSource config, return rows
    // -------------------------------------------------------------------------

    private Path inputSourceTemplateDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("inputSourceTemplate");
    }

    @GetMapping("/inputsourcetemplate")
    public ResponseEntity<?> listInputSourceTemplates() throws Exception {
        Path dir = inputSourceTemplateDir();
        List<Map<String, Object>> templates = new ArrayList<>();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                List<Path> files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
                for (Path p : files) {
                    String name = p.getFileName().toString().replaceAll("\\.json$", "");
                    Object content = objectMapper.readValue(p.toFile(), Object.class);
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("name", name);
                    entry.put("content", content);
                    templates.add(entry);
                }
            } catch (Exception ignored) {}
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", templates);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/inputsourcetemplate/{name}")
    public ResponseEntity<?> getInputSourceTemplate(@PathVariable String name) throws Exception {
        Path file = inputSourceTemplateDir().resolve(name + ".json");
        if (!Files.exists(file)) return badRequest("inputSource template not found: " + name);
        Object content = objectMapper.readValue(file.toFile(), Object.class);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name", name);
        response.put("content", content);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/inputsourcetemplate")
    public ResponseEntity<?> saveInputSourceTemplate(@RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");
        if (!name.matches("[\\w\\-. ]+")) return badRequest("name contains invalid characters");
        Object content = body.get("content");
        if (content == null) return badRequest("content is required");
        Path dir = inputSourceTemplateDir();
        Files.createDirectories(dir);
        Path file = dir.resolve(name + ".json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "saved");
        response.put("name", name);
        response.put("path", file.toAbsolutePath().toString());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/inputsourcetemplate/test")
    public ResponseEntity<?> testInputSourceTemplate(@RequestBody Map<String, Object> body) throws Exception {
        Object inputSourceRaw = body.get("inputSource");
        if (inputSourceRaw == null) return badRequest("inputSource is required");
        com.mycompany.batch.config.BatchProperties.InputSourceProperties inputSource;
        try {
            inputSource = objectMapper.convertValue(inputSourceRaw,
                    com.mycompany.batch.config.BatchProperties.InputSourceProperties.class);
        } catch (Exception e) {
            return badRequest("invalid inputSource: " + e.getMessage());
        }
        try {
            List<DataRow> rows = batchService.readDataRowsFromInputSource(inputSource);
            List<Map<String, Object>> data = rows.stream().map(r -> r.getData()).toList();
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("data", data);
            response.put("count", data.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return badRequest("inputSource test failed: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // GET /batch/admin
    //   Returns server.json properties, dynamic runtime attributes, and the
    //   classpath/filesystem directories scanned for operations, templates, etc.
    // -------------------------------------------------------------------------

    @GetMapping("/admin")
    public ResponseEntity<?> getAdminInfo() throws Exception {
        Map<String, Object> response = new LinkedHashMap<>();

        response.put("appVersion", appVersion);

        // 1. Static server.json properties
        response.put("serverProperties", new LinkedHashMap<>(serverPropertiesLoader.getProperties()));

        // 2. Dynamic attributes added to every batch run
        Map<String, Object> dynamic = new LinkedHashMap<>();
        String hostname;
        try { hostname = java.net.InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { hostname = "localhost"; }
        long pid = ProcessHandle.current().pid();
        dynamic.put("HOSTNAME",   hostname);
        dynamic.put("PORTNUMBER", serverPort);
        dynamic.put("DATESTAMP",  java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")));
        dynamic.put("DATETIME",   java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
        dynamic.put("PROCESSID",  pid);
        dynamic.put("JVMID",      hostname + "." + pid);
        response.put("dynamicAttributes", dynamic);

        // 3. Resource directories scanned at startup and runtime
        Map<String, List<String>> dirs = new LinkedHashMap<>();
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", "");
        String templateDir = serverPropertiesLoader.getProperties().getOrDefault("TEMPLATEDIR",
                dataDir.isBlank() ? "" : Path.of(dataDir).resolve("operationTemplate").toString());

        dirs.put("operations",        resourceDirs("classpath*:operations/", dataDir.isBlank() ? null : Path.of(dataDir).resolve("operations").toString()));
        dirs.put("joindataset",       resourceDirs("classpath*:joindataset/", null));
        dirs.put("requestTemplate",   resourceDirs("classpath*:requestTemplate/", null));
        dirs.put("templates",         resourceDirs(null, templateDir.isBlank() ? null : templateDir));
        dirs.put("operationTemplate", resourceDirs("classpath*:operationTemplate/", null));
        response.put("resourceDirectories", dirs);

        return ResponseEntity.ok(response);
    }

    private List<String> resourceDirs(String classpathPattern, String filesystemPath) {
        List<String> result = new ArrayList<>();
        if (classpathPattern != null) {
            try {
                Resource[] res = new PathMatchingResourcePatternResolver().getResources(classpathPattern);
                for (Resource r : res) {
                    try { result.add(r.getURL().toString()); } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {}
            if (result.isEmpty()) result.add(classpathPattern + " (not found)");
        }
        if (filesystemPath != null && !filesystemPath.isBlank()) {
            Path p = Path.of(filesystemPath);
            result.add(p.toAbsolutePath() + (Files.isDirectory(p) ? " ✓" : " (not found)"));
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Page endpoints
    //
    // GET  /batch/page          — list saved page names (DATADIR/pages/ + classpath:pages/)
    // GET  /batch/page/{name}   — get page JSON content
    // POST /batch/page          — save page {"name","content"}
    // -------------------------------------------------------------------------

    private Path pageDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("pages");
    }

    @GetMapping("/page")
    public ResponseEntity<?> listPages() {
        List<String> names = new ArrayList<>();
        // Scan DATADIR/pages/
        Path dir = pageDir();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                stream.filter(p -> p.toString().endsWith(".json"))
                      .map(p -> p.getFileName().toString().replaceAll("\\.json$", ""))
                      .sorted()
                      .forEach(names::add);
            } catch (Exception ignored) {}
        }
        // Also scan classpath:pages/
        try {
            Resource[] resources = new PathMatchingResourcePatternResolver().getResources("classpath*:pages/*.json");
            for (Resource r : resources) {
                String filename = r.getFilename();
                if (filename != null && filename.endsWith(".json")) {
                    String n = filename.substring(0, filename.length() - 5);
                    if (!names.contains(n)) names.add(n);
                }
            }
        } catch (Exception ignored) {}
        names.sort(String::compareTo);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", names);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/page/{name}")
    public ResponseEntity<?> getPage(@PathVariable String name) throws Exception {
        if (!name.matches("[\\w\\-.]+")) return badRequest("invalid page name");
        // Try DATADIR/pages/ first
        Path file = pageDir().resolve(name + ".json");
        if (Files.exists(file)) {
            Object content = objectMapper.readValue(file.toFile(), Object.class);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("name", name);
            response.put("content", content);
            return ResponseEntity.ok(response);
        }
        // Fall back to classpath:pages/
        String classpathResource = "pages/" + name + ".json";
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(classpathResource)) {
            if (is != null) {
                Object content = objectMapper.readValue(is, Object.class);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("name", name);
                response.put("content", content);
                return ResponseEntity.ok(response);
            }
        } catch (Exception ignored) {}
        return badRequest("page not found: " + name);
    }

    @PostMapping("/page")
    public ResponseEntity<?> savePage(@RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");
        if (!name.matches("[\\w\\-.]+")) return badRequest("name contains invalid characters (use letters, digits, _ - .)");
        Object content = body.get("content");
        if (content == null) return badRequest("content is required");
        Path dir = pageDir();
        Files.createDirectories(dir);
        Path file = dir.resolve(name + ".json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "saved");
        response.put("name", name);
        response.put("path", file.toAbsolutePath().toString());
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // JSON Schema endpoints
    //
    // GET  /batch/jsonschema          — list saved schema names (DATADIR/jsonschemas/)
    // GET  /batch/jsonschema/{name}   — get schema JSON content
    // POST /batch/jsonschema          — save schema {"name","content"}
    // -------------------------------------------------------------------------

    private Path jsonSchemaDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("jsonschemas");
    }

    @GetMapping("/jsonschema")
    public ResponseEntity<?> listJsonSchemas() {
        List<String> names = new ArrayList<>();
        Path dir = jsonSchemaDir();
        if (Files.isDirectory(dir)) {
            try (var stream = Files.list(dir)) {
                stream.filter(p -> p.toString().endsWith(".json"))
                      .map(p -> p.getFileName().toString().replaceAll("\\.json$", ""))
                      .sorted()
                      .forEach(names::add);
            } catch (Exception ignored) {}
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("names", names);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/jsonschema/{name}")
    public ResponseEntity<?> getJsonSchema(@PathVariable String name) throws Exception {
        if (!name.matches("[\\w\\-.]+")) return badRequest("invalid schema name");
        Path file = jsonSchemaDir().resolve(name + ".json");
        if (Files.exists(file)) {
            Object content = objectMapper.readValue(file.toFile(), Object.class);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("name", name);
            response.put("content", content);
            return ResponseEntity.ok(response);
        }
        return badRequest("schema not found: " + name);
    }

    @PostMapping("/jsonschema")
    public ResponseEntity<?> saveJsonSchema(@RequestBody Map<String, Object> body) throws Exception {
        String name = body.get("name") instanceof String s ? s.trim() : "";
        if (name.isBlank()) return badRequest("name is required");
        if (!name.matches("[\\w\\-.]+")) return badRequest("name contains invalid characters (use letters, digits, _ - .)");
        Object content = body.get("content");
        if (content == null) return badRequest("content is required");
        Path dir = jsonSchemaDir();
        Files.createDirectories(dir);
        Path file = dir.resolve(name + ".json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "saved");
        response.put("name", name);
        response.put("path", file.toAbsolutePath().toString());
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // Operation Config endpoints
    //
    // GET    /batch/operationConfig          — list all config names
    // GET    /batch/operationConfig/{config} — get a specific config
    // POST   /batch/operationConfig          — save a config {"content":{...}}
    // DELETE /batch/operationConfig/{config} — delete a config
    // -------------------------------------------------------------------------

    private Path operationConfigDir() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("operationConfig");
    }

    @GetMapping("/operationConfig")
    public ResponseEntity<?> listOperationConfigs() {
        List<String> names = new ArrayList<>();
        Path dir = operationConfigDir();
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

    @GetMapping("/operationConfig/{config}")
    public ResponseEntity<?> getOperationConfig(@PathVariable String config) throws Exception {
        if (!config.matches("[\\w\\-.]+")) return badRequest("invalid config name");
        Path file = operationConfigDir().resolve(config + ".json");
        if (!Files.exists(file)) return badRequest("config not found: " + config);
        Object content = objectMapper.readValue(file.toFile(), Object.class);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("config", config);
        response.put("content", content);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/operationConfig")
    public ResponseEntity<?> saveOperationConfig(@RequestBody Map<String, Object> body) throws Exception {
        Object content = body.get("content");
        if (content == null) return badRequest("content is required");
        String configName = null;
        if (content instanceof Map<?, ?> m && m.get("config") instanceof String s) {
            configName = s.trim();
        }
        if (configName == null || configName.isBlank()) return badRequest("content.config is required");
        if (!configName.matches("[\\w\\-.]+")) return badRequest("config name contains invalid characters");
        Path dir = operationConfigDir();
        Files.createDirectories(dir);
        Path file = dir.resolve(configName + ".json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), content);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "saved");
        response.put("config", configName);
        response.put("path", file.toAbsolutePath().toString());
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/operationConfig/{config}")
    public ResponseEntity<?> deleteOperationConfig(@PathVariable String config) throws Exception {
        if (!config.matches("[\\w\\-.]+")) return badRequest("invalid config name");
        Path file = operationConfigDir().resolve(config + ".json");
        if (!Files.exists(file)) return badRequest("config not found: " + config);
        Files.delete(file);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "deleted");
        response.put("config", config);
        return ResponseEntity.ok(response);
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return errorsResponse("validation", message);
    }

    private ResponseEntity<Map<String, Object>> internalServerError(String message) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("activity", "error");
        err.put("message",  message != null ? message : "Unknown error");
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("Errors", List.of(err));
        return ResponseEntity.internalServerError().body(body);
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
