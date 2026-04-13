package com.mycompany.batch.web;

import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/batch")
public class BatchController {

    private final BatchService batchService;
    private final BatchProperties batchProperties;

    public BatchController(BatchService batchService, BatchProperties batchProperties) {
        this.batchService = batchService;
        this.batchProperties = batchProperties;
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

            // HTTP properties (flat)
            entry.put("http.url",          op.getHttp().getUrl());
            entry.put("http.method",        op.getHttp().getMethod());
            entry.put("http.contentType",   op.getHttp().getContentType());
            entry.put("http.bodyTemplate",  op.getHttp().getBodyTemplate());
            entry.put("http.threadCount",   op.getHttp().getThreadCount());

            // Custom HTTP headers (flat)
            op.getHttp().getHeader().forEach((k, v) -> entry.put("http.header." + k, v));

            // XPath properties (flat)
            entry.put("xpath.config",       op.getXpath().getConfig());
            entry.put("xpath.threadCount",  op.getXpath().getThreadCount());

            // Auth properties (flat)
            String authMethod = op.getAuth().getMethod().trim().toUpperCase();
            entry.put("auth.method", authMethod);
            switch (authMethod) {
                case "BASIC" -> {
                    entry.put("auth.basic.username", op.getAuth().getBasic().getUsername());
                    entry.put("auth.basic.password", "***");
                }
                case "JWT" -> {
                    entry.put("auth.jwt.url",             op.getAuth().getJwt().getUrl());
                    entry.put("auth.jwt.applicationName", op.getAuth().getJwt().getApplicationName());
                    entry.put("auth.jwt.username",        op.getAuth().getJwt().getUsername());
                    entry.put("auth.jwt.password",        "***");
                }
                case "KERBEROS" -> {
                    entry.put("auth.kerberos.username",         op.getAuth().getKerberos().getUsername());
                    entry.put("auth.kerberos.keytab",           op.getAuth().getKerberos().getKeytab());
                    entry.put("auth.kerberos.servicePrincipal", op.getAuth().getKerberos().getServicePrincipal());
                }
            }

            // Input source properties (flat)
            String inputType = op.getInputSource().getType();
            entry.put("inputSource.type", inputType);
            if ("HTTPCONFIG".equalsIgnoreCase(inputType)) {
                entry.put("inputSource.httpConfig.url",             op.getInputSource().getHttpConfig().getUrl());
                entry.put("inputSource.httpConfig.method",          op.getInputSource().getHttpConfig().getMethod());
                entry.put("inputSource.httpConfig.jsonataTransform",op.getInputSource().getHttpConfig().getJsonataTransform());
            }

            // Output data properties (flat)
            entry.put("outputData.type", op.getOutputData().getType());

            // Data extraction properties (flat)
            String extractType = op.getDataExtraction().getType();
            entry.put("dataExtraction.type", extractType);
            if ("JSON".equalsIgnoreCase(extractType)) {
                entry.put("dataExtraction.jsonataTransform", op.getDataExtraction().getJsonataTransform());
            }

            data.add(entry);
        });

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data", data);
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
    public ResponseEntity<Map<String, Object>> runGet(
            @RequestParam Map<String, String> params) throws Exception {
        return executeRun(buildRunRequestFromParams(params));
    }

    // -------------------------------------------------------------------------
    // POST /batch/run (form / query params) — mirrors the GET endpoint
    // -------------------------------------------------------------------------

    @PostMapping(value = "/run",
            consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE,
                        MediaType.MULTIPART_FORM_DATA_VALUE})
    public ResponseEntity<Map<String, Object>> runPostForm(
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
    public ResponseEntity<Map<String, Object>> runPostJson(
            @RequestBody RunRequest request) throws Exception {
        return executeRun(request);
    }

    // -------------------------------------------------------------------------
    // Legacy PSV endpoints (kept for backward compatibility)
    // -------------------------------------------------------------------------

    @GetMapping("/run/psv")
    public ResponseEntity<Map<String, Object>> runPsvGet(
            @RequestParam Map<String, String> params) throws Exception {
        return runPsv(params);
    }

    @PostMapping("/run/psv")
    public ResponseEntity<Map<String, Object>> runPsvPost(
            @RequestParam Map<String, String> params) throws Exception {
        return runPsv(params);
    }

    // -------------------------------------------------------------------------
    // Shared execution logic
    // -------------------------------------------------------------------------

    /** Shared entry point for all run endpoints. */
    ResponseEntity<Map<String, Object>> executeRun(RunRequest request) throws Exception {
        if (request.operationType() == null || request.operationType().isBlank()) {
            return badRequest("operationType is required");
        }

        BatchService.BatchResult result;
        try {
            result = batchService.run(request);
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }

        String outputData = resolveOutputData(request);

        if ("FILE".equals(outputData)) {
            String outputFilePath = request.outputFilePath();
            if (outputFilePath == null || outputFilePath.isBlank()) {
                try {
                    outputFilePath = batchProperties.getOperation(request.operationType())
                            .getOutputData().getOutputFilePath();
                } catch (Exception ignored) {}
            }
            if (outputFilePath == null || outputFilePath.isBlank()) {
                return badRequest("outputFilePath is required when outputData=FILE");
            }
            try {
                batchService.writeToPsv(result, outputFilePath);
            } catch (Exception e) {
                return badRequest("Failed to write output file: " + e.getMessage());
            }
            return ResponseEntity.ok(buildFileResponse(request.operationType(), result,
                    Path.of(outputFilePath).toAbsolutePath().toString()));
        }

        return ResponseEntity.ok(buildHttpResponse(request.operationType(), result));
    }

    private String resolveOutputData(RunRequest request) {
        if (request.outputData() != null && !request.outputData().isBlank()) {
            return request.outputData().trim().toUpperCase();
        }
        try {
            return batchProperties.getOperation(request.operationType())
                    .getOutputData().getType().trim().toUpperCase();
        } catch (Exception e) {
            return "HTTP";
        }
    }

    /** Builds the full JSON response returned to HTTP clients and the WebSocket. */
    Map<String, Object> buildHttpResponse(String operationType, BatchService.BatchResult result) {
        BatchProperties.OperationProperties op = batchProperties.getOperation(operationType);
        Map<String, Object> httpStats = new LinkedHashMap<>();
        httpStats.put("method",      op.getHttp().getMethod());
        httpStats.put("threadCount", op.getHttp().getThreadCount());
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
        metadata.put("operationType", operationType);
        metadata.put("httpStats",    httpStats);
        metadata.put("summary",      summary);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata", metadata);
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
        metadata.put("operationType", operationType);
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
                params.get("operationType"),
                params.get("inputSource"),
                params.get("inputFilePath"),
                ids,
                parseInputCount(params.get("inputCount")),
                params.get("outputData"),
                params.get("outputFilePath"),
                parseInputCount(params.get("debugMode")));
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
        metadata.put("operationType", operationType);
        metadata.put("summary",       summary);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata",   metadata);
        response.put("outputFile", result.outputFile());
        return ResponseEntity.ok(response);
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

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error",   true);
        body.put("message", message);
        return ResponseEntity.badRequest().body(body);
    }
}
