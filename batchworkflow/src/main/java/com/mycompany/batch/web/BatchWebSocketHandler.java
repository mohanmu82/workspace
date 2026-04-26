package com.mycompany.batch.web;

import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.model.DataRow;
import com.mycompany.batch.model.ExecutionMode;
import com.mycompany.batch.model.OutputDataType;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * WebSocket endpoint registered at {@code /batch/ws}.
 *
 * <p>Supports two execution modes controlled by {@code executionMode} in the request:
 *
 * <h3>SYNC (default)</h3>
 * Behaves identically to the REST endpoint: processes all rows, then sends a single
 * response JSON with the complete result set.
 *
 * <h3>ASYNC</h3>
 * Immediately sends an ACK message, then streams each completed row individually as
 * it finishes, followed by a final "done" message containing batch metadata.
 *
 * <pre>{@code
 * // ACK
 * {"type":"ack","batchUuid":"…","rowCount":5,"message":"batch … processing asynchronously with 5 rows"}
 *
 * // Per-row (one message per completed DataRow)
 * {"type":"row","batchUuid":"…","row":{…}}
 *
 * // Completion
 * {"type":"done","batchUuid":"…","metadata":{…},"columns":[…]}
 *
 * // Error (unexpected failure)
 * {"type":"error","batchUuid":"…","message":"…"}
 * }</pre>
 */
@Component
public class BatchWebSocketHandler extends TextWebSocketHandler {

    private final BatchService batchService;
    private final BatchController batchController;
    private final BatchProperties batchProperties;
    private final ObjectMapper objectMapper;
    private final CacheFactory cacheFactory;

    public BatchWebSocketHandler(BatchService batchService,
                                 BatchController batchController,
                                 BatchProperties batchProperties,
                                 ObjectMapper objectMapper,
                                 CacheFactory cacheFactory) {
        this.batchService = batchService;
        this.batchController = batchController;
        this.batchProperties = batchProperties;
        this.objectMapper = objectMapper;
        this.cacheFactory = cacheFactory;
    }

    @Override
    protected void handleTextMessage(@org.springframework.lang.NonNull WebSocketSession session, @org.springframework.lang.NonNull TextMessage message) throws Exception {
        try {
            Map<String, Object> body = objectMapper.readValue(message.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
            RunRequest request = batchController.deserializeRunRequest(body);

            if (request.operation() == null || request.operation().isBlank()) {
                throw new IllegalArgumentException("operation is required");
            }

            if (request.executionMode() == ExecutionMode.ASYNC) {
                handleAsync(session, request);
            } else {
                handleSync(session, request);
            }

        } catch (Exception e) {
            Map<String, Object> errEntry = new LinkedHashMap<>();
            errEntry.put("activity", "validation");
            errEntry.put("message",  e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("Errors", List.of(errEntry));
            sendWsSafe(session, objectMapper.writeValueAsString(err));
        }
    }

    // -------------------------------------------------------------------------
    // SYNC path — process everything, send one response
    // -------------------------------------------------------------------------

    private void handleSync(WebSocketSession session, RunRequest request) throws Exception {
        Object response;
        BatchService.BatchResult result = batchService.run(request);
        batchController.saveToRuntimeCache(request, result.batchUuid(), result.timestamp());

        OutputDataType outputData = resolveOutputData(request);

        if (outputData == OutputDataType.FILE) {
            String outputFilePath = resolveOutputFilePath(request);
            if (outputFilePath == null || outputFilePath.isBlank()) {
                throw new IllegalArgumentException("outputFilePath is required when outputData=FILE");
            }
            outputFilePath = batchService.resolvePath(outputFilePath, result.operationProperties());
            batchService.writeToPsv(result, outputFilePath, Boolean.TRUE.equals(request.appendOutput()));
            response = batchController.buildFileResponse(request.operation(), result, outputFilePath);
        } else {
            response = batchController.buildHttpResponse(request.operation(), result, request.httpThreadCount());
            if (request.responseProcessor() != null && !request.responseProcessor().isBlank()) {
                response = batchService.applyResponseProcessor(response, request.responseProcessor());
            }
        }

        if (request.jsonataTransform() != null) {
            response = batchService.applyJsonataTransform(response, request.jsonataTransform());
        }
        sendWsSafe(session, objectMapper.writeValueAsString(response));
    }

    // -------------------------------------------------------------------------
    // ASYNC path — ACK immediately, stream rows, send done
    // -------------------------------------------------------------------------

    private void handleAsync(WebSocketSession session, RunRequest request) throws Exception {
        final RunRequest resolvedReq = batchService.resolveAlias(request);
        final java.util.Map<String, String> opProperties = batchService.loadRequestProperties(resolvedReq);
        final List<DataRow> rows = batchService.buildInputRows(resolvedReq, opProperties);

        final OutputDataType outputData = resolveOutputData(request);
        final String resolvedOutputFilePath;
        if (outputData == OutputDataType.FILE) {
            String raw = resolveOutputFilePath(request);
            if (raw == null || raw.isBlank()) {
                throw new IllegalArgumentException("outputFilePath is required when outputData=FILE");
            }
            resolvedOutputFilePath = batchService.resolvePath(raw, opProperties);
        } else {
            resolvedOutputFilePath = null;
        }

        String batchUuid = UUID.randomUUID().toString();
        batchController.saveToRuntimeCache(request, batchUuid,
                java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        // Send ACK before any processing starts
        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("type",      "ack");
        ack.put("batchUuid", batchUuid);
        ack.put("rowCount",  rows.size());
        ack.put("message",   "batch " + batchUuid
                + " processing asynchronously with " + rows.size() + " rows");
        sendWsSafe(session, objectMapper.writeValueAsString(ack));

        // State for per-row PSV streaming (FILE mode only)
        final Object fileLock          = new Object();
        final boolean[]    headerDone  = {false};
        final List[]       psvCols     = {null};

        // Kick off async processing — handler returns immediately after this call
        batchService.runAsync(rows, resolvedReq, row -> {
            try {
                if (outputData == OutputDataType.FILE) {
                    synchronized (fileLock) {
                        try {
                            if (!headerDone[0]) {
                                List<String> cols = batchService.initPsvStream(
                                        row, resolvedOutputFilePath,
                                        Boolean.TRUE.equals(resolvedReq.appendOutput()));
                                psvCols[0] = cols;
                                headerDone[0] = true;
                                batchService.appendPsvRow(row, cols, resolvedOutputFilePath);
                            } else {
                                @SuppressWarnings("unchecked")
                                List<String> cols = (List<String>) psvCols[0];
                                batchService.appendPsvRow(row, cols, resolvedOutputFilePath);
                            }
                        } catch (Exception ignored) {}
                    }
                }
                Map<String, Object> msg = new LinkedHashMap<>();
                msg.put("type",      "row");
                msg.put("batchUuid", batchUuid);
                msg.put("row",       row);
                sendWsSafe(session, objectMapper.writeValueAsString(msg));
            } catch (Exception ignored) {}
        }).thenAccept(result -> {
            try {
                Map<String, Object> meta = new LinkedHashMap<>();
                meta.put("processed",      result.processed());
                meta.put("succeeded",      result.succeeded());
                meta.put("failed",         result.failed());
                meta.put("timeTakenMs",    result.timeTakenMs());
                meta.put("responseSizeKb", result.responseSizeKb());
                meta.put("timestamp",      result.timestamp());
                meta.put("batchUuid",      batchUuid);
                if (resolvedReq.httpThreadCount() != null) meta.put("threadCount", resolvedReq.httpThreadCount());
                if (outputData == OutputDataType.FILE) meta.put("outputFile", resolvedOutputFilePath);

                Map<String, Object> done = new LinkedHashMap<>();
                done.put("type",      "done");
                done.put("batchUuid", batchUuid);
                done.put("metadata",  meta);
                done.put("columns",   result.columns());
                sendWsSafe(session, objectMapper.writeValueAsString(done));
            } catch (Exception ignored) {}
        }).exceptionally(ex -> {
            try {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                Map<String, Object> errEntry = new LinkedHashMap<>();
                errEntry.put("activity", "error");
                errEntry.put("message",  cause.getMessage() != null
                        ? cause.getMessage() : cause.getClass().getSimpleName());
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("type",      "error");
                err.put("batchUuid", batchUuid);
                err.put("Errors",    List.of(errEntry));
                sendWsSafe(session, objectMapper.writeValueAsString(err));
            } catch (Exception ignored) {}
            return null;
        });
    }

    // -------------------------------------------------------------------------
    // Thread-safe WebSocket send (called from thread-pool threads in ASYNC mode)
    // -------------------------------------------------------------------------

    private void sendWsSafe(WebSocketSession session, String text) {
        try {
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(text));
                }
            }
        } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private OutputDataType resolveOutputData(RunRequest request) {
        if (request.outputData() != null) return request.outputData();
        try {
            OutputDataType t = batchProperties.getOperation(request.operation()).getOutputData().getType();
            return t != null ? t : OutputDataType.HTTP;
        } catch (Exception e) {
            return OutputDataType.HTTP;
        }
    }

    private String resolveOutputFilePath(RunRequest request) {
        if (request.outputFilePath() != null && !request.outputFilePath().isBlank()) {
            return request.outputFilePath();
        }
        try {
            return batchProperties.getOperation(request.operation())
                    .getOutputData().getOutputFilePath();
        } catch (Exception e) {
            return null;
        }
    }
}
