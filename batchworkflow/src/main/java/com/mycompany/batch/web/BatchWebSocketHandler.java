package com.mycompany.batch.web;

import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.model.DataRow;
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

    public BatchWebSocketHandler(BatchService batchService,
                                 BatchController batchController,
                                 BatchProperties batchProperties,
                                 ObjectMapper objectMapper) {
        this.batchService = batchService;
        this.batchController = batchController;
        this.batchProperties = batchProperties;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            RunRequest request = objectMapper.readValue(message.getPayload(), RunRequest.class);

            if (request.operation() == null || request.operation().isBlank()) {
                throw new IllegalArgumentException("operation is required");
            }

            if ("ASYNC".equalsIgnoreCase(request.executionMode())) {
                handleAsync(session, request);
            } else {
                handleSync(session, request);
            }

        } catch (Exception e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error",   true);
            err.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            sendWsSafe(session, objectMapper.writeValueAsString(err));
        }
    }

    // -------------------------------------------------------------------------
    // SYNC path — process everything, send one response
    // -------------------------------------------------------------------------

    private void handleSync(WebSocketSession session, RunRequest request) throws Exception {
        Map<String, Object> response;
        BatchService.BatchResult result = batchService.run(request);
        String outputData = resolveOutputData(request);

        if ("FILE".equals(outputData)) {
            String outputFilePath = resolveOutputFilePath(request);
            if (outputFilePath == null || outputFilePath.isBlank()) {
                throw new IllegalArgumentException("outputFilePath is required when outputData=FILE");
            }
            batchService.writeToPsv(result, outputFilePath);
            response = batchController.buildFileResponse(request.operation(), result, outputFilePath);
        } else {
            response = batchController.buildHttpResponse(request.operation(), result);
        }

        sendWsSafe(session, objectMapper.writeValueAsString(response));
    }

    // -------------------------------------------------------------------------
    // ASYNC path — ACK immediately, stream rows, send done
    // -------------------------------------------------------------------------

    private void handleAsync(WebSocketSession session, RunRequest request) throws Exception {
        // Resolve alias first so all preset fields are visible to buildInputRows and runAsync
        request = batchService.resolveAlias(request);
        // Build input rows synchronously so we know the count for the ACK
        List<DataRow> rows = batchService.buildInputRows(request);
        String batchUuid = UUID.randomUUID().toString();

        // Send ACK before any processing starts
        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("type",      "ack");
        ack.put("batchUuid", batchUuid);
        ack.put("rowCount",  rows.size());
        ack.put("message",   "batch " + batchUuid
                + " processing asynchronously with " + rows.size() + " rows");
        sendWsSafe(session, objectMapper.writeValueAsString(ack));

        // Kick off async processing — handler returns immediately after this call
        batchService.runAsync(rows, request, row -> {
            try {
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
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("type",      "error");
                err.put("batchUuid", batchUuid);
                err.put("message",   cause.getMessage() != null
                        ? cause.getMessage() : cause.getClass().getSimpleName());
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

    private String resolveOutputData(RunRequest request) {
        if (request.outputData() != null && !request.outputData().isBlank()) {
            return request.outputData().trim().toUpperCase();
        }
        try {
            return batchProperties.getOperation(request.operation())
                    .getOutputData().getType().trim().toUpperCase();
        } catch (Exception e) {
            return "HTTP";
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
