package com.mycompany.batch.web;

import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * WebSocket endpoint registered at {@code /batch/ws}.
 *
 * <p>Accepts a JSON message with the same shape as {@link RunRequest} and returns
 * the same JSON structure as the REST {@code POST /batch/run} endpoint.
 *
 * <p>Example client message:
 * <pre>{@code
 * {
 *   "operation":  "pubmed",
 *   "inputSource":"REQUEST",
 *   "ids":        ["38000001", "38000002"],
 *   "outputData": "HTTP"
 * }
 * }</pre>
 *
 * <p>Example client message writing results to a file:
 * <pre>{@code
 * {
 *   "operation":     "pubmed",
 *   "inputSource":   "FILE",
 *   "inputFilePath": "/data/ids.csv",
 *   "outputData":    "FILE",
 *   "outputFilePath":"/data/out.psv"
 * }
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
        Map<String, Object> response;
        try {
            RunRequest request = objectMapper.readValue(message.getPayload(), RunRequest.class);

            if (request.operation() == null || request.operation().isBlank()) {
                throw new IllegalArgumentException("operation is required");
            }

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

        } catch (Exception e) {
            response = new LinkedHashMap<>();
            response.put("error",   true);
            response.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }

        session.sendMessage(new TextMessage(objectMapper.writeValueAsString(response)));
    }

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
