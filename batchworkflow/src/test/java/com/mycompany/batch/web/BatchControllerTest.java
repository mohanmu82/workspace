package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.ExecutionMode;
import com.mycompany.batch.model.HttpMethod;
import com.mycompany.batch.model.InputSourceType;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchControllerTest {

    @Mock BatchService batchService;
    @Mock BatchProperties batchProperties;
    @Mock CacheFactory cacheFactory;
    @Mock ServerPropertiesLoader serverPropertiesLoader;
    @Mock BatchWebSocketHandler batchWebSocketHandler;

    BatchController controller;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        controller = new BatchController(batchService, batchProperties, cacheFactory,
                objectMapper, serverPropertiesLoader, batchWebSocketHandler);
    }

    // -------------------------------------------------------------------------
    // deserializeRunRequest
    // -------------------------------------------------------------------------

    @Test
    void deserializeRunRequest_knownFieldsOnly_mapsDirectlyWithNoProperties() {
        Map<String, Object> body = Map.of("operation", "myOp");
        RunRequest result = controller.deserializeRunRequest(body);
        assertThat(result.operation()).isEqualTo("myOp");
        assertThat(result.properties()).isNull();
    }

    @Test
    void deserializeRunRequest_unknownTopLevelKey_promotedToProperties() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("operation", "myOp");
        body.put("username", "alice");
        body.put("token", "abc123");

        RunRequest result = controller.deserializeRunRequest(body);

        assertThat(result.operation()).isEqualTo("myOp");
        assertThat(result.properties())
                .containsEntry("username", "alice")
                .containsEntry("token", "abc123");
    }

    @Test
    void deserializeRunRequest_explicitPropertiesOverrideUnknownTopLevelKeys() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("operation", "myOp");
        body.put("username", "unknown-user");
        body.put("properties", Map.of("username", "explicit-user"));

        RunRequest result = controller.deserializeRunRequest(body);

        assertThat(result.properties()).containsEntry("username", "explicit-user");
    }

    @Test
    void deserializeRunRequest_withInputSourceFile_parsedToEnum() {
        Map<String, Object> body = Map.of("operation", "myOp", "inputSource", "FILE");
        RunRequest result = controller.deserializeRunRequest(body);
        assertThat(result.inputSource()).isEqualTo(InputSourceType.FILE);
    }

    @Test
    void deserializeRunRequest_withIdsAndInputCount_parsedCorrectly() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("operation", "myOp");
        body.put("ids", List.of("id1", "id2", "id3"));
        body.put("inputCount", 2);

        RunRequest result = controller.deserializeRunRequest(body);

        assertThat(result.ids()).containsExactly("id1", "id2", "id3");
        assertThat(result.inputCount()).isEqualTo(2);
    }

    @Test
    void deserializeRunRequest_withExecutionModeAsync_parsedToEnum() {
        Map<String, Object> body = Map.of("operation", "myOp", "executionMode", "ASYNC");
        RunRequest result = controller.deserializeRunRequest(body);
        assertThat(result.executionMode()).isEqualTo(ExecutionMode.ASYNC);
    }

    // -------------------------------------------------------------------------
    // buildHttpResponse
    // -------------------------------------------------------------------------

    @Test
    void buildHttpResponse_containsExpectedStructure() {
        BatchProperties.OperationProperties op = mock(BatchProperties.OperationProperties.class);
        BatchProperties.HttpProperties http = mock(BatchProperties.HttpProperties.class);
        when(batchProperties.getOperation("testOp")).thenReturn(op);
        when(op.getEffectiveHttp()).thenReturn(http);
        when(http.getMethod()).thenReturn(HttpMethod.GET);
        when(http.getThreadCount()).thenReturn(5);

        var result = buildResult(3, 2, 1, List.of(Map.of("id", "1")));

        Map<String, Object> response = controller.buildHttpResponse("testOp", result, null);

        assertThat(response).containsKeys("metadata", "columns", "data");
        assertThat(response.get("data")).isEqualTo(result.results());
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildHttpResponse_summaryFieldsMatchBatchResult() {
        BatchProperties.OperationProperties op = mock(BatchProperties.OperationProperties.class);
        BatchProperties.HttpProperties http = mock(BatchProperties.HttpProperties.class);
        when(batchProperties.getOperation("op")).thenReturn(op);
        when(op.getEffectiveHttp()).thenReturn(http);
        when(http.getMethod()).thenReturn(HttpMethod.POST);
        when(http.getThreadCount()).thenReturn(10);

        var result = buildResult(5, 4, 1, List.of());

        Map<String, Object> response = controller.buildHttpResponse("op", result, null);
        Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");
        Map<String, Object> summary = (Map<String, Object>) metadata.get("summary");

        assertThat(summary).containsEntry("processed", 5);
        assertThat(summary).containsEntry("succeeded", 4);
        assertThat(summary).containsEntry("failed", 1);
        assertThat(summary).containsEntry("timeTakenMs", 150L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildHttpResponse_withThreadCountOverride_usesOverride() {
        BatchProperties.OperationProperties op = mock(BatchProperties.OperationProperties.class);
        BatchProperties.HttpProperties http = mock(BatchProperties.HttpProperties.class);
        when(batchProperties.getOperation("op")).thenReturn(op);
        when(op.getEffectiveHttp()).thenReturn(http);
        when(http.getMethod()).thenReturn(HttpMethod.GET);

        var result = buildResult(0, 0, 0, List.of());

        Map<String, Object> response = controller.buildHttpResponse("op", result, 20);
        Map<String, Object> httpStats = (Map<String, Object>)
                ((Map<String, Object>) response.get("metadata")).get("httpStats");

        assertThat(httpStats).containsEntry("threadCount", 20);
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildHttpResponse_withNullThreadCountOverride_usesOperationDefault() {
        BatchProperties.OperationProperties op = mock(BatchProperties.OperationProperties.class);
        BatchProperties.HttpProperties http = mock(BatchProperties.HttpProperties.class);
        when(batchProperties.getOperation("op")).thenReturn(op);
        when(op.getEffectiveHttp()).thenReturn(http);
        when(http.getMethod()).thenReturn(HttpMethod.GET);
        when(http.getThreadCount()).thenReturn(8);

        var result = buildResult(0, 0, 0, List.of());

        Map<String, Object> response = controller.buildHttpResponse("op", result, null);
        Map<String, Object> httpStats = (Map<String, Object>)
                ((Map<String, Object>) response.get("metadata")).get("httpStats");

        assertThat(httpStats).containsEntry("threadCount", 8);
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildHttpResponse_metadataContainsBatchUuidAndOperation() {
        BatchProperties.OperationProperties op = mock(BatchProperties.OperationProperties.class);
        BatchProperties.HttpProperties http = mock(BatchProperties.HttpProperties.class);
        when(batchProperties.getOperation("myOp")).thenReturn(op);
        when(op.getEffectiveHttp()).thenReturn(http);
        when(http.getMethod()).thenReturn(HttpMethod.GET);
        when(http.getThreadCount()).thenReturn(1);

        var result = buildResult(0, 0, 0, List.of());

        Map<String, Object> response = controller.buildHttpResponse("myOp", result, null);
        Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");

        assertThat(metadata).containsEntry("batchUuid", "test-uuid");
        assertThat(metadata).containsEntry("operation", "myOp");
        assertThat(metadata).containsEntry("timestamp", "2024-01-01T00:00:00");
    }

    // -------------------------------------------------------------------------
    // buildFileResponse
    // -------------------------------------------------------------------------

    @Test
    void buildFileResponse_containsOutputFileAndMetadata() {
        var result = buildResult(10, 9, 1, List.of());

        Map<String, Object> response = controller.buildFileResponse("fileOp", result, "/output/data.psv");

        assertThat(response).containsEntry("outputFile", "/output/data.psv");
        assertThat(response).containsKey("metadata");
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildFileResponse_metadataContainsExpectedFields() {
        var result = buildResult(10, 9, 1, List.of());

        Map<String, Object> response = controller.buildFileResponse("fileOp", result, "/output/data.psv");
        Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");

        assertThat(metadata).containsEntry("batchUuid", "test-uuid");
        assertThat(metadata).containsEntry("operation", "fileOp");
        assertThat(metadata).containsKey("summary");
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildFileResponse_summaryFieldsMatchBatchResult() {
        var result = buildResult(10, 9, 1, List.of());

        Map<String, Object> response = controller.buildFileResponse("fileOp", result, "/out.psv");
        Map<String, Object> summary = (Map<String, Object>)
                ((Map<String, Object>) response.get("metadata")).get("summary");

        assertThat(summary).containsEntry("processed", 10);
        assertThat(summary).containsEntry("succeeded", 9);
        assertThat(summary).containsEntry("failed", 1);
    }

    // -------------------------------------------------------------------------
    // errorsResponse
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void errorsResponse_returns400_withErrorsListContainingActivityAndMessage() {
        ResponseEntity<Map<String, Object>> response =
                controller.errorsResponse("validation", "name is required");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        Map<String, Object> body = java.util.Objects.requireNonNull(response.getBody());
        List<Map<String, Object>> errors = (List<Map<String, Object>>) body.get("Errors");
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0))
                .containsEntry("activity", "validation")
                .containsEntry("message", "name is required");
    }

    @Test
    @SuppressWarnings("unchecked")
    void errorsResponse_withNullMessage_substitutesUnknownError() {
        ResponseEntity<Map<String, Object>> response = controller.errorsResponse("error", null);

        Map<String, Object> body = java.util.Objects.requireNonNull(response.getBody());
        List<Map<String, Object>> errors = (List<Map<String, Object>>) body.get("Errors");
        assertThat(errors.get(0)).containsEntry("message", "Unknown error");
    }

    // -------------------------------------------------------------------------
    // clearCache
    // -------------------------------------------------------------------------

    @Test
    void clearCache_delegatesToCacheFactory_andReturnsClearedStatus() {
        ResponseEntity<Map<String, Object>> response = controller.clearCache("myCache");

        verify(cacheFactory).clear("myCache");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @SuppressWarnings("unchecked")
    void clearCache_responseBodyContainsClearedStatus() {
        ResponseEntity<Map<String, Object>> response = controller.clearCache("myCache");

        List<Map<String, Object>> data = (List<Map<String, Object>>)
                java.util.Objects.requireNonNull(response.getBody()).get("data");
        assertThat(data).hasSize(1);
        assertThat(data.get(0)).containsEntry("status", "CLEARED");
        assertThat(data.get(0).get("message").toString()).contains("myCache");
    }

    // -------------------------------------------------------------------------
    // executeRun early-exit paths
    // -------------------------------------------------------------------------

    @Test
    void executeRun_withNullOperation_returnsBadRequest() throws Exception {
        RunRequest req = minimalRequest(null);
        ResponseEntity<?> response = controller.executeRun(req);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void executeRun_withBlankOperation_returnsBadRequest() throws Exception {
        RunRequest req = minimalRequest("  ");
        ResponseEntity<?> response = controller.executeRun(req);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void executeRun_withAsyncModeAndNoWsSessionId_returnsBadRequest() throws Exception {
        RunRequest req = new RunRequest("myOp", null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null,
                ExecutionMode.ASYNC, null, null, null, null, null,
                null, null, null, null, null, null);

        ResponseEntity<?> response = controller.executeRun(req);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private RunRequest minimalRequest(String operation) {
        return new RunRequest(operation, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private BatchService.BatchResult buildResult(int processed, int succeeded, int failed,
                                                  List<Map<String, Object>> data) {
        return new BatchService.BatchResult(processed, succeeded, failed,
                new BatchService.HttpStats(10L, 100L, 50.0),
                List.of(),
                data,
                "test-uuid",
                "2024-01-01T00:00:00",
                150L,
                1.5,
                Map.of());
    }
}
