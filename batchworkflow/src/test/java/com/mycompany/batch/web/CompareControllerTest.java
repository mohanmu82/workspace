package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.service.BatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CompareController comparison logic.
 * Uses a real BatchController (with real ObjectMapper) so deserializeRunRequest works,
 * and mocks BatchService to control the datasets returned for each run.
 */
@ExtendWith(MockitoExtension.class)
class CompareControllerTest {

    @Mock BatchService batchService;
    @Mock BatchProperties batchProperties;
    @Mock CacheFactory cacheFactory;
    @Mock ServerPropertiesLoader serverPropertiesLoader;
    @Mock BatchWebSocketHandler batchWebSocketHandler;

    CompareController controller;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        BatchController batchController = new BatchController(batchService, batchProperties,
                cacheFactory, objectMapper, serverPropertiesLoader, batchWebSocketHandler);
        controller = new CompareController(batchService, batchController);
    }

    // -------------------------------------------------------------------------
    // Request validation
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_missingDataset1_returnsBadRequest() {
        Map<String, Object> body = Map.of(
                "key1", "id",
                "dataset2", Map.of("operation", "op2"));

        ResponseEntity<?> response = controller.compareDatasets(body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void compareDatasets_missingDataset2_returnsBadRequest() {
        Map<String, Object> body = Map.of(
                "key1", "id",
                "dataset1", Map.of("operation", "op1"));

        ResponseEntity<?> response = controller.compareDatasets(body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void compareDatasets_missingKey1_returnsBadRequest() {
        Map<String, Object> body = Map.of(
                "dataset1", Map.of("operation", "op1"),
                "dataset2", Map.of("operation", "op2"));

        ResponseEntity<?> response = controller.compareDatasets(body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void compareDatasets_keyNotFoundInDataset1_returnsBadRequest() throws Exception {
        List<Map<String, Object>> data1 = List.of(Map.of("otherId", "1", "val", "A"));
        List<Map<String, Object>> data2 = List.of(Map.of("id", "1", "val", "A"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        Map<String, Object> body = Map.of(
                "key1", "id",
                "dataset1", Map.of("operation", "op1"),
                "dataset2", Map.of("operation", "op2"));

        ResponseEntity<?> response = controller.compareDatasets(body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // -------------------------------------------------------------------------
    // Comparison status: SAME
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_identicalRows_allMarkedSame() throws Exception {
        List<Map<String, Object>> data = List.of(Map.of("id", "1", "name", "Alice"));
        when(batchService.run(any())).thenReturn(result(data), result(data));

        ResponseEntity<?> response = compareWith("id", "op1", "op2");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> rows = dataRows(response);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).containsEntry("compareStatus", "SAME");
    }

    @Test
    void compareDatasets_multipleIdenticalRows_allSame() throws Exception {
        List<Map<String, Object>> data = List.of(
                Map.of("id", "1", "x", "a"),
                Map.of("id", "2", "x", "b"),
                Map.of("id", "3", "x", "c"));
        when(batchService.run(any())).thenReturn(result(data), result(data));

        List<Map<String, Object>> rows = dataRows(compareWith("id", "op1", "op2"));

        assertThat(rows).hasSize(3);
        assertThat(rows).allMatch(r -> "SAME".equals(r.get("compareStatus")));
    }

    // -------------------------------------------------------------------------
    // Comparison status: MISMATCH
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_differentColumnValues_markedMismatch() throws Exception {
        List<Map<String, Object>> data1 = List.of(Map.of("id", "1", "name", "Alice"));
        List<Map<String, Object>> data2 = List.of(Map.of("id", "1", "name", "Bob"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        List<Map<String, Object>> rows = dataRows(compareWith("id", "op1", "op2"));

        assertThat(rows).hasSize(1);
        Map<String, Object> row = rows.get(0);
        assertThat(row).containsEntry("compareStatus", "MISMATCH");
        assertThat(row).containsEntry("name", "Alice");        // dataset1 value
        assertThat(row).containsEntry("__ds2_name", "Bob");   // dataset2 value
    }

    @Test
    @SuppressWarnings("unchecked")
    void compareDatasets_mismatch_listsAffectedColumns() throws Exception {
        List<Map<String, Object>> data1 = List.of(Map.of("id", "1", "a", "X", "b", "Y"));
        List<Map<String, Object>> data2 = List.of(Map.of("id", "1", "a", "X", "b", "Z"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        List<Map<String, Object>> rows = dataRows(compareWith("id", "op1", "op2"));

        Map<String, Object> row = rows.get(0);
        assertThat(row).containsEntry("compareStatus", "MISMATCH");
        List<String> mismatched = (List<String>) row.get("__mismatchedColumns");
        assertThat(mismatched).containsExactly("b");
    }

    // -------------------------------------------------------------------------
    // Comparison status: DATASET1_ONLY / DATASET2_ONLY
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_rowOnlyInDataset1_markedDataset1Only() throws Exception {
        List<Map<String, Object>> data1 = List.of(
                Map.of("id", "1", "name", "Alice"),
                Map.of("id", "2", "name", "Bob"));
        List<Map<String, Object>> data2 = List.of(Map.of("id", "1", "name", "Alice"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        List<Map<String, Object>> rows = dataRows(compareWith("id", "op1", "op2"));

        assertThat(rows).hasSize(2);
        assertThat(rows).anyMatch(r -> "DATASET1_ONLY".equals(r.get("compareStatus")));
        assertThat(rows).anyMatch(r -> "SAME".equals(r.get("compareStatus")));
    }

    @Test
    void compareDatasets_rowOnlyInDataset2_markedDataset2Only() throws Exception {
        List<Map<String, Object>> data1 = List.of(Map.of("id", "1", "name", "Alice"));
        List<Map<String, Object>> data2 = List.of(
                Map.of("id", "1", "name", "Alice"),
                Map.of("id", "3", "name", "Carol"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        List<Map<String, Object>> rows = dataRows(compareWith("id", "op1", "op2"));

        assertThat(rows).hasSize(2);
        assertThat(rows).anyMatch(r -> "DATASET2_ONLY".equals(r.get("compareStatus")));
    }

    // -------------------------------------------------------------------------
    // Summary counts
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_summaryCountsAreCorrect() throws Exception {
        List<Map<String, Object>> data1 = List.of(
                Map.of("id", "1", "val", "A"),  // SAME
                Map.of("id", "2", "val", "B"),  // MISMATCH
                Map.of("id", "3", "val", "C")   // DATASET1_ONLY
        );
        List<Map<String, Object>> data2 = List.of(
                Map.of("id", "1", "val", "A"),  // SAME
                Map.of("id", "2", "val", "X"),  // MISMATCH
                Map.of("id", "4", "val", "D")   // DATASET2_ONLY
        );
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        Map<String, Object> summary = summary(compareWith("id", "op1", "op2"));

        assertThat(summary).containsEntry("same", 1);
        assertThat(summary).containsEntry("mismatch", 1);
        assertThat(summary).containsEntry("dataset1Only", 1);
        assertThat(summary).containsEntry("dataset2Only", 1);
        assertThat(summary).containsEntry("total", 4);
    }

    // -------------------------------------------------------------------------
    // Column match statistics
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void compareDatasets_columnMatchPercent_calculatedCorrectly() throws Exception {
        List<Map<String, Object>> data1 = List.of(
                Map.of("id", "1", "val", "A"),
                Map.of("id", "2", "val", "B"));
        List<Map<String, Object>> data2 = List.of(
                Map.of("id", "1", "val", "A"),   // match
                Map.of("id", "2", "val", "X"));  // mismatch
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        Map<String, Object> cols = columns(compareWith("id", "op1", "op2"));
        Map<String, Integer> matchPct      = (Map<String, Integer>) cols.get("matchPct");
        Map<String, Integer> mismatchCount = (Map<String, Integer>) cols.get("mismatchCount");

        assertThat(matchPct.get("val")).isEqualTo(50);
        assertThat(mismatchCount.get("val")).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void compareDatasets_allMatch_columnMatchPct100() throws Exception {
        List<Map<String, Object>> data = List.of(
                Map.of("id", "1", "val", "A"),
                Map.of("id", "2", "val", "B"));
        when(batchService.run(any())).thenReturn(result(data), result(data));

        Map<String, Object> cols = columns(compareWith("id", "op1", "op2"));
        Map<String, Integer> matchPct = (Map<String, Integer>) cols.get("matchPct");

        assertThat(matchPct.get("val")).isEqualTo(100);
    }

    @Test
    @SuppressWarnings("unchecked")
    void compareDatasets_columnSets_reportedCorrectly() throws Exception {
        List<Map<String, Object>> data1 = List.of(Map.of("id", "1", "onlyIn1", "x", "common", "y"));
        List<Map<String, Object>> data2 = List.of(Map.of("id", "1", "onlyIn2", "z", "common", "y"));
        when(batchService.run(any())).thenReturn(result(data1), result(data2));

        Map<String, Object> cols = columns(compareWith("id", "op1", "op2"));

        assertThat((List<String>) cols.get("common")).contains("common");
        assertThat((List<String>) cols.get("dataset1Only")).contains("onlyIn1");
        assertThat((List<String>) cols.get("dataset2Only")).contains("onlyIn2");
    }

    // -------------------------------------------------------------------------
    // key2 defaults to key1 when omitted
    // -------------------------------------------------------------------------

    @Test
    void compareDatasets_key2DefaultsToKey1() throws Exception {
        List<Map<String, Object>> data = List.of(Map.of("id", "1", "val", "A"));
        when(batchService.run(any())).thenReturn(result(data), result(data));

        Map<String, Object> body = Map.of(
                "key1", "id",
                "dataset1", Map.of("operation", "op1"),
                "dataset2", Map.of("operation", "op2"));

        ResponseEntity<?> response = controller.compareDatasets(body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(dataRows(response)).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private ResponseEntity<?> compareWith(String key, String op1, String op2) {
        Map<String, Object> body = Map.of(
                "key1", key,
                "dataset1", Map.of("operation", op1),
                "dataset2", Map.of("operation", op2));
        return controller.compareDatasets(body);
    }

    private BatchService.BatchResult result(List<Map<String, Object>> data) {
        return new BatchService.BatchResult(data.size(), data.size(), 0,
                new BatchService.HttpStats(0L, 0L, 0.0),
                List.of(), data,
                "uuid-" + System.nanoTime(),
                "2024-01-01T00:00:00",
                100L, 0.5, null);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> dataRows(ResponseEntity<?> response) {
        Map<String, Object> body = (Map<String, Object>) Objects.requireNonNull(response.getBody());
        return (List<Map<String, Object>>) body.get("data");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> metadata(ResponseEntity<?> response) {
        Map<String, Object> body = (Map<String, Object>) Objects.requireNonNull(response.getBody());
        return (Map<String, Object>) body.get("metadata");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> summary(ResponseEntity<?> response) {
        return (Map<String, Object>) metadata(response).get("summary");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> columns(ResponseEntity<?> response) {
        return (Map<String, Object>) metadata(response).get("columns");
    }
}
