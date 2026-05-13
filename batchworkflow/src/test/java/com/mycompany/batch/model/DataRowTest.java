package com.mycompany.batch.model;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataRowTest {

    @Test
    void defaultConstructor_createsEmptyCollections() {
        DataRow row = new DataRow();
        assertThat(row.getData()).isEmpty();
        assertThat(row.getMetadata()).isEmpty();
        assertThat(row.getNamedOutputs()).isEmpty();
        assertThat(row.getDatasets()).isEmpty();
        assertThat(row.getResponseBody()).isNull();
        assertThat(row.getExpandedRows()).isNull();
        assertThat(row.getLastHttpStatusCode()).isZero();
    }

    @Test
    void mapConstructor_populatesData() {
        Map<String, Object> initial = Map.of("id", "123", "name", "Alice");
        DataRow row = new DataRow(initial);
        assertThat(row.getData()).containsEntry("id", "123").containsEntry("name", "Alice");
        assertThat(row.getMetadata()).isEmpty();
    }

    @Test
    void putNamedOutput_storesValue_getNamedOutput_retrievesIt() {
        DataRow row = new DataRow();
        row.putNamedOutput("activity1.RESULT", "value1");
        assertThat(row.getNamedOutput("activity1.RESULT")).isEqualTo("value1");
    }

    @Test
    void getNamedOutput_returnsNull_forMissingKey() {
        DataRow row = new DataRow();
        assertThat(row.getNamedOutput("nonexistent")).isNull();
    }

    @Test
    void responseBody_setterAndGetter() {
        DataRow row = new DataRow();
        row.setResponseBody("{\"status\":\"ok\"}");
        assertThat(row.getResponseBody()).isEqualTo("{\"status\":\"ok\"}");
    }

    @Test
    void httpStatusCode_defaultsToZero_setterAndGetter() {
        DataRow row = new DataRow();
        assertThat(row.getLastHttpStatusCode()).isZero();
        row.setLastHttpStatusCode(200);
        assertThat(row.getLastHttpStatusCode()).isEqualTo(200);
    }

    @Test
    void expandedRows_setterAndGetter() {
        DataRow row = new DataRow();
        assertThat(row.getExpandedRows()).isNull();
        List<Map<String, Object>> expanded = List.of(Map.of("a", "1"), Map.of("a", "2"));
        row.setExpandedRows(expanded);
        assertThat(row.getExpandedRows()).hasSize(2).containsExactlyElementsOf(expanded);
    }

    @Test
    void toResponseMap_withoutMetadata_includesDataExcludesMetadataAndSequenceNumber() {
        DataRow row = new DataRow(Map.of("id", "123", "SEQUENCE_NUMBER", 1));
        row.getMetadata().put("activity1_url", "http://example.com");

        Map<String, Object> result = row.toResponseMap(false);

        assertThat(result).containsEntry("id", "123");
        assertThat(result).doesNotContainKey("SEQUENCE_NUMBER");
        assertThat(result).doesNotContainKey("activity1_url");
    }

    @Test
    void toResponseMap_withMetadata_mergesMetadataAfterData() {
        DataRow row = new DataRow(Map.of("id", "456"));
        row.getMetadata().put("timeTakenMs", 123L);
        row.getMetadata().put("activity1_url", "http://example.com");

        Map<String, Object> result = row.toResponseMap(true);

        assertThat(result).containsEntry("id", "456");
        assertThat(result).containsEntry("timeTakenMs", 123L);
        assertThat(result).containsEntry("activity1_url", "http://example.com");
    }

    @Test
    void toResponseMap_doesNotMutateOriginalData() {
        DataRow row = new DataRow(Map.of("id", "789"));
        row.toResponseMap(false);
        assertThat(row.getData()).containsKey("id").doesNotContainKey("SEQUENCE_NUMBER");
    }

    @Test
    void datasets_mapIsAccessible_andMutable() {
        DataRow row = new DataRow();
        Map<String, Object> innerRow = Map.of("col", "val");
        row.getDatasets().put("myVar", Map.of("key1", innerRow));
        assertThat(row.getDatasets()).containsKey("myVar");
        assertThat(row.getDatasets().get("myVar")).containsKey("key1");
    }
}
