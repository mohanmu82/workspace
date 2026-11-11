package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DbPublishServiceTest {

    @Test
    void identifiersAreSafeUnquotedInBothDialects() {
        assertThat(DbPublishService.identifier("Order Status (ms)")).isEqualTo("order_status_ms");
        assertThat(DbPublishService.identifier("2024 total")).isEqualTo("c_2024_total");
        assertThat(DbPublishService.identifier("date")).isEqualTo("date_");
        assertThat(DbPublishService.identifier("")).isEqualTo("c_");
        assertThat(DbPublishService.identifier("a".repeat(40))).hasSize(DbPublishService.MAX_IDENTIFIER);
    }

    @Test
    void columnNamesStayDistinctAndClearOfTheFixedColumns() {
        Map<String, String> names = DbPublishService.columnNames(List.of("Source", "source", "Run UUID", "a b", "a-b", "a b"));
        assertThat(names).containsExactly(
                Map.entry("Source", "source_2"),
                Map.entry("source", "source_3"),
                Map.entry("Run UUID", "run_uuid_2"),
                Map.entry("a b", "a_b"),
                Map.entry("a-b", "a_b_2"));
    }

    @Test
    void createTableCarriesTheRunColumnsFirst() {
        assertThat(DbPublishService.createTable("t", List.of("x", "y"), "TEXT"))
                .isEqualTo("CREATE TABLE t (run_uuid VARCHAR(36), captured_at TIMESTAMP, source TEXT, x TEXT, y TEXT)");
    }

    @Test
    void schedulerReadsRowsFromOneTargetOrSeveral() {
        List<Map<String, Object>> one = List.of(Map.of("a", 1));
        assertThat(DbPublishScheduler.rowsOf(one)).isEqualTo(one);
        assertThat(DbPublishScheduler.rowsOf(Map.of("grid", one, "label", "text"))).isEqualTo(one);
        assertThat(DbPublishScheduler.rowsOf("text")).isNull();
        assertThat(DbPublishScheduler.rowsOf(Map.of("label", "text"))).isNull();
    }
}
