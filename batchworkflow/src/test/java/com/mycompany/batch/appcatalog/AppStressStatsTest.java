package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** The latency figures a page action's stress run (repeat &gt; 1) reports. */
class AppStressStatsTest {

    private static AppUseCaseInstanceOutput run(String status, long timeTaken) {
        return new AppUseCaseInstanceOutput(
                "x", 0L, "i", "label", "app", "uat", "uat", "uc",
                Map.of(), status, 200, 1, 1, timeTaken, "http://host/x", List.of(),
                "GET", "LOCAL", null, null, null, Map.of(), Map.of(), null, null, null, null);
    }

    @Test
    void reportsCountsPercentilesAndThroughput() {
        // 1..100 ms, the last ten failing — out of order, as parallel runs finish.
        List<AppUseCaseInstanceOutput> runs = IntStream.rangeClosed(1, 100)
                .map(i -> 101 - i)
                .mapToObj(ms -> run(ms > 90 ? "FAILED" : "SUCCESS", ms))
                .toList();

        Map<String, Object> s = AppExecutionService.latencyStats(runs, 2000);

        assertThat(s.get("repeat")).isEqualTo(100);
        assertThat(s.get("succeeded")).isEqualTo(90L);
        assertThat(s.get("failed")).isEqualTo(10L);
        assertThat(s.get("minMs")).isEqualTo(1L);
        assertThat(s.get("maxMs")).isEqualTo(100L);
        assertThat(s.get("avgMs")).isEqualTo(51L);
        assertThat(s.get("p50Ms")).isEqualTo(50L);
        assertThat(s.get("p95Ms")).isEqualTo(95L);
        assertThat(s.get("p99Ms")).isEqualTo(99L);
        assertThat(s.get("requestsPerSecond")).isEqualTo(50.0);
        assertThat((List<?>) s.get("timings")).first().isEqualTo(100L);
    }

    @Test
    void singleRunIsEveryPercentile() {
        Map<String, Object> s = AppExecutionService.latencyStats(List.of(run("SUCCESS", 42)), 0);
        assertThat(s.get("p50Ms")).isEqualTo(42L);
        assertThat(s.get("p99Ms")).isEqualTo(42L);
        assertThat(s.get("requestsPerSecond")).isEqualTo(0.0);
    }
}
