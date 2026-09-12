package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the performance summary makes of a pile of executions: one line per app, environment and use
 * case, counting every call that was made and reporting how long they took.
 *
 * <p>The three questions worth pinning down are the ones somebody reading the grid will ask of it.
 * Are failures counted — they are, because a call that took nine seconds to fail is the one being
 * looked for. Is the average rounded or truncated — rounded, because every number beside it is a
 * whole millisecond. And what order do the lines come in — busiest first, so the endpoint the page
 * exists to find is at the top rather than wherever it last happened to run.
 */
class AppPerformanceSummaryTest {

    private static AppUseCaseInstanceOutput run(String app, String environment, String useCase,
                                                String status, long timeTaken) {
        return new AppUseCaseInstanceOutput(
                "x", 0L, "i", "label", app, environment, "uat", useCase,
                Map.of(), status, 200, 1, 1, timeTaken, "http://host/x", List.of(),
                "GET", "LOCAL", null, null, null, Map.of(), Map.of(), null, null, null, null);
    }

    private static AppPerformanceRow only(List<AppPerformanceRow> rows) {
        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    @Test
    void runsOfOneUseCase_becomeOneRowCarryingItsCountAndTimings() {
        AppPerformanceRow row = only(AppExecutionService.summarise(List.of(
                run("orders", "uat", "getOrder", "SUCCESS", 100),
                run("orders", "uat", "getOrder", "SUCCESS", 300),
                run("orders", "uat", "getOrder", "SUCCESS", 200)), null, null, null));

        assertThat(row.app()).isEqualTo("orders");
        assertThat(row.environment()).isEqualTo("uat");
        assertThat(row.useCase()).isEqualTo("getOrder");
        assertThat(row.requestCount()).isEqualTo(3);
        assertThat(row.avgTimeTaken()).isEqualTo(200);
        assertThat(row.maxTimeTaken()).isEqualTo(300);
    }

    @Test
    void sameUseCaseInTwoEnvironments_staysTwoRows() {
        // The whole reason the environment is part of the key: "getOrder is slow" is not a useful
        // thing to be told when it is slow in one environment and fine in the other.
        List<AppPerformanceRow> rows = AppExecutionService.summarise(List.of(
                run("orders", "uat",  "getOrder", "SUCCESS", 100),
                run("orders", "prod", "getOrder", "SUCCESS", 900)), null, null, null);

        assertThat(rows).extracting(AppPerformanceRow::environment).containsExactlyInAnyOrder("uat", "prod");
        assertThat(rows).allSatisfy(row -> assertThat(row.requestCount()).isEqualTo(1));
    }

    @Test
    void failedAndErroredCalls_areCountedWithTheSuccessfulOnes() {
        AppPerformanceRow row = only(AppExecutionService.summarise(List.of(
                run("orders", "uat", "getOrder", "SUCCESS", 100),
                run("orders", "uat", "getOrder", "FAILED",  500),
                run("orders", "uat", "getOrder", "ERROR",  9000)), null, null, null));

        assertThat(row.requestCount()).isEqualTo(3);
        // The nine-second failure is exactly what someone opening this grid came to find, so it is
        // the maximum rather than something dropped for not having succeeded.
        assertThat(row.maxTimeTaken()).isEqualTo(9000);
        assertThat(row.avgTimeTaken()).isEqualTo(3200);
    }

    @Test
    void averageIsRounded_ratherThanTruncated() {
        AppPerformanceRow row = only(AppExecutionService.summarise(List.of(
                run("orders", "uat", "getOrder", "SUCCESS", 10),
                run("orders", "uat", "getOrder", "SUCCESS", 11)), null, null, null));
        assertThat(row.avgTimeTaken()).isEqualTo(11);
    }

    @Test
    void busiestEndpointComesFirst() {
        List<AppPerformanceRow> rows = AppExecutionService.summarise(List.of(
                run("orders",   "uat", "getOrder",  "SUCCESS", 10),
                run("payments", "uat", "getStatus", "SUCCESS", 10),
                run("payments", "uat", "getStatus", "SUCCESS", 10),
                run("payments", "uat", "getStatus", "SUCCESS", 10)), null, null, null);

        assertThat(rows).extracting(AppPerformanceRow::app).containsExactly("payments", "orders");
    }

    @Test
    void tiedCounts_areOrderedByNameSoTheGridDoesNotShuffleBetweenReadings() {
        List<AppPerformanceRow> rows = AppExecutionService.summarise(List.of(
                run("zeta",  "uat", "run", "SUCCESS", 10),
                run("alpha", "uat", "run", "SUCCESS", 10)), null, null, null);

        assertThat(rows).extracting(AppPerformanceRow::app).containsExactly("alpha", "zeta");
    }

    @Test
    void filters_narrowToWhatTheyName() {
        List<AppUseCaseInstanceOutput> runs = List.of(
                run("orders",   "uat",  "getOrder",  "SUCCESS", 10),
                run("orders",   "prod", "getOrder",  "SUCCESS", 10),
                run("payments", "uat",  "getStatus", "SUCCESS", 10));

        assertThat(AppExecutionService.summarise(runs, "orders", null, null)).hasSize(2);
        assertThat(AppExecutionService.summarise(runs, "orders", "uat", null)).hasSize(1);
        assertThat(only(AppExecutionService.summarise(runs, null, null, "getStatus")).app()).isEqualTo("payments");
    }

    @Test
    void blankFilters_meanEveryAppRatherThanAnAppWithNoName() {
        List<AppUseCaseInstanceOutput> runs = List.of(
                run("orders",   "uat", "getOrder",  "SUCCESS", 10),
                run("payments", "uat", "getStatus", "SUCCESS", 10));
        assertThat(AppExecutionService.summarise(runs, "", "   ", null)).hasSize(2);
    }

    @Test
    void filtersIgnoreCase_sinceTheyUsuallyComeOffADropdownTheOperatorTyped() {
        assertThat(AppExecutionService.summarise(
                List.of(run("Orders", "UAT", "getOrder", "SUCCESS", 10)), "orders", "uat", null)).hasSize(1);
    }

    @Test
    void noRunsAtAll_isAnEmptySummaryRatherThanAFailure() {
        assertThat(AppExecutionService.summarise(List.of(), null, null, null)).isEmpty();
    }

    @Test
    void aRunThatRecordedNoEnvironment_stillGroupsUnderTheEmptyName() {
        // A run with nothing in the field is still a run that happened, and dropping it would make
        // the counts on the page disagree with the counts on Global Runs.
        AppPerformanceRow row = only(AppExecutionService.summarise(
                List.of(run("orders", null, "getOrder", "SUCCESS", 40)), null, null, null));
        assertThat(row.environment()).isEmpty();
        assertThat(row.requestCount()).isEqualTo(1);
    }
}
