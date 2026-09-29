package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A time series chart: it names its time column, its filters name real tests, it can be filled by an
 * action, drawn from a grid or laid out as a tab — and nothing else carries its settings.
 */
class AppPageTimeSeriesTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        return control;
    }

    private static AppPageControl series(String id) {
        AppPageControl chart = control(id, "timeseries");
        chart.setTimeField("createdAt");
        return chart;
    }

    private static AppPage page(AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setControls(List.of(controls));
        return page;
    }

    @Test
    void aTimeSeriesNamingItsTimeColumn_isFine() {
        AppPageControl chart = series("ts");
        chart.setChartFilters(List.of(new AppPageRowFilter("status", "EQUALS", "${statusPick}"),
                new AppPageRowFilter("host", null, "prod")));
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart))).doesNotThrowAnyException();
    }

    @Test
    void aTimeSeriesWithNoTimeColumn_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(control("ts", "timeseries"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names no time column");
    }

    @Test
    void aFilterWithNoColumnOrAnUnknownTest_isRefused() {
        AppPageControl chart = series("ts");
        chart.setChartFilters(List.of(new AppPageRowFilter(" ", "EQUALS", "x")));
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("names no column");
        chart.setChartFilters(List.of(new AppPageRowFilter("status", "LIKE", "x")));
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("unknown test");
    }

    @Test
    void timeSeriesSettingsOnAnythingElse_areRefused() {
        AppPageControl bar = control("bar", "bar");
        bar.setTimeField("createdAt");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(bar)))
                .hasMessageContaining("only a time series chart has a time column");
        AppPageControl grid = control("g", "grid");
        grid.setChartFilters(List.of(new AppPageRowFilter("status", "EQUALS", "x")));
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(grid)))
                .hasMessageContaining("only a time series or line chart filters");
    }

    @Test
    void aTimeColumnOnALineChart_isRefused() {
        // A line chart counts along its x axis; a column of instants belongs on the chart that reads
        // one, and left here it would be a setting nothing ever looked at.
        AppPageControl chart = control("l", "line");
        chart.setTimeField("createdAt");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("a time column belongs on a time series chart");
    }

    @Test
    void anXColumnOnATimeSeries_isRefused() {
        AppPageControl chart = series("ts");
        chart.setXField("seq");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("an x column belongs on a line chart");
    }

    @Test
    void bucketAndAggregateFallBackToTheirDefaults() {
        AppPageControl chart = series("ts");
        assertThat(chart.getTimeBucket()).isEqualTo("AUTO");
        assertThat(chart.getTimeAggregate()).isEqualTo("SUM");
        chart.setTimeBucket("fifteen_minutes");
        chart.setTimeAggregate("avg");
        assertThat(chart.getTimeBucket()).isEqualTo("FIFTEEN_MINUTES");
        assertThat(chart.getTimeAggregate()).isEqualTo("AVG");
        chart.setTimeBucket("fortnight");
        chart.setTimeAggregate("median");
        assertThat(chart.getTimeBucket()).isEqualTo("AUTO");
        assertThat(chart.getTimeAggregate()).isEqualTo("SUM");
    }

    @Test
    void aTimeSeriesCanBeFilledByAnActionDrawnFromAGridAndBeATab() {
        AppPageControl chart = series("ts");
        AppPageAction action = new AppPageAction();
        action.setTargetControlId("ts");
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(chart), action, "Action"))
                .doesNotThrowAnyException();

        chart.setSourceGridControlId("g");
        AppPageControl tabs = control("tabs", "tabs");
        tabs.setTabControlIds(List.of("ts"));
        AppPage page = page(chart, control("g", "grid"), tabs);
        assertThatCode(() -> AppCatalogService.validateChartControls(page)).doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateTabs(page)).doesNotThrowAnyException();
    }

    // ── The offset column ────────────────────────────────────────────────────
    // For rows whose instant is in two columns rather than one: a business date, and the seconds or
    // minutes into it. Neither says when the row happened on its own.

    @Test
    void aTimeSeriesAddingASecondsColumnToItsDate_isFine() {
        AppPageControl chart = series("ts");
        chart.setTimeField("RUN_DATE");
        chart.setTimeOffsetField("ELAPSED_SECS");
        chart.setTimeOffsetUnit("SECONDS");
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart))).doesNotThrowAnyException();
    }

    @Test
    void theOffsetUnitFallsBackToSeconds() {
        AppPageControl chart = series("ts");
        assertThat(chart.getTimeOffsetUnit()).isEqualTo("SECONDS");
        chart.setTimeOffsetUnit("minutes");
        assertThat(chart.getTimeOffsetUnit()).isEqualTo("MINUTES");
        chart.setTimeOffsetUnit("fortnights");
        assertThat(chart.getTimeOffsetUnit()).isEqualTo("SECONDS");
    }

    @Test
    void anOffsetReadFromTheTimeColumnItself_isRefused() {
        // A date plus itself in seconds is not an instant — the offset is a second column, added on
        // top of the first. Matched however either is capitalised, as every column name here is.
        AppPageControl chart = series("ts");
        chart.setTimeField("RUN_DATE");
        chart.setTimeOffsetField("run_date");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same column as its time");
    }

    @Test
    void anOffsetColumnOnAnythingButATimeSeries_isRefused() {
        // It is something added to a time column, so a chart with no time column has nothing to add
        // it to and would carry a setting nothing would ever read.
        AppPageControl line = control("l", "line");
        line.setTimeOffsetField("elapsedSecs");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(line)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset column belongs on a time series");

        AppPageControl pie = control("p", "pie");
        pie.setTimeOffsetField("elapsedSecs");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(pie)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a time series chart adds an offset column");
    }
}
