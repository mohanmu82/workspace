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
                .hasMessageContaining("only a time series chart filters");
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
}
