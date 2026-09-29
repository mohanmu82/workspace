package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A line chart: rows read along an axis that counts rather than one that tells the time.
 *
 * <p>The whole of what makes it a different control from a time series is that it needs no column to
 * be an axis. A row's x is its position in the rows — 1 for the first that came back, n for the nth —
 * so every tick along the bottom is a line of the response, and a response whose <em>order</em> is
 * the sequence can be charted at all: a log, a run of samples, a paged fetch, none of which has a
 * timestamp in it to read along instead. So the tests here are mostly about what it does <em>not</em>
 * have to name.
 */
class AppPageLineChartTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        return control;
    }

    private static AppPage page(AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setControls(List.of(controls));
        return page;
    }

    @Test
    void aLineChartNamingNothingAtAll_isComplete() {
        // Not an omission: with no x column the axis is the line number, which is the point of it.
        assertThatCode(() -> AppCatalogService.validateChartControls(page(control("l", "line"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aLineChartNamingAnXColumn_isFine() {
        AppPageControl chart = control("l", "line");
        chart.setXField("sequence");
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart)))
                .doesNotThrowAnyException();
    }

    @Test
    void aBlankXColumn_readsAsNone() {
        AppPageControl chart = control("l", "line");
        chart.setXField("  ");
        assertThat(chart.getXField()).isNull();
        chart.setXField("  seq  ");
        assertThat(chart.getXField()).isEqualTo("seq");
    }

    @Test
    void aLineChartFiltersItsRowsLikeATimeSeries() {
        AppPageControl chart = control("l", "line");
        chart.setChartFilters(List.of(new AppPageRowFilter("status", "EQUALS", "${statusPick}"),
                new AppPageRowFilter("host", null, "prod")));
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart)))
                .doesNotThrowAnyException();
    }

    @Test
    void aFilterWithNoColumnOrAnUnknownTest_isRefused() {
        AppPageControl chart = control("l", "line");
        chart.setChartFilters(List.of(new AppPageRowFilter(" ", "EQUALS", "x")));
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("names no column");
        chart.setChartFilters(List.of(new AppPageRowFilter("status", "LIKE", "x")));
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .hasMessageContaining("unknown test");
    }

    @Test
    void anXColumnOnAnythingButALineChart_isRefused() {
        AppPageControl grid = control("g", "grid");
        grid.setXField("seq");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(grid)))
                .hasMessageContaining("only a line chart has an x column");
    }

    @Test
    void anActionMayFillOneAndAGridMayFeedIt() {
        AppPageControl chart = control("l", "line");
        AppPageAction action = new AppPageAction();
        action.setTargetControlId("l");
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(chart), action, "Action"))
                .doesNotThrowAnyException();

        chart.setSourceGridControlId("g");
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart, control("g", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aLineChartCannotBeGivenATabPerRow() {
        // A fan-out giving each row its own tab needs a tab set to put them in; a chart has one
        // reading of one call and nowhere to hold forty.
        AppPageAction action = new AppPageAction();
        action.setTargetControlId("l");
        action.setRowSourceControlId("g");
        action.setRowOutputMode("TABS");
        assertThatThrownBy(() -> AppCatalogService.validateActionTarget(
                page(control("l", "line"), control("g", "grid")), action, "Action"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a tab set");
    }
}
