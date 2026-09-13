package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The settings the chart controls and tab sets carry: a pie-with-grids has to name a tab set that is
 * on the page, a default tab is a tab set's alone, and a bar chart is vertical unless told otherwise.
 */
class AppPageChartControlTest {

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
    void aPieWithGridsNamingATabSetOnThePage_isFine() {
        AppPageControl chart = control("chart", "piegrid");
        chart.setTabsControlId("tabs");
        assertThatCode(() -> AppCatalogService.validateChartControls(page(chart, control("tabs", "tabs"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aPieWithGridsNamingNoTabSet_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(control("chart", "piegrid"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names no tab set");
    }

    @Test
    void aPieWithGridsNamingSomethingThatIsNotATabSet_isRefused() {
        AppPageControl chart = control("chart", "piegrid");
        chart.setTabsControlId("g");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart, control("g", "grid"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a tab set");
        chart.setTabsControlId("gone");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(chart)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void aTabSetNamedOnAnythingButAPieWithGrids_isRefused() {
        AppPageControl pie = control("pie", "pie");
        pie.setTabsControlId("tabs");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(pie, control("tabs", "tabs"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a pie chart with grids");
    }

    @Test
    void aDefaultTabOnAnythingButATabSet_isRefused() {
        AppPageControl grid = control("g", "grid");
        grid.setDefaultTabControlId("x");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(grid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a tab set has a default tab");
    }

    @Test
    void aBarChartIsVerticalUnlessToldOtherwise() {
        AppPageControl bar = control("bar", "bar");
        assertThat(bar.getOrientation()).isEqualTo("VERTICAL");
        bar.setOrientation("horizontal");
        assertThat(bar.getOrientation()).isEqualTo("HORIZONTAL");
        bar.setOrientation("sideways");
        assertThat(bar.getOrientation()).isEqualTo("VERTICAL");
    }

    @Test
    void aChartIsSomethingAnActionCanFill() {
        AppPage page = page(control("bar", "bar"), control("chart", "piegrid"));
        for (String target : List.of("bar", "chart")) {
            AppPageAction action = new AppPageAction();
            action.setTargetControlId(target);
            assertThatCode(() -> AppCatalogService.validateActionTarget(page, action, "Action"))
                    .doesNotThrowAnyException();
        }
    }
}
