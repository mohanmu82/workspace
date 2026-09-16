package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a tab set may hold. A chart is the same kind of thing as a grid as far as a tab is concerned —
 * one reading of one call, wanting the full width and a few rows of height — so a page answering a
 * question with a grid, a pie of it and a bar chart beside them can be three tabs rather than three
 * controls down a screen nobody can see the bottom of.
 */
class AppPageTabChildrenTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        return control;
    }

    private static AppPageControl tabs(String id, String... childIds) {
        AppPageControl tabs = control(id, "tabs");
        tabs.setTabControlIds(List.of(childIds));
        return tabs;
    }

    private static AppPage page(AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setControls(List.of(controls));
        return page;
    }

    @Test
    void aTabSetOfGrids_isWhatItAlwaysWas() {
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(tabs("t", "g1", "g2"), control("g1", "grid"), control("g2", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aGridAPieAndABarChartInOneSet_isThePointOfThis() {
        AppPageControl pie = control("p", "pie");
        AppPageControl bar = control("b", "bar");
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(tabs("t", "g", "p", "b"), control("g", "grid"), pie, bar)))
                .doesNotThrowAnyException();
    }

    @Test
    void aPieWithGridsMayBeATabOfAnotherSetThanTheOneItFills() {
        AppPageControl chart = control("chart", "piegrid");
        chart.setTabsControlId("results");
        AppPage page = page(tabs("charts", "chart"), tabs("results"), chart);
        assertThatCode(() -> {
            AppCatalogService.validateTabs(page);
            AppCatalogService.validateChartControls(page);
        }).doesNotThrowAnyException();
    }

    @Test
    void aPieWithGridsInsideTheSetItFills_isRefused() {
        // It would add a tab per pie beside itself on every run, and the operator would lose sight of
        // the chart to look at what it had just produced.
        AppPageControl chart = control("chart", "piegrid");
        chart.setTabsControlId("t");
        assertThatThrownBy(() -> AppCatalogService.validateChartControls(page(tabs("t", "chart"), chart)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tab set it is itself a tab of");
    }

    @Test
    void aTabSetInsideATabSet_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("outer", "inner"), tabs("inner"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a tab set holds grids and charts");
    }

    @Test
    void aTextBoxAsATab_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("t", "box"), control("box", "text"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a tab set holds grids and charts");
    }

    @Test
    void aTabNamingSomethingThatIsNotOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("t", "gone"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void oneChartClaimedByTwoTabSets_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(tabs("t1", "p"), tabs("t2", "p"), control("p", "pie"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one tab set");
    }

    @Test
    void aSetOpeningOnAChartItHolds_isFine() {
        AppPageControl set = tabs("t", "g", "p");
        set.setDefaultTabControlId("p");
        assertThatCode(() -> AppCatalogService.validateTabs(page(set, control("g", "grid"), control("p", "pie"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aSetOpeningOnATabItDoesNotHold_isRefused() {
        AppPageControl set = tabs("t", "g");
        set.setDefaultTabControlId("p");
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(set, control("g", "grid"), control("p", "pie"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("opens on a tab it does not hold");
    }
}
