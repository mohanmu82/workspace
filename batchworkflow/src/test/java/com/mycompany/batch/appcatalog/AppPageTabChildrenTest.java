package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a tab set may hold: any control but a hidden field. Grids and charts are what tab sets were
 * built for — a page answering a question with a grid, a pie of it and a bar chart beside them is
 * three tabs rather than three controls down a screen nobody can see the bottom of — and the form
 * half of a page belongs in tabs just as often, so everything else is allowed in too.
 *
 * <p>Including another tab set, which is how a strip that has grown too long is grouped. The one
 * arrangement refused is a set that ends up holding itself, at whatever depth.
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
    void aTabSetInsideATabSet_isHowALongStripIsGrouped() {
        // Four environments' worth of grids under one "UAT" tab, rather than four names on the
        // outer strip competing with everything else on it.
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(tabs("outer", "uat", "prod"), tabs("uat", "g1", "g2"), tabs("prod", "g3"),
                     control("g1", "grid"), control("g2", "grid"), control("g3", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void groupsNestedSeveralDeep_areFine() {
        // Nothing caps the depth: what is refused is arriving back at the start, not going far.
        assertThatCode(() -> AppCatalogService.validateTabs(
                page(tabs("a", "b"), tabs("b", "c"), tabs("c", "d"), tabs("d", "g"), control("g", "grid"))))
                .doesNotThrowAnyException();
    }

    @Test
    void aTabSetHoldingItself_isRefused() {
        // It has no depth at which it stops being drawn, and there is no outermost tab to be looking at.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("t", "t"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ends up holding itself");
    }

    @Test
    void twoTabSetsHoldingEachOther_isRefused() {
        // Each holds the other once, so the "one set claims it" rule does not catch this on its own.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("a", "b"), tabs("b", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ends up holding itself");
    }

    @Test
    void aRingOfThreeTabSets_isRefusedToo() {
        // Followed down far enough, a ring arrives back at its start exactly as a pair does.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(
                page(tabs("a", "b"), tabs("b", "c"), tabs("c", "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ends up holding itself");
    }

    @Test
    void aHiddenFieldAsATab_isRefused() {
        // It is not on screen to be looked at, so the tab would be a name over nothing — and it has
        // to stay where the page lays it out to go on carrying its value.
        assertThatThrownBy(() -> AppCatalogService.validateTabs(page(tabs("t", "h"), control("h", "hidden"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("except a hidden field");
    }

    @Test
    void aFormOfFieldsAndAButtonAsTabs_isTheOtherHalfOfThis() {
        // The first tab is what to fill in, the rest are the readings of what came back.
        AppPage page = page(tabs("t", "box", "pick", "go", "note", "g"),
                control("box", "text"), control("pick", "select"), control("go", "button"),
                control("note", "label"), control("g", "grid"));
        assertThatCode(() -> AppCatalogService.validateTabs(page)).doesNotThrowAnyException();
    }

    @Test
    void aLineChartAsATab_isFine() {
        assertThatCode(() -> AppCatalogService.validateTabs(page(tabs("t", "l"), control("l", "line"))))
                .doesNotThrowAnyException();
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
