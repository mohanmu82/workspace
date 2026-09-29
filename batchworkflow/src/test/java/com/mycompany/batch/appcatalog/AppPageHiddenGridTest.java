package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A grid that holds its rows without being drawn — see {@link AppPageControl#isHideOnRun()}.
 *
 * <p>Two things are worth refusing at the save rather than leaving to be noticed on a running page.
 * A control that is not a grid has no rows to keep out of sight, so the tick on one is a setting
 * nothing would ever read; and a grid that lives in a tab set is drawn by that set rather than by the
 * layout, so hiding it would leave a tab with nothing behind it.
 */
class AppPageHiddenGridTest {

    private static final String WHERE = "Control 'Reference'";

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    private static AppPage pageOf(AppPageControl... controls) {
        AppPage page = new AppPage();
        page.setControls(List.of(controls));
        return page;
    }

    @Test
    void aGridIsNotHiddenUntilItIsTicked() {
        assertThat(control("ref", "grid").isHideOnRun()).isFalse();
    }

    @Test
    void aHiddenGridOnTheLayout_isFine() {
        AppPageControl grid = control("ref", "grid");
        grid.setHideOnRun(true);
        assertThatCode(() -> AppCatalogService.validateHideOnRun(pageOf(grid), grid, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aGridLeftUntickedIsNotChecked_whateverElseIsTrueOfIt() {
        AppPageControl grid = control("ref", "grid");
        AppPageControl tabs = control("tabs", "tabs");
        tabs.setTabControlIds(List.of("ref"));
        assertThatCode(() -> AppCatalogService.validateHideOnRun(pageOf(grid, tabs), grid, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aHiddenControlThatIsNotAGrid_isRefused() {
        AppPageControl chart = control("pie", "pie");
        chart.setHideOnRun(true);
        assertThatThrownBy(() -> AppCatalogService.validateHideOnRun(pageOf(chart), chart, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid can hold its rows without being drawn");
    }

    /** A box the operator fills hides by being a hidden field, which is a control type of its own. */
    @Test
    void aHiddenValueControl_isPointedAtTheHiddenFieldType() {
        AppPageControl box = control("box", "text");
        box.setHideOnRun(true);
        assertThatThrownBy(() -> AppCatalogService.validateHideOnRun(pageOf(box), box, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hidden field");
    }

    @Test
    void aHiddenGridInsideATabSet_wouldLeaveATabWithNothingBehindIt() {
        AppPageControl grid = control("ref", "grid");
        grid.setHideOnRun(true);
        AppPageControl tabs = control("tabs", "tabs");
        tabs.setTabControlIds(List.of("ref"));
        assertThatThrownBy(() -> AppCatalogService.validateHideOnRun(pageOf(grid, tabs), grid, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Take it out of the tab set");
    }

    /**
     * Hiding a grid takes nothing else away from it: the dataset it opens holding, the rows it is
     * judged on, the variable that verdict is published under — all of them are still its own, and
     * all of them still have to pass the checks they always did.
     */
    @Test
    void aHiddenGridKeepsEverythingElseAGridHas() {
        AppPageControl grid = control("ref", "grid");
        grid.setHideOnRun(true);
        grid.setRowErrorExpression("STATUS != SUCCESS");
        grid.setDisplayFilterExpression("STATUS != SKIPPED");
        grid.setStatusCondition("ROWCOUNT>0");
        grid.setStatusVariable("refStatus");
        assertThatCode(() -> {
            AppCatalogService.validateRowErrorExpression(grid, WHERE);
            AppCatalogService.validateDisplayFilterExpression(grid, WHERE);
            AppCatalogService.validateGridStatus(grid, List.of(), WHERE);
            AppCatalogService.validateHideOnRun(pageOf(grid), grid, WHERE);
        }).doesNotThrowAnyException();
    }
}
