package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a trigger leaves the operator looking once it has finished — see
 * {@link AppPageControl#getFocusControlId()}.
 *
 * <p>The setting is worth checking at the save rather than at the click because the failure is
 * silent: a button that scrolls to a control that is not there, or to one that is never drawn,
 * looks exactly like a button that was never asked to scroll anywhere. Here it can be named.
 */
class AppPageFocusControlTest {

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
    void aGridOnThePage_isSomewhereToScrollTo() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("g1");
        AppPage page = page(button, control("g1", "grid"));
        assertThatCode(() -> AppCatalogService.validateFocusControl(page, button)).doesNotThrowAnyException();
    }

    @Test
    void namingNothing_leavesTheOperatorWhereTheyAre() {
        AppPageControl button = control("b1", "button");
        AppPage page = page(button, control("g1", "grid"));
        assertThatCode(() -> AppCatalogService.validateFocusControl(page, button)).doesNotThrowAnyException();
        button.setFocusControlId("   ");
        assertThatCode(() -> AppCatalogService.validateFocusControl(page, button)).doesNotThrowAnyException();
    }

    @Test
    void aControlThatIsNotOnThePage_isRefusedRatherThanSavedAsAButtonThatScrollsNowhere() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("gone");
        AppPage page = page(button, control("g1", "grid"));
        assertThatThrownBy(() -> AppCatalogService.validateFocusControl(page, button))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gone");
    }

    @Test
    void scrollingToItself_isWhereTheOperatorAlreadyIs() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("b1");
        AppPage page = page(button, control("g1", "grid"));
        assertThatThrownBy(() -> AppCatalogService.validateFocusControl(page, button))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aHiddenField_isNeverDrawn_soItIsNotSomewhereToLook() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("h1");
        AppPageControl hidden = control("h1", "hidden");
        hidden.setFieldName("orderId");
        AppPage page = page(button, hidden);
        assertThatThrownBy(() -> AppCatalogService.validateFocusControl(page, button))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hidden");
    }

    @Test
    void aGridHoldingItsRowsWithoutBeingDrawn_isRefusedForTheSameReason() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("g1");
        AppPageControl grid = control("g1", "grid");
        grid.setHideOnRun(true);
        AppPage page = page(button, grid);
        assertThatThrownBy(() -> AppCatalogService.validateFocusControl(page, button))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aGridInsideATabSet_isFine_sinceTheRunningPageOpensTheTabOnItsWayThere() {
        AppPageControl button = control("b1", "button");
        button.setFocusControlId("g1");
        AppPageControl tabs = control("t1", "tabs");
        tabs.setTabControlIds(List.of("g1"));
        AppPage page = page(button, tabs, control("g1", "grid"));
        assertThatCode(() -> AppCatalogService.validateFocusControl(page, button)).doesNotThrowAnyException();
    }
}
