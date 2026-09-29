package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The filters an action narrows what it binds with — see {@link AppPageAction#getBindFilters()}.
 *
 * <p>A grid and a dropdown both take a list and neither always wants all of it: the endpoint that
 * answers with every order is the grid of the open ones, and the instance list that answers with
 * every environment is the dropdown of the production ones. So the question these tests answer is
 * where such a filter is allowed to be written — anything that takes a list of rows — and where it is
 * refused rather than saved as a test nothing would ever make.
 */
class AppPageBindFilterTest {

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

    private static AppPageAction aimedAt(String targetControlId) {
        AppPageAction action = new AppPageAction();
        action.setTargetControlId(targetControlId);
        action.setBindFilters(List.of(new AppPageRowFilter("status", "EQUALS", "OPEN")));
        return action;
    }

    @Test
    void filteringWhatFillsAGrid_isThePoint() {
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("g", "grid")), aimedAt("g"), "Action"))
                .doesNotThrowAnyException();
    }

    @Test
    void filteringWhatFillsADropdown_isTheOtherHalfOfThePoint() {
        // And the half that had no workaround: a grid at least has a filter row the operator can
        // type into, and a dropdown has nothing of the kind.
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("s", "select")), aimedAt("s"), "Action"))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("m", "multiselect")), aimedAt("m"), "Action"))
                .doesNotThrowAnyException();
    }

    @Test
    void filteringWhatFillsANewGrid_isFine() {
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(), aimedAt(AppPageAction.NEW_GRID), "Action"))
                .doesNotThrowAnyException();
    }

    @Test
    void aFilterOnATextBox_isRefused() {
        // A box takes one value; there are no rows in it for a test to keep or drop.
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(
                page(control("box", "textarea")), aimedAt("box"), "Action"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid or a dropdown takes a list of");
    }

    @Test
    void aFilterOnAChart_isRefused() {
        // A chart filters its own rows under its own settings, which the operator can change while
        // the page runs — see AppPageControl#chartFilters.
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(
                page(control("p", "pie")), aimedAt("p"), "Action"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid or a dropdown takes a list of");
    }

    @Test
    void aFilterWithNowhereToBind_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(page(), aimedAt(""), "Action"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nowhere to bind it");
    }

    @Test
    void aFilterNamingNoColumnOrAnUnknownTest_isRefused() {
        AppPageAction action = aimedAt("g");
        action.setBindFilters(List.of(new AppPageRowFilter("  ", "EQUALS", "OPEN")));
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(page(control("g", "grid")), action, "Action"))
                .hasMessageContaining("names no column");
        action.setBindFilters(List.of(new AppPageRowFilter("status", "LIKE", "OPEN")));
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(page(control("g", "grid")), action, "Action"))
                .hasMessageContaining("unknown test");
    }

    @Test
    void aFanOutCollectingIntoOneGrid_mayFilterTheCollection() {
        AppPageAction action = aimedAt("out");
        action.setRowSourceControlId("in");
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("in", "grid"), control("out", "grid")), action, "Action"))
                .doesNotThrowAnyException();
    }

    @Test
    void aFanOutGivingEachRowATab_mayFilterEachTabsGrid() {
        AppPageAction action = aimedAt("tabs");
        action.setRowSourceControlId("in");
        action.setRowOutputMode("TABS");
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("in", "grid"), control("tabs", "tabs")), action, "Action"))
                .doesNotThrowAnyException();
    }

    @Test
    void aFurtherTargetIsHeldToTheSameRule() {
        AppPageAction action = new AppPageAction();
        action.setTargetControlId("g");
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId("box");
        binding.setBindFilters(List.of(new AppPageRowFilter("status", "EQUALS", "OPEN")));
        action.setExtraBindings(List.of(binding));
        assertThatThrownBy(() -> AppCatalogService.validateBindFilters(
                page(control("g", "grid"), control("box", "text")), action, "Action"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("target 2");
    }

    @Test
    void aFurtherTargetFillingASecondGrid_isFine() {
        AppPageAction action = new AppPageAction();
        action.setTargetControlId("g1");
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId("g2");
        binding.setBindFilters(List.of(new AppPageRowFilter("status", "NOT_EQUALS", "OPEN")));
        action.setExtraBindings(List.of(binding));
        assertThatCode(() -> AppCatalogService.validateBindFilters(
                page(control("g1", "grid"), control("g2", "grid")), action, "Action"))
                .doesNotThrowAnyException();
    }
}
