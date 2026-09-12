package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a page's own variables have to be for the page to be storable, and what a control may
 * therefore no longer be called.
 *
 * <p>All of it guards the same failure: a page on which <code>${name}</code> means two things, or
 * means nothing. A variable a template cannot spell is one no action could ever read; two of a name
 * make "which value is this" unanswerable; and a name the run computes for itself is one the stored
 * value would lose to, silently, every time the page ran.
 */
class AppPageVariableTest {

    private static AppPage page(AppPageVariable... variables) {
        AppPage page = new AppPage();
        page.setVariables(List.of(variables));
        return page;
    }

    private static AppPageControl valueControl(String fieldName) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-1");
        control.setType("text");
        control.setFieldName(fieldName);
        control.setLabel("Order");
        return control;
    }

    // ── The variables themselves ─────────────────────────────────────────────

    @Test
    void aNamedVariable_isFine() {
        assertThat(AppCatalogService.validateVariables(
                page(new AppPageVariable("runTag", "${DATESTAMP}-nightly", "what this run is called"))))
                .containsExactly("runTag");
    }

    @Test
    void aVariableWithNoValue_isStillStorable() {
        // Declared for an operator to see and a page to fill in later; unlike a transform with no
        // expression, there is nothing it could fail at.
        assertThatCode(() -> AppCatalogService.validateVariables(page(new AppPageVariable("batch", "", null))))
                .doesNotThrowAnyException();
    }

    @Test
    void aVariableWithNoName_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateVariables(page(new AppPageVariable("  ", "x", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no name");
    }

    @Test
    void aNameNoTemplateCouldSpell_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateVariables(page(new AppPageVariable("run tag", "x", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a name a template can spell");
    }

    @Test
    void aNameStartingWithADigit_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateVariables(page(new AppPageVariable("2ndRun", "x", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a name a template can spell");
    }

    @Test
    void twoVariablesOfOneName_areRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateVariables(
                page(new AppPageVariable("batch", "a", null), new AppPageVariable("batch", "b", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate page variable: batch");
    }

    @Test
    void redefiningOneTheRunWorksOutForItself_isRefused() {
        // The stored value would lose to the computed one every time the page ran, so the page says
        // so where it can be fixed rather than in a request body nobody reads.
        assertThatThrownBy(() -> AppCatalogService.validateVariables(page(new AppPageVariable("UUID", "x", null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("redefines one this page already has");
    }

    @Test
    void everyBuiltInIsRefusedByName() {
        for (String name : AppPageVariable.BUILT_IN) {
            assertThatThrownBy(() -> AppCatalogService.validateVariables(page(new AppPageVariable(name, "x", null))))
                    .as("redefining " + name)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ── And what a control may be called ─────────────────────────────────────

    @Test
    void aFieldNameNoVariableHolds_isFine() {
        assertThatCode(() -> AppCatalogService.checkFieldNameFree(
                valueControl("orderId"), List.of("runTag"), "Control 'Order'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aFieldNameAVariableHolds_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.checkFieldNameFree(
                valueControl("runTag"), List.of("runTag"), "Control 'Order'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field name of a page variable: runTag");
    }

    @Test
    void aFieldNameABuiltInHolds_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.checkFieldNameFree(
                valueControl("DATESTAMP"), List.of(), "Control 'Order'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("every page already has");
    }

    @Test
    void aBuiltInNameInAnotherCase_isStillRefused() {
        // ${machine} and ${MACHINE} are the same placeholder to nobody, but a control called
        // "machine" beside a built-in called MACHINE is exactly the confusion this is for.
        assertThatThrownBy(() -> AppCatalogService.checkFieldNameFree(
                valueControl("machine"), List.of(), "Control 'Order'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("every page already has");
    }

    @Test
    void aControlWithNoFieldNameAtAll_isLeftAlone() {
        // A button or a grid holds no value and needs no field name; the check that it has one when
        // it should belongs to the caller.
        assertThatCode(() -> AppCatalogService.checkFieldNameFree(
                valueControl(null), List.of("runTag"), "Control 'Order'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aLegalNameIsWhatATemplateCanSpell() {
        assertThat(AppPageVariable.isLegalName("runTag")).isTrue();
        assertThat(AppPageVariable.isLegalName("_run2")).isTrue();
        assertThat(AppPageVariable.isLegalName("run-tag")).isFalse();
        assertThat(AppPageVariable.isLegalName("")).isFalse();
        assertThat(AppPageVariable.isLegalName(null)).isFalse();
    }
}
