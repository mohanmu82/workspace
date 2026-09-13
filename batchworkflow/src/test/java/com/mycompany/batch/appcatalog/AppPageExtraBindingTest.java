package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One call bound into several controls: what an action's further targets have to name for the page
 * to be storable, and where they are refused because there is no single response to read twice.
 */
class AppPageExtraBindingTest {

    private static final String WHERE = "Action 'Orders'";
    private static final List<String> TRANSFORMS = List.of("flatten");

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type) || "textarea".equals(type) || "select".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("grid", "grid"), control("pick", "select"), control("pie", "pie"),
                                 control("notes", "textarea"), control("tabs", "tabs"), control("go", "button")));
        return page;
    }

    private static AppPageBinding binding(String target, String... transforms) {
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId(target);
        binding.setTransformNames(List.of(transforms));
        return binding;
    }

    private static AppPageAction action(AppPageBinding... bindings) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-orders");
        action.setActionLabel("Orders");
        action.setAppUseCaseInstanceId("i-1");
        action.setTargetControlId("grid");
        action.setExtraBindings(List.of(bindings));
        return action;
    }

    @Test
    void oneCallIntoASelectAPieAndATextArea_isFine() {
        AppPageAction action = action(binding("pick"), binding("pie", "flatten"), binding("notes"),
                                      binding(AppPageAction.NEW_GRID));
        assertThatCode(() -> AppCatalogService.validateExtraBindings(page(), action, TRANSFORMS, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void noFurtherTargets_isTheOrdinaryCase() {
        assertThatCode(() -> AppCatalogService.validateExtraBindings(page(), action(), TRANSFORMS, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aTargetWithNoControl_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action(binding(" ")), TRANSFORMS, WHERE))
                .hasMessageContaining("target 2 has no control chosen");
    }

    @Test
    void aTargetNotOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action(binding("gone")), TRANSFORMS, WHERE))
                .hasMessageContaining("not on this page");
    }

    @Test
    void aTargetThatCannotHoldAnAnswer_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action(binding("go")), TRANSFORMS, WHERE))
                .hasMessageContaining("not a button");
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action(binding("tabs")), TRANSFORMS, WHERE))
                .hasMessageContaining("tab set");
    }

    @Test
    void aTransformThePageDoesNotHave_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action(binding("pie", "missing")), TRANSFORMS, WHERE))
                .hasMessageContaining("target 2 applies a transform that is not on this page: missing");
    }

    @Test
    void aFanOut_takesNoFurtherTargets() {
        AppPageAction action = action(binding("pick"));
        action.setRowSourceControlId("grid");
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action, TRANSFORMS, WHERE))
                .hasMessageContaining("runs once per row");
    }

    @Test
    void aPerformanceSummary_takesNoFurtherTargets() {
        AppPageAction action = action(binding("pick"));
        action.setActionKind(AppPageAction.PERFORMANCE);
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action, TRANSFORMS, WHERE))
                .hasMessageContaining("summarises performance");
    }
}
