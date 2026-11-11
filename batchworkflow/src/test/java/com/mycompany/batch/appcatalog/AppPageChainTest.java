package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What a chain of page actions run per row has to name for the page to be storable. */
class AppPageChainTest {

    private static final String WHERE = "Action 'Per user'";

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        return control;
    }

    private static AppPageAction pageAction(String id) {
        AppPageAction action = new AppPageAction();
        action.setActionId(id);
        action.setAppUseCaseInstanceId("i-" + id);
        return action;
    }

    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("users", "grid"), control("user", "text"), control("out", "grid"),
                                 control("pie", "pie"), control("json", "textarea")));
        page.setActions(List.of(pageAction("getAttributes"), pageAction("getBasefilter")));
        return page;
    }

    private static AppPageAction chain(String source, String target, AppPageChainStep... steps) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-chain");
        action.setActionKind("chain");
        action.setRowSourceControlId(source);
        action.setTargetControlId(target);
        action.setChainSteps(List.of(steps));
        return action;
    }

    private static final AppPageChainStep ATTRS = new AppPageChainStep("getAttributes", 8);
    private static final AppPageChainStep FILTER = new AppPageChainStep("getBasefilter", 0);

    @Test
    void kindIsRead_andConcurrencyDefaults() {
        assertThat(chain("users", "out").isChain()).isTrue();
        assertThat(FILTER.maxConcurrency()).isEqualTo(AppPageChainStep.DEFAULT_CONCURRENCY);
    }

    @Test
    void aChainOverAGridOrOneTextBox_intoAGrid_isFine() {
        assertThatCode(() -> AppCatalogService.validateChainAction(page(), chain("users", "out", ATTRS, FILTER), WHERE))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateChainAction(page(), chain("user", AppPageAction.NEW_GRID, ATTRS), WHERE))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateChainAction(page(), chain("users", "json", ATTRS), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void noRowSource_noSteps_orAnUnknownStep_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(), chain(null, "out", ATTRS), WHERE))
                .hasMessageContaining("no rows to run them over");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(), chain("users", "out"), WHERE))
                .hasMessageContaining("add at least one step");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(),
                chain("users", "out", new AppPageChainStep("gone", 2)), WHERE))
                .hasMessageContaining("not on this page: gone");
    }

    @Test
    void aStepThatIsNotAnOrdinaryAction_orTooManyAtOnce_isRefused() {
        AppPage page = page();
        page.getActions().get(0).setRowSourceControlId("users");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page, chain("users", "out", ATTRS), WHERE))
                .hasMessageContaining("not an ordinary action run once");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(),
                chain("users", "out", new AppPageChainStep("getAttributes", 51)), WHERE))
                .hasMessageContaining("at most 50");
    }

    @Test
    void theTargetHasToBeAGridOtherThanTheSource() {
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(), chain("users", "pie", ATTRS), WHERE))
                .hasMessageContaining("must target a grid");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(), chain("users", "users", ATTRS), WHERE))
                .hasMessageContaining("same grid it fills");
        assertThatThrownBy(() -> AppCatalogService.validateChainAction(page(), chain("users", "", ATTRS), WHERE))
                .hasMessageContaining("nowhere to put the rows");
    }
}
