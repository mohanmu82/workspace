package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a performance action has to be for the page to be storable.
 *
 * <p>It answers to almost none of the checks an ordinary action does, because it does almost none of
 * what an ordinary action does — it names no endpoint, sends nothing and reads no response. What is
 * left is where its rows go, and the rules there are strict for one reason: the summary is a table,
 * and an action that ran and had nowhere to put a table would have done nothing at all while
 * reporting success.
 */
class AppPagePerformanceActionTest {

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    /** A page carrying one grid, one text box and one chart — the three a target could name. */
    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("g", "grid"), control("box", "text"), control("chart", "pie")));
        return page;
    }

    private static AppPageAction performanceAction(String targetControlId) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-perf");
        action.setActionLabel("How are we doing");
        action.setActionKind(AppPageAction.PERFORMANCE);
        action.setTargetControlId(targetControlId);
        return action;
    }

    @Test
    void anActionAimedAtAGrid_isFine() {
        assertThatCode(() -> AppCatalogService.validatePerformanceAction(page(), performanceAction("g"), "Action 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void anActionStackingANewGridPerRun_isFine() {
        assertThatCode(() -> AppCatalogService.validatePerformanceAction(
                page(), performanceAction(AppPageAction.NEW_GRID), "Action 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aTextBoxTarget_isRefused_sinceASummaryIsATable() {
        assertThatThrownBy(() -> AppCatalogService.validatePerformanceAction(
                page(), performanceAction("box"), "Action 'How are we doing'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("How are we doing")
                .hasMessageContaining("must target a grid")
                .hasMessageContaining("text");
    }

    @Test
    void aChartTarget_isRefusedForTheSameReason() {
        assertThatThrownBy(() -> AppCatalogService.validatePerformanceAction(
                page(), performanceAction("chart"), "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a grid");
    }

    @Test
    void noTargetAtAll_isRefused_sinceThereIsNoCallToRunItFor() {
        // An ordinary action with no target is still worth running for the call it makes. This one
        // makes none, so a targetless performance action is an action that would do nothing whatever.
        assertThatThrownBy(() -> AppCatalogService.validatePerformanceAction(
                page(), performanceAction(""), "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nowhere to put it");
    }

    @Test
    void aTargetThatIsNoLongerOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validatePerformanceAction(
                page(), performanceAction("deleted"), "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void fanningOutOverAGridsRows_isRefused_sinceEveryCallWouldSummariseTheSameThing() {
        AppPageAction action = performanceAction("g");
        action.setRowSourceControlId("g");
        assertThatThrownBy(() -> AppCatalogService.validatePerformanceAction(page(), action, "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same summary");
    }

    @Test
    void anActionWithNoKindSet_isAnOrdinaryOne_soPagesSavedBeforeThisKeepWorking() {
        AppPageAction action = new AppPageAction();
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.USECASE);
        assertThat(action.isPerformance()).isFalse();
    }

    @Test
    void anUnrecognisedKind_readsAsAnOrdinaryAction_ratherThanAsSomethingUnrunnable() {
        AppPageAction action = new AppPageAction();
        action.setActionKind("SOMETHING_ELSE");
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.USECASE);
    }
}
