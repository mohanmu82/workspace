package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a compare action has to be for the page to be storable.
 *
 * <p>Nearly all of it guards one failure, and it is a quiet one: a comparison that runs, succeeds
 * and reports no differences because it never compared two things. One environment left blank, or
 * both spelled the same, produces exactly that — a green table with nothing in it, which is also
 * what a genuinely clean comparison produces, so nobody looking at it would think to check. The
 * catalog refuses it at the point where the difference is still visible.
 *
 * <p>The rest is where the report lands. It is a table of paths and values, so it goes in a grid,
 * and an action with nowhere to put it has read two responses to no purpose whatever.
 */
class AppPageCompareActionTest {

    private static final String WHERE = "Action 'Drift check'";

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

    private static AppPageAction compareAction(String envA, String envB, String targetControlId) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-cmp");
        action.setActionLabel("Drift check");
        action.setActionKind(AppPageAction.COMPARE);
        action.setAppUseCaseInstanceId("i-1");
        action.setCompareEnvironmentA(envA);
        action.setCompareEnvironmentB(envB);
        action.setTargetControlId(targetControlId);
        return action;
    }

    // ── The kind itself ──────────────────────────────────────────────────────

    @Test
    void theKindSurvivesBeingSet_andIsNotPerformance() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.COMPARE);
        assertThat(action.isCompare()).isTrue();
        assertThat(action.isPerformance()).isFalse();
    }

    @Test
    void anUnknownKind_stillReadsAsAnOrdinaryAction() {
        AppPageAction action = new AppPageAction();
        action.setActionKind("DIFFING");
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.USECASE);
        assertThat(action.isCompare()).isFalse();
    }

    // ── What each side is read as ────────────────────────────────────────────

    @Test
    void theTypeDefaultsToJson_andOnlyXmlChangesIt() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        assertThat(action.getCompareType()).isEqualTo(AppPageAction.COMPARE_JSON);
        action.setCompareType("xml");
        assertThat(action.getCompareType()).isEqualTo(AppPageAction.COMPARE_XML);
        assertThat(action.isCompareXml()).isTrue();
        action.setCompareType("yaml");
        assertThat(action.getCompareType()).isEqualTo(AppPageAction.COMPARE_JSON);
    }

    /** A threshold is a tolerance, and one below zero would tolerate less than none of it. */
    @Test
    void aNegativeThreshold_readsAsNoThreshold() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        action.setCompareTolerancePercent(-5);
        assertThat(action.getCompareTolerancePercent()).isZero();
        action.setCompareTolerancePercent(0.1);
        assertThat(action.getCompareTolerancePercent()).isEqualTo(0.1);
    }

    @Test
    void theReportDefaultsToDifferencesAlone() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        assertThat(action.getCompareReport()).isEqualTo(AppPageAction.REPORT_DIFFERENCES);
        action.setCompareReport("everything");
        assertThat(action.getCompareReport()).isEqualTo(AppPageAction.REPORT_EVERYTHING);
        action.setCompareReport("some of it");
        assertThat(action.getCompareReport()).isEqualTo(AppPageAction.REPORT_DIFFERENCES);
    }

    // ── The two environments ─────────────────────────────────────────────────

    @Test
    void twoEnvironmentsAndAGrid_isFine() {
        assertThatCode(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "UAT", "g"), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aBlankSide_isRefused_sinceItWouldCompareAnEnvironmentWithItself() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "", "g"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Drift check")
                .hasMessageContaining("only names one")
                .hasMessageContaining("report no differences");
    }

    @Test
    void neitherSideNamed_saysSo() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction(null, null, "g"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only names neither");
    }

    @Test
    void oneEnvironmentNamedTwice_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("UAT", "UAT", "g"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("compares 'UAT' against itself");
    }

    /**
     * Two templates spelled the same are not refused. What they come to is a question about the
     * operator's picks at run time, and the running page answers it there; refusing the page would
     * be the catalog guessing about values it cannot see.
     */
    @Test
    void twoPlaceholdersSpelledAlike_areLeftToTheRunningPage() {
        assertThatCode(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("${env}", "${env}", "g"), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aThresholdOverAHundredPercent_isRefusedAsAMisreadUnit() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        action.setCompareTolerancePercent(1000);
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(page(), action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0.1 means");
    }

    // ── Where the report lands ───────────────────────────────────────────────

    @Test
    void anActionStackingANewGridPerRun_isFine() {
        assertThatCode(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "UAT", AppPageAction.NEW_GRID), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void noTarget_isRefused_sinceTheComparisonWouldGoNowhere() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "UAT", ""), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nowhere to put the differences");
    }

    @Test
    void aTextBoxTarget_isRefused_sinceAReportIsATable() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "UAT", "box"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a grid")
                .hasMessageContaining("text");
    }

    @Test
    void aTargetThatIsNoLongerOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(
                page(), compareAction("DEV", "UAT", "gone"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    // ── What it cannot also be ───────────────────────────────────────────────

    @Test
    void fanningOutOverAGrid_isRefused_sinceThereIsOneTablePerRun() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        action.setRowSourceControlId("g");
        assertThatThrownBy(() -> AppCatalogService.validateCompareAction(page(), action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("for each row of");
    }

    @Test
    void furtherTargets_areRefused_sinceTheReportFillsOneGrid() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId("box");
        action.setExtraBindings(List.of(binding));
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(
                page(), action, List.of(), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remove its other targets");
    }

    @Test
    void enrichedColumns_areRefused_sinceThereAreTwoCallsBehindEveryRow() {
        AppPageAction action = compareAction("DEV", "UAT", "g");
        action.setEnrichColumns(List.of(new AppPageEnrichColumn(
                "code", AppPageEnrichColumn.META, "statusCode", null, null, null, null)));
        assertThatThrownBy(() -> AppCatalogService.validateEnrichColumns(
                page(), action, name -> true, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two calls behind every row");
    }
}
