package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a dataset action has to be for the page to be storable.
 *
 * <p>Like a performance summary it names no endpoint, sends nothing and reads no response, so it
 * answers to almost none of the checks an ordinary action does. What it does have to name is the
 * dataset it reads — a name the library has not got is an action that would fetch nothing — and a
 * control its rows can go in.
 *
 * <p>Where it parts company with the other special kinds is the range of that last one. A summary
 * and a comparison each produce a table of their own fixed shape, so a grid is the only thing that
 * can hold one. A dataset's rows are ordinary rows, and a dropdown or a chart takes a list of rows
 * as readily as a grid does.
 */
class AppPageDatasetActionTest {

    /** The library holds one dataset, called "desks", and nothing else. */
    private static final Predicate<String> LIBRARY = "desks"::equals;

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    /** One of each thing a target could name: a grid, a dropdown, a chart, a box and a link. */
    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("g", "grid"), control("pick", "select"),
                control("chart", "pie"), control("box", "text"), control("link", "link")));
        return page;
    }

    private static AppPageAction datasetAction(String targetControlId) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-ds");
        action.setActionLabel("Load the desks");
        action.setActionKind(AppPageAction.DATASET);
        action.setDatasetName("desks");
        action.setTargetControlId(targetControlId);
        return action;
    }

    private static void validate(AppPage page, AppPageAction action) {
        AppCatalogService.validateDatasetAction(page, action, LIBRARY, "Action 'Load the desks'");
    }

    // ── Where the rows may go ────────────────────────────────────────────

    @Test
    void aGridTarget_isFine() {
        assertThatCode(() -> validate(page(), datasetAction("g"))).doesNotThrowAnyException();
    }

    @Test
    void aNewGridPerRun_isFine() {
        assertThatCode(() -> validate(page(), datasetAction(AppPageAction.NEW_GRID))).doesNotThrowAnyException();
    }

    @Test
    void aDropdownTarget_isFine_sinceRowsMakeOptionsAsReadilyAsTheyMakeATable() {
        assertThatCode(() -> validate(page(), datasetAction("pick"))).doesNotThrowAnyException();
    }

    @Test
    void aChartTarget_isFine_forTheSameReason() {
        assertThatCode(() -> validate(page(), datasetAction("chart"))).doesNotThrowAnyException();
    }

    @Test
    void aTextBoxTarget_isRefused_sinceThereIsNoResponseToReadOneValueOutOf() {
        assertThatThrownBy(() -> validate(page(), datasetAction("box")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Load the desks")
                .hasMessageContaining("must target a grid, a select, a multi-select or a chart")
                .hasMessageContaining("text");
    }

    @Test
    void aLinkTarget_isRefusedForTheSameReason() {
        assertThatThrownBy(() -> validate(page(), datasetAction("link")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a grid, a select, a multi-select or a chart");
    }

    @Test
    void noTargetAtAll_isRefused_sinceThereIsNoCallToRunItFor() {
        // An ordinary action with no target is still worth running for the call it makes. This one
        // makes none, so a targetless dataset action is an action that would do nothing whatever.
        assertThatThrownBy(() -> validate(page(), datasetAction("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nowhere to put it");
    }

    @Test
    void aTargetThatIsNoLongerOnThePage_isRefused() {
        assertThatThrownBy(() -> validate(page(), datasetAction("deleted")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    // ── Which dataset ────────────────────────────────────────────────────

    @Test
    void namingNoDataset_isRefused() {
        AppPageAction action = datasetAction("g");
        action.setDatasetName("");
        assertThatThrownBy(() -> validate(page(), action))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names no static dataset");
    }

    @Test
    void namingADatasetTheLibraryHasNotGot_isRefused_ratherThanFetchingNothingAtRunTime() {
        AppPageAction action = datasetAction("g");
        action.setDatasetName("deleted-last-week");
        assertThatThrownBy(() -> validate(page(), action))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown static dataset")
                .hasMessageContaining("deleted-last-week");
    }

    // ── How it narrows ───────────────────────────────────────────────────

    @Test
    void conditionsNamingAColumnAndAKnownTest_areFine() {
        AppPageAction action = datasetAction("g");
        action.setDatasetFilters(List.of(new AppPageDatasetFilter("region", "equals", "${regionPick}"),
                                         new AppPageDatasetFilter("desk", "contains", "FX")));
        assertThatCode(() -> validate(page(), action)).doesNotThrowAnyException();
    }

    @Test
    void aConditionWithNoOperator_readsAsEquals_whichIsWhatTypingAValueBesideAColumnMeans() {
        assertThat(new AppPageDatasetFilter("region", null, "EMEA").opOrDefault()).isEqualTo("equals");
        assertThat(new AppPageDatasetFilter("region", "  ", "EMEA").opOrDefault()).isEqualTo("equals");

        AppPageAction action = datasetAction("g");
        action.setDatasetFilters(List.of(new AppPageDatasetFilter("region", null, "EMEA")));
        assertThatCode(() -> validate(page(), action)).doesNotThrowAnyException();
    }

    @Test
    void aConditionNamingNoColumn_isRefused_sinceItWouldNarrowNothing() {
        AppPageAction action = datasetAction("g");
        action.setDatasetFilters(List.of(new AppPageDatasetFilter("", "equals", "EMEA")));
        assertThatThrownBy(() -> validate(page(), action))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("filter 1")
                .hasMessageContaining("names no column");
    }

    @Test
    void aTestTheDatasetLibraryDoesNotKnow_isRefusedHere_ratherThanAtTheFarEnd() {
        AppPageAction action = datasetAction("g");
        action.setDatasetFilters(List.of(new AppPageDatasetFilter("region", "matchesRegex", ".*")));
        assertThatThrownBy(() -> validate(page(), action))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("matchesRegex")
                .hasMessageContaining("equals");
    }

    @Test
    void anEmptyValue_isNotRefused_sinceItIsRoutinelyATemplateNobodyHasFilledInYet() {
        AppPageAction action = datasetAction("g");
        action.setDatasetFilters(List.of(new AppPageDatasetFilter("region", "equals", "")));
        assertThatCode(() -> validate(page(), action)).doesNotThrowAnyException();
    }

    // ── Fanning out ──────────────────────────────────────────────────────

    @Test
    void fanningOutOverAGridsRows_isRefused_sinceEveryQueryWouldAskTheSameThing() {
        AppPageAction action = datasetAction("g");
        action.setRowSourceControlId("g");
        assertThatThrownBy(() -> validate(page(), action))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identical list");
    }

    // ── The kind itself ──────────────────────────────────────────────────

    @Test
    void theKindSurvivesARoundTrip_andIsRecognisedWhateverItsCase() {
        AppPageAction action = new AppPageAction();
        action.setActionKind("dataset");
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.DATASET);
        assertThat(action.isDataset()).isTrue();
        assertThat(action.isPerformance()).isFalse();
        assertThat(action.isCompare()).isFalse();
    }

    @Test
    void anActionSavedBeforeThisExisted_isAnOrdinaryOne_andCarriesNoDatasetWiring() {
        AppPageAction action = new AppPageAction();
        assertThat(action.getActionKind()).isEqualTo(AppPageAction.USECASE);
        assertThat(action.isDataset()).isFalse();
        assertThat(action.getDatasetName()).isNull();
        assertThat(action.getDatasetFavorite()).isNull();
        assertThat(action.getDatasetFilters()).isEmpty();
    }

    @Test
    void aBlankDatasetNameOrFavourite_isHeldAsNothingAtAll_soTheChecksAboveHaveOneAbsenceToTest() {
        AppPageAction action = new AppPageAction();
        action.setDatasetName("  ");
        action.setDatasetFavorite("  ");
        assertThat(action.getDatasetName()).isNull();
        assertThat(action.getDatasetFavorite()).isNull();

        action.setDatasetName("  desks  ");
        assertThat(action.getDatasetName()).isEqualTo("desks");
    }

    @Test
    void nullConditions_areHeldAsAnEmptyList_ratherThanBlowingUpWhateverReadsThem() {
        AppPageAction action = new AppPageAction();
        action.setDatasetFilters(null);
        assertThat(action.getDatasetFilters()).isEmpty();
    }

    // ── What it may not also carry ───────────────────────────────────────

    @Test
    void furtherTargets_areRefused_sinceOneListFillsOneControl() {
        AppPageAction action = datasetAction("g");
        AppPageBinding binding = new AppPageBinding();
        binding.setTargetControlId("pick");
        action.setExtraBindings(List.of(binding));
        assertThatThrownBy(() -> AppCatalogService.validateExtraBindings(page(), action, List.of(), "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remove its other targets");
    }

    @Test
    void anEnrichedColumnLookingIntoAnotherDataset_isFine_sinceThatIsAQuestionAboutTheRows() {
        AppPageAction action = datasetAction("g");
        action.setEnrichColumns(List.of(new AppPageEnrichColumn(
                "owner", AppPageEnrichColumn.VLOOKUP, null, "desks", "desk", "desk", "owner")));
        assertThatCode(() -> AppCatalogService.validateEnrichColumns(page(), action, LIBRARY, "Action 'x'"))
                .doesNotThrowAnyException();
    }

    @Test
    void anEnrichedColumnReadingTheCallRecord_isRefused_sinceThereIsNoCall() {
        AppPageAction action = datasetAction("g");
        action.setEnrichColumns(List.of(new AppPageEnrichColumn(
                "status", AppPageEnrichColumn.META, "statusCode", null, null, null, null)));
        assertThatThrownBy(() -> AppCatalogService.validateEnrichColumns(page(), action, LIBRARY, "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("makes no call")
                .hasMessageContaining("call record");
    }

    @Test
    void anEnrichedColumnReadingAResponseHeader_isRefusedForTheSameReason() {
        AppPageAction action = datasetAction("g");
        action.setEnrichColumns(List.of(new AppPageEnrichColumn(
                "corr", AppPageEnrichColumn.HEADER, "X-Correlation-Id", null, null, null, null)));
        assertThatThrownBy(() -> AppCatalogService.validateEnrichColumns(page(), action, LIBRARY, "Action 'x'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("response header");
    }
}
