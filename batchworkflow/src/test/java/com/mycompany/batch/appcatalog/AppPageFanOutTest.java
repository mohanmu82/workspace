package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What an action running once per row of a grid has to name for the page to be storable: a grid that
 * is really there to read the rows from, and somewhere that can hold one answer per row to put them.
 *
 * <p>The failure all of it guards is a page that looks wired up and cannot do what it says: a
 * fan-out over a grid that is not on the page, one reading the grid it is about to overwrite, or one
 * pointing its per-row answers at a text box that could only ever show the last of them.
 */
class AppPageFanOutTest {

    private static final String WHERE = "Action 'Detail'";

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    /** A page with one grid to read rows from, one to write them to, a tab set and a text box. */
    private static AppPage page() {
        AppPage page = new AppPage();
        page.setControls(List.of(control("src", "grid"), control("out", "grid"),
                                 control("tabs", "tabs"), control("box", "text")));
        return page;
    }

    private static AppPageAction action(String rowSource, String outputMode, String target) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-detail");
        action.setActionLabel("Detail");
        action.setAppUseCaseInstanceId("i-1");
        action.setRowSourceControlId(rowSource);
        if (outputMode != null) action.setRowOutputMode(outputMode);
        action.setTargetControlId(target);
        return action;
    }

    // ── The row source ───────────────────────────────────────────────────────

    @Test
    void aFanOutOverAGridOnThePage_isFine() {
        AppPageAction action = action("src", AppPageAction.ROWS, "out");
        assertThatCode(() -> AppCatalogService.validateRowSource(page(), action, WHERE))
                .doesNotThrowAnyException();
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(), action, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void anActionNamingNoRowSource_isTheOrdinaryKindAndIsNotChecked() {
        AppPageAction action = action("", null, "box");
        assertThat(action.isRowFanOut()).isFalse();
        assertThatCode(() -> AppCatalogService.validateRowSource(page(), action, WHERE))
                .doesNotThrowAnyException();
        // And it may still fill a text box, which a fan-out may not.
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(), action, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aRowSourceThatIsNotOnThePage_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateRowSource(page(), action("gone", AppPageAction.ROWS, "out"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void aRowSourceThatIsNotAGrid_hasNoRowsToRunOver() {
        assertThatThrownBy(() -> AppCatalogService.validateRowSource(page(), action("box", AppPageAction.ROWS, "out"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid has rows");
    }

    @Test
    void readingTheRowsOutOfTheGridItFills_wouldRunOverWhateverTheLastRunLeft() {
        assertThatThrownBy(() -> AppCatalogService.validateRowSource(page(), action("out", AppPageAction.ROWS, "out"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same grid it fills");
    }

    // ── Where the answers land ───────────────────────────────────────────────

    @Test
    void aTabPerRow_targetsATabSet() {
        AppPageAction action = action("src", AppPageAction.TABS, "tabs");
        assertThat(action.isTabsPerRow()).isTrue();
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(), action, WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aTabPerRowAimedAtAGrid_hasNowhereToPutTheTabs() {
        assertThatThrownBy(() -> AppCatalogService.validateActionTarget(page(), action("src", AppPageAction.TABS, "out"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a tab set");
    }

    @Test
    void aRowPerCallAimedAtATextBox_couldOnlyEverShowTheLastOne() {
        assertThatThrownBy(() -> AppCatalogService.validateActionTarget(page(), action("src", AppPageAction.ROWS, "box"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must target a grid");
    }

    @Test
    void anOrdinaryActionAimedAtATabSet_isSentToOneOfItsGridsInstead() {
        assertThatThrownBy(() -> AppCatalogService.validateActionTarget(page(), action("", null, "tabs"), WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one of the grids inside");
    }

    @Test
    void aFanOutCollectingIntoANewGridPerRun_needsNoPlacedControl() {
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(),
                action("src", AppPageAction.ROWS, AppPageAction.NEW_GRID), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aFanOutWithNoTarget_runsForItsEffectAlone() {
        assertThatCode(() -> AppCatalogService.validateActionTarget(page(), action("src", AppPageAction.ROWS, ""), WHERE))
                .doesNotThrowAnyException();
    }

    // ── The fields themselves ────────────────────────────────────────────────

    @Test
    void theOutputModeIsOneOfTwo_andAnythingElseCollectsIntoOneGrid() {
        AppPageAction action = new AppPageAction();
        action.setRowOutputMode("tabs");
        assertThat(action.getRowOutputMode()).isEqualTo(AppPageAction.TABS);
        action.setRowOutputMode("something else");
        assertThat(action.getRowOutputMode()).isEqualTo(AppPageAction.ROWS);
        action.setRowOutputMode(null);
        assertThat(action.getRowOutputMode()).isEqualTo(AppPageAction.ROWS);
    }

    @Test
    void theRowCapIsOnByDefaultAndNeverNegative() {
        AppPageAction action = new AppPageAction();
        assertThat(action.getRowLimit()).isEqualTo(25);
        action.setRowLimit(-3);
        assertThat(action.getRowLimit()).isZero();
    }

    @Test
    void aBlankRowSourceIsNoRowSource_soAnUntouchedActionStillRunsOnce() {
        AppPageAction action = new AppPageAction();
        action.setRowSourceControlId("   ");
        assertThat(action.getRowSourceControlId()).isNull();
        assertThat(action.isRowFanOut()).isFalse();
        assertThat(action.isTabsPerRow()).isFalse();
    }
}
