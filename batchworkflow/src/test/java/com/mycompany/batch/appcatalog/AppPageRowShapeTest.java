package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two things a fan-out may say about the rows it runs over: which of them to call for, and what
 * the grid it collects the answers into is made of.
 *
 * <p>Both are refused outright on an action that does not fan out, and that is the point of the
 * checks rather than an incidental strictness: a filter on an action that makes one call, or a set
 * of collected columns on one that has no collected grid, is wiring that looks alive in the designer
 * and could never once run.
 */
class AppPageRowShapeTest {

    private static final String WHERE = "Action 'Detail'";

    private static AppPageAction fanOut(String outputMode) {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-detail");
        action.setActionLabel("Detail");
        action.setAppUseCaseInstanceId("i-1");
        action.setRowSourceControlId("src");
        action.setRowOutputMode(outputMode);
        return action;
    }

    private static AppPageAction runsOnce() {
        AppPageAction action = new AppPageAction();
        action.setActionId("a-detail");
        action.setActionLabel("Detail");
        action.setAppUseCaseInstanceId("i-1");
        return action;
    }

    // ── Row filters ──────────────────────────────────────────────────────────

    @Test
    void noFilters_isTheOrdinaryCase() {
        assertThatCode(() -> AppCatalogService.validateRowFilters(fanOut(AppPageAction.ROWS), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void aFilterNamingAColumnAndATest_isFine() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowFilters(List.of(new AppPageRowFilter("status", "EQUALS", "${statusPick}")));
        assertThatCode(() -> AppCatalogService.validateRowFilters(action, WHERE)).doesNotThrowAnyException();
    }

    @Test
    void aFilterWithNoTestNamed_readsAsContains() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowFilters(List.of(new AppPageRowFilter("status", "", "FAILED")));
        assertThatCode(() -> AppCatalogService.validateRowFilters(action, WHERE)).doesNotThrowAnyException();
    }

    @Test
    void aFilterNamingNoColumn_isRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowFilters(List.of(new AppPageRowFilter(" ", "EQUALS", "FAILED")));
        assertThatThrownBy(() -> AppCatalogService.validateRowFilters(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names no column");
    }

    @Test
    void aFilterWithATestNothingUnderstands_isRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowFilters(List.of(new AppPageRowFilter("status", "SOUNDS_LIKE", "FAILED")));
        assertThatThrownBy(() -> AppCatalogService.validateRowFilters(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown test");
    }

    @Test
    void filtersOnAnActionThatRunsOnce_areRefused() {
        AppPageAction action = runsOnce();
        action.setRowFilters(List.of(new AppPageRowFilter("status", "EQUALS", "FAILED")));
        assertThatThrownBy(() -> AppCatalogService.validateRowFilters(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not run over rows");
    }

    // ── Collected columns ────────────────────────────────────────────────────

    @Test
    void noColumns_keepsTheOldShape() {
        assertThatCode(() -> AppCatalogService.validateRowColumns(fanOut(AppPageAction.ROWS), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void columnsFromEachOfTheFourPlaces_areFine() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(
                new AppPageResultColumn("Order",   AppPageResultColumn.ROW,    "orderId"),
                new AppPageResultColumn("Status",  AppPageResultColumn.PATH,   "data.order.status"),
                new AppPageResultColumn("Code",    AppPageResultColumn.META,   "statusCode"),
                new AppPageResultColumn("Type",    AppPageResultColumn.HEADER, "Content-Type"),
                new AppPageResultColumn("Call",    AppPageResultColumn.LABEL,  null)));
        assertThatCode(() -> AppCatalogService.validateRowColumns(action, WHERE)).doesNotThrowAnyException();
    }

    @Test
    void aColumnWithNoKindNamed_readsTheResponse() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(new AppPageResultColumn("Status", "", "data.status")));
        assertThatCode(() -> AppCatalogService.validateRowColumns(action, WHERE)).doesNotThrowAnyException();
    }

    @Test
    void aColumnWithNoName_isRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(new AppPageResultColumn(" ", AppPageResultColumn.META, "statusCode")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no name");
    }

    @Test
    void twoColumnsOfOneName_areRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(
                new AppPageResultColumn("Status", AppPageResultColumn.META, "statusCode"),
                new AppPageResultColumn("Status", AppPageResultColumn.PATH, "data.status")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two result columns called Status");
    }

    @Test
    void aColumnFromNowhereThisPageKnows_isRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(new AppPageResultColumn("Status", "GUESS", "statusCode")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no idea how to read");
    }

    @Test
    void aColumnSayingWhereButNotWhat_isRefused() {
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(new AppPageResultColumn("Status", AppPageResultColumn.META, "")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("but not what to read");
    }

    @Test
    void theLabelColumnNeedsNoExpression() {
        // There is one label per call, so there is nothing for an expression to pick between.
        AppPageAction action = fanOut(AppPageAction.ROWS);
        action.setRowColumns(List.of(new AppPageResultColumn("Call", AppPageResultColumn.LABEL, "")));
        assertThatCode(() -> AppCatalogService.validateRowColumns(action, WHERE)).doesNotThrowAnyException();
    }

    @Test
    void columnsOnAnActionThatRunsOnce_areRefused() {
        AppPageAction action = runsOnce();
        action.setRowColumns(List.of(new AppPageResultColumn("Code", AppPageResultColumn.META, "statusCode")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("makes one call");
    }

    @Test
    void columnsOnAFanOutGivingEachRowATab_areRefused() {
        // Every call fills a grid with its whole answer there, so there is no collected grid these
        // could lay out.
        AppPageAction action = fanOut(AppPageAction.TABS);
        action.setRowColumns(List.of(new AppPageResultColumn("Code", AppPageResultColumn.META, "statusCode")));
        assertThatThrownBy(() -> AppCatalogService.validateRowColumns(action, WHERE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no collected grid");
    }

    @Test
    void filtersOnAFanOutGivingEachRowATab_areStillFine() {
        // Which rows to call for is the same question whichever shape the answers take.
        AppPageAction action = fanOut(AppPageAction.TABS);
        action.setRowFilters(List.of(new AppPageRowFilter("status", "EQUALS", "FAILED")));
        assertThatCode(() -> AppCatalogService.validateRowFilters(action, WHERE)).doesNotThrowAnyException();
    }
}
