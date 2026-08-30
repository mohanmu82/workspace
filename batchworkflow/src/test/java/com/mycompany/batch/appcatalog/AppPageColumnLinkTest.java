package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a grid's clickable columns have to be for the page to be storable: only a grid has them, each
 * column is named once, each does something, and what each writes into and runs is really on the
 * page. The failure all of it guards is the same one — a column drawn as clickable that answers a
 * click with nothing.
 */
class AppPageColumnLinkTest {

    private static final List<String> LIBRARY = List.of("a-load-lines", "a-load-customer");

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    private static AppPageAssignment sets(String targetControlId, String value) {
        AppPageAssignment assignment = new AppPageAssignment();
        assignment.setTargetControlId(targetControlId);
        assignment.setValue(value);
        return assignment;
    }

    private static AppPageColumnLink link(String column, List<AppPageAssignment> assignments, List<String> actionIds) {
        AppPageColumnLink link = new AppPageColumnLink();
        link.setColumn(column);
        link.setAssignments(assignments);
        link.setActionIds(actionIds);
        return link;
    }

    /** A page holding one grid and one text box, with the grid's clickable columns as given. */
    private static AppPage pageWith(AppPageControl grid, List<AppPageColumnLink> links) {
        grid.setColumnLinks(links);
        AppPage page = new AppPage();
        page.setControls(List.of(grid, control("box", "text")));
        return page;
    }

    @Test
    void aColumnThatSetsAValueAndRunsAnAction_isFine() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(
                link("orderId", List.of(sets("box", "${orderId}")), List.of("a-load-lines"))));
        assertThatCode(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void twoColumnsDrillingDownDifferently_isThePointOfConfiguringThemPerColumn() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(
                link("orderId",    List.of(sets("box", "")), List.of("a-load-lines")),
                link("customerId", List.of(sets("box", "")), List.of("a-load-customer"))));
        assertThatCode(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void settingAValueWithoutRunningAnything_isACompleteJob() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(link("orderId", List.of(sets("box", "")), List.of())));
        assertThatCode(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void aColumnThatNeitherSetsNorRuns_isRefusedRatherThanDrawnAsALiveCell() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(link("orderId", List.of(), List.of())));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orderId")
                .hasMessageContaining("would do nothing");
    }

    @Test
    void anUnnamedColumn_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(link("  ", List.of(sets("box", "")), List.of())));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column name");
    }

    @Test
    void theSameColumnTwice_isRefusedSinceOnlyOneOfThemCouldRun() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(
                link("orderId", List.of(sets("box", "")), List.of()),
                link("orderId", List.of(sets("box", "x")), List.of())));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clickable twice");
    }

    @Test
    void anActionTheLibraryDoesNotHold_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(link("orderId", List.of(), List.of("a-gone"))));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a-gone");
    }

    @Test
    void writingIntoAControlThatIsNotOnThePage_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of(link("orderId", List.of(sets("nowhere", "")), List.of())));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void writingIntoAGrid_isRefused_sinceAValueHasNowhereToGoInOne() {
        AppPageControl grid = control("g", "grid");
        AppPageControl other = control("g2", "grid");
        grid.setColumnLinks(List.of(link("orderId", List.of(sets("g2", "")), List.of())));
        AppPage page = new AppPage();
        page.setControls(List.of(grid, other));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("input, a hidden field or a label");
    }

    @Test
    void clickableColumnsOnAnythingButAGrid_areRefused() {
        AppPageControl box = control("box2", "text");
        AppPage page = pageWith(box, List.of(link("orderId", List.of(), List.of("a-load-lines"))));
        assertThatThrownBy(() -> AppCatalogService.validateColumnLinks(page, box, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid has clickable columns");
    }

    @Test
    void aGridWithNoClickableColumns_isEveryGridSavedBeforeThis() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, List.of());
        assertThatCode(() -> AppCatalogService.validateColumnLinks(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }
}
