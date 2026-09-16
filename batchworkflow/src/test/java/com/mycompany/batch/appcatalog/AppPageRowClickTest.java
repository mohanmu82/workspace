package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a grid's clickable rows have to be for the page to be storable. The failure every check here
 * guards is the one a clickable column guards too, one step wider: a row drawn as clickable tells the
 * operator by the only means the page has that pointing at it will do something, and it must not then
 * answer the click with nothing.
 */
class AppPageRowClickTest {

    private static final List<String> LIBRARY = List.of("a-load-lines", "a-load-customer");

    private static AppPageControl control(String id, String type) {
        AppPageControl control = new AppPageControl();
        control.setControlId(id);
        control.setType(type);
        control.setLabel(type + " " + id);
        if ("hidden".equals(type) || "text".equals(type)) control.setFieldName("f" + id);
        return control;
    }

    private static AppPageAssignment sets(String targetControlId, String value) {
        AppPageAssignment assignment = new AppPageAssignment();
        assignment.setTargetControlId(targetControlId);
        assignment.setValue(value);
        return assignment;
    }

    private static AppPageRowClick click(List<AppPageAssignment> assignments, List<String> actionIds) {
        AppPageRowClick click = new AppPageRowClick();
        click.setAssignments(assignments);
        click.setActionIds(actionIds);
        return click;
    }

    /** A page holding one grid, a hidden field for the row's JSON and a box, with the click as given. */
    private static AppPage pageWith(AppPageControl grid, AppPageRowClick click) {
        grid.setRowClick(click);
        AppPage page = new AppPage();
        page.setControls(List.of(grid, control("rowJson", "hidden"), control("box", "text")));
        return page;
    }

    @Test
    void aGridWithNoRowClick_isTheOrdinaryGridItAlwaysWas() {
        AppPageControl grid = control("g", "grid");
        assertThat(grid.getRowClick()).isNull();
        assertThatCode(() -> AppCatalogService.validateRowClick(pageWith(grid, null), grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void puttingTheRowSomewhereAndRunningSomethingOverIt_isThePointOfIt() {
        AppPageControl grid = control("g", "grid");
        // The blank value is the whole row as JSON — see AppPageRowClick.
        AppPage page = pageWith(grid, click(List.of(sets("rowJson", "")), List.of("a-load-lines")));
        assertThatCode(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void puttingTheRowSomewhereWithoutRunningAnything_isACompleteJob() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(sets("rowJson", "")), List.of()));
        assertThatCode(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void runningSomethingWithoutSettingAnything_isAlsoComplete() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(), List.of("a-load-lines")));
        assertThatCode(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void namingFieldsOfTheRowBesideTheWholeRow_isOneClickDoingBoth() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(
                List.of(sets("rowJson", ""), sets("box", "${orderId}")), List.of("a-load-lines")));
        assertThatCode(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .doesNotThrowAnyException();
    }

    @Test
    void rowsThatNeitherSetNorRun_areRefusedRatherThanDrawnAsLive() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(), List.of()));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a click on one would do nothing");
    }

    @Test
    void onlyAGridHasRows_soARowClickLeftOnSomethingElseIsRefused() {
        AppPageControl box = control("box2", "text");
        box.setRowClick(click(List.of(), List.of("a-load-lines")));
        AppPage page = new AppPage();
        page.setControls(List.of(box));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, box, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid or a pie with grids has clickable rows");
    }

    @Test
    void aPieWithGrids_mayMakeTheRowsOfItsGridsClickable() {
        AppPageControl pie = control("pg", "piegrid");
        AppPage page = pageWith(pie, click(List.of(sets("rowJson", "")), List.of("a-load-lines")));
        assertThatCode(() -> AppCatalogService.validateRowClick(page, pie, LIBRARY)).doesNotThrowAnyException();
    }

    @Test
    void writingIntoAControlThatIsNotOnThePage_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(sets("gone", "")), List.of()));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not on this page");
    }

    @Test
    void writingTheRowIntoTheGridItCameFrom_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(sets("g", "")), List.of()));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("into itself");
    }

    @Test
    void writingTheRowIntoSomethingThatHoldsNoValue_isRefused() {
        AppPageControl grid = control("g", "grid");
        grid.setRowClick(click(List.of(sets("other", "")), List.of()));
        AppPage page = new AppPage();
        page.setControls(List.of(grid, control("other", "button")));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a value goes into an input");
    }

    @Test
    void runningAnActionThatIsNotOnThePage_isRefused() {
        AppPageControl grid = control("g", "grid");
        AppPage page = pageWith(grid, click(List.of(), List.of("a-somewhere-else")));
        assertThatThrownBy(() -> AppCatalogService.validateRowClick(page, grid, LIBRARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("runs an action that is not on this page");
    }

    @Test
    void clickableRowsAndClickableColumnsLiveTogether() {
        AppPageControl grid = control("g", "grid");
        AppPageColumnLink link = new AppPageColumnLink();
        link.setColumn("orderId");
        link.setAssignments(List.of(sets("box", "")));
        link.setActionIds(List.of("a-load-lines"));
        grid.setColumnLinks(List.of(link));
        AppPage page = pageWith(grid, click(List.of(sets("rowJson", "")), List.of("a-load-customer")));
        assertThatCode(() -> {
            AppCatalogService.validateColumnLinks(page, grid, LIBRARY);
            AppCatalogService.validateRowClick(page, grid, LIBRARY);
        }).doesNotThrowAnyException();
    }
}
