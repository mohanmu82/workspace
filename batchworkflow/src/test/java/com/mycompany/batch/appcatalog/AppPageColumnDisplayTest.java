package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two settings that decide what a grid's table looks like before anybody touches it: the
 * columns it leaves undrawn, and whether it opens with its text wrapped.
 *
 * <p>Both are checked at the save for the same reason every other grid-only setting is: a list of
 * hidden columns left behind on a control that has since become a pie chart is a setting nothing
 * will ever read, and a page carrying it looks configured in a way it is not.
 */
class AppPageColumnDisplayTest {

    private static AppPageControl grid() {
        AppPageControl control = new AppPageControl();
        control.setType("grid");
        control.setLabel("Orders");
        return control;
    }

    @Test
    void namingColumnsToHide_isFineOnAGrid() {
        AppPageControl grid = grid();
        grid.setHiddenColumns(List.of("internalId", "correlationToken"));
        assertThatCode(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void hidingNothing_isEveryGridSavedBeforeThisExisted() {
        assertThatCode(() -> AppCatalogService.validateHiddenColumns(grid(), "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void onlyAGridHasColumnsToHide() {
        AppPageControl pie = new AppPageControl();
        pie.setType("pie");
        pie.setLabel("Split");
        pie.setHiddenColumns(List.of("internalId"));
        assertThatThrownBy(() -> AppCatalogService.validateHiddenColumns(pie, "Control 'Split'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pie");
    }

    @Test
    void aBlankOrRepeatedName_isRefused() {
        AppPageControl grid = grid();
        grid.setHiddenColumns(List.of("internalId", "  "));
        assertThatThrownBy(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class);
        grid.setHiddenColumns(List.of("internalId", "INTERNALID"));
        assertThatThrownBy(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("twice");
    }

    @Test
    void fixingAColumnAndHidingIt_areTwoInstructionsThatContradictEachOther() {
        AppPageControl grid = grid();
        grid.setColumns(List.of("id", "status", "internalId"));
        grid.setHiddenColumns(List.of("internalid"));
        assertThatThrownBy(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one list or the other");
    }

    @Test
    void showingAColumnFirstAndHidingIt_isRefusedTheSameWay() {
        AppPageControl grid = grid();
        grid.setLeadColumns(List.of("status"));
        grid.setHiddenColumns(List.of("status"));
        assertThatThrownBy(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void hidingAColumnTheFixedListDoesNotName_isTheOrdinaryCase() {
        // The rows carry more than the grid draws, and one of the extras is being hidden from the
        // exports as well — nothing contradicts anything, so it saves.
        AppPageControl grid = grid();
        grid.setColumns(List.of("id", "status"));
        grid.setHiddenColumns(List.of("internalId"));
        assertThatCode(() -> AppCatalogService.validateHiddenColumns(grid, "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void onlyAGridHasRowsToWrap() {
        AppPageControl grid = grid();
        grid.setWrapText(true);
        assertThatCode(() -> AppCatalogService.validateWrapText(grid, "Control 'Orders'"))
                .doesNotThrowAnyException();

        AppPageControl box = new AppPageControl();
        box.setType("textarea");
        box.setLabel("Notes");
        box.setWrapText(true);
        assertThatThrownBy(() -> AppCatalogService.validateWrapText(box, "Control 'Notes'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("textarea");
    }
}
