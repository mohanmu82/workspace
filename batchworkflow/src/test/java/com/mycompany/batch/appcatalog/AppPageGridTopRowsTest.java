package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A grid's row cap — how many rows it shows once they are in order — as the save reads it.
 *
 * <p>The cap only ever narrows a grid, so every value that cannot be read as a count of rows has to
 * come out as "no cap" rather than as "no rows": a mistyped box must leave the grid showing what it
 * always showed, not empty it.
 */
class AppPageGridTopRowsTest {

    private static AppPageControl grid() {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-results");
        control.setLabel("Results");
        control.setType("grid");
        return control;
    }

    @Test
    void aGridWithNoCap_showsEveryRow() {
        assertThat(grid().getTopRows()).isZero();
    }

    @Test
    void aCapIsKeptAsGiven() {
        AppPageControl control = grid();
        control.setTopRows(10);
        assertThat(control.getTopRows()).isEqualTo(10);
    }

    @Test
    void aNegativeCap_isNoCapRatherThanNoRows() {
        AppPageControl control = grid();
        control.setTopRows(-5);
        assertThat(control.getTopRows()).isZero();
    }

    @Test
    void theCapAndTheOrderAreSetIndependently() {
        AppPageControl control = grid();
        control.setSortColumn("durationMs");
        control.setSortDirection("desc");
        control.setTopRows(10);
        // "The ten slowest" is these three settings together: the column names what slow means, the
        // direction puts the slowest first, and the cap keeps ten of them.
        assertThat(control.getSortColumn()).isEqualTo("durationMs");
        assertThat(control.getSortDirection()).isEqualTo("DESC");
        assertThat(control.getTopRows()).isEqualTo(10);
    }

    @Test
    void aCapSurvivesTheOrderBeingCleared() {
        AppPageControl control = grid();
        control.setTopRows(25);
        control.setSortColumn("  ");
        // Still a cap: the operator can sort the grid themselves, and the first 25 of whatever order
        // they choose is the answer they asked for.
        assertThat(control.getSortColumn()).isNull();
        assertThat(control.getTopRows()).isEqualTo(25);
    }
}
