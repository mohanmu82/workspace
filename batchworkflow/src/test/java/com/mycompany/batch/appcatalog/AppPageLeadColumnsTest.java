package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The columns a grid shows first — see {@link AppPageControl#getLeadColumns()}.
 *
 * <p>Ordering happens in the browser, over whatever fields the rows turned out to carry, so a name
 * the rows do not answer to is not something this can know about and is deliberately not an error.
 * What it can know about is a list that cannot mean what it says: one on a control with no columns,
 * one naming the same column twice, and one asking for a column first on a grid whose columns are
 * fixed to a set that does not include it.
 */
class AppPageLeadColumnsTest {

    private static AppPageControl grid(List<String> lead) {
        AppPageControl control = new AppPageControl();
        control.setControlId("g");
        control.setType("grid");
        control.setLabel("Orders");
        control.setLeadColumns(lead);
        return control;
    }

    @Test
    void namingTheColumnsThatMatter_putsThemFirst() {
        assertThatCode(() -> AppCatalogService.validateLeadColumns(grid(List.of("status", "orderId")), "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void namingNone_isEveryGridSavedBeforeThisExisted() {
        assertThat(grid(null).getLeadColumns()).isEmpty();
        assertThatCode(() -> AppCatalogService.validateLeadColumns(grid(null), "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aColumnTheRowsMayNotCarry_isNotAnErrorHere() {
        // Whether a field is there is a fact about the response, not about the page: the same grid is
        // routinely filled by two calls, and one of them carrying it is reason enough to name it.
        assertThatCode(() -> AppCatalogService.validateLeadColumns(grid(List.of("whateverTheyReturn")), "Control 'Orders'"))
                .doesNotThrowAnyException();
    }

    @Test
    void theSameColumnTwice_canOnlyBeATypoForAnother() {
        assertThatThrownBy(() -> AppCatalogService.validateLeadColumns(grid(List.of("status", "STATUS")), "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("twice");
    }

    @Test
    void aBlankName_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateLeadColumns(grid(List.of("status", "  ")), "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void onAGridWhoseColumnsAreFixed_itMayOnlyReorderThem() {
        AppPageControl control = grid(List.of("status"));
        control.setColumns(List.of("orderId", "Status", "amount"));
        assertThatCode(() -> AppCatalogService.validateLeadColumns(control, "Control 'Orders'"))
                .doesNotThrowAnyException();

        control.setLeadColumns(List.of("createdAt"));
        assertThatThrownBy(() -> AppCatalogService.validateLeadColumns(control, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never be on the grid");
    }

    @Test
    void aListLeftOnAControlThatIsNoLongerAGrid_isRefused() {
        AppPageControl control = grid(List.of("status"));
        control.setType("select");
        assertThatThrownBy(() -> AppCatalogService.validateLeadColumns(control, "Control 'Orders'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid");
    }
}
