package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A grid's status — a row-count condition and the variable it is published under — as the save reads it. */
class AppPageGridStatusTest {

    private static final String WHERE = "Control 'Results'";

    private static AppPageControl grid(String condition, String variable) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-results");
        control.setLabel("Results");
        control.setType("grid");
        control.setStatusCondition(condition);
        control.setStatusVariable(variable);
        return control;
    }

    @Test
    void conditionIsStoredWithoutSpacesOrCase() {
        assertThat(grid("rowcount >= 0", null).getStatusCondition()).isEqualTo("ROWCOUNT>=0");
        assertThat(grid("  ", null).getStatusCondition()).isNull();
    }

    @Test
    void everyOfferedCondition_isKept() {
        for (String condition : AppPageControl.STATUS_CONDITIONS) {
            assertThatCode(() -> AppCatalogService.validateGridStatus(grid(condition, "errorsOk"), List.of(), WHERE))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void aGridWithNoStatus_isTheOrdinaryCase() {
        assertThatCode(() -> AppCatalogService.validateGridStatus(grid(null, null), List.of(), WHERE))
                .doesNotThrowAnyException();
    }

    @Test
    void anUnknownCondition_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateGridStatus(grid("ROWCOUNT<5", null), List.of(), WHERE))
                .hasMessageContaining("not one of");
    }

    @Test
    void aStatusOnSomethingThatIsNotAGrid_isRefused() {
        AppPageControl text = grid("ROWCOUNT=0", null);
        text.setType("text");
        assertThatThrownBy(() -> AppCatalogService.validateGridStatus(text, List.of(), WHERE))
                .hasMessageContaining("only a grid has a status");
    }

    @Test
    void aVariableWithNoCondition_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateGridStatus(grid(null, "errorsOk"), List.of(), WHERE))
                .hasMessageContaining("no status condition");
    }

    @Test
    void aVariableATemplateCannotSpell_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateGridStatus(grid("ROWCOUNT=0", "errors ok"), List.of(), WHERE))
                .hasMessageContaining("not a name a template can spell");
    }

    @Test
    void aVariableThatIsAlreadyAPageVariable_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateGridStatus(grid("ROWCOUNT=0", "runTag"), List.of("runTag"), WHERE))
                .hasMessageContaining("already a page variable");
    }

    @Test
    void twoGridsPublishingTheSameVariable_isRefused() {
        AppPageControl other = grid("ROWCOUNT>0", "errorsOk");
        other.setControlId("c-other");
        AppPage page = new AppPage();
        page.setControls(List.of(grid("ROWCOUNT=0", "errorsOk"), other));
        assertThatThrownBy(() -> AppCatalogService.validateGridStatusNames(page))
                .hasMessageContaining("same variable");
    }

    @Test
    void aVariableThatIsAFieldName_isRefused() {
        AppPageControl box = new AppPageControl();
        box.setControlId("c-box");
        box.setType("text");
        box.setFieldName("errorsOk");
        AppPage page = new AppPage();
        page.setControls(List.of(grid("ROWCOUNT=0", "errorsOk"), box));
        assertThatThrownBy(() -> AppCatalogService.validateGridStatusNames(page))
                .hasMessageContaining("field name");
    }
}
