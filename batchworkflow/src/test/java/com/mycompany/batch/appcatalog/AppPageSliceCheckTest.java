package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A pie's error check: which of its wedges are wrong.
 *
 * <p>The grid's row check turned on the one thing a pie has instead of rows. A grid has always
 * answered "did the call work" and "are the rows right" separately; a pie had only the first, so a
 * status pie with a fat red DOWN wedge in it reported success because the call that drew it had
 * worked. What is checked here is the save's half of closing that: only a pie carries one, and what it
 * carries has to be an expression the browser can actually read — one it cannot would leave a chart
 * judging nothing while looking as though it did.
 */
class AppPageSliceCheckTest {

    private static AppPageControl control(String type, String expression) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-status");
        control.setType(type);
        control.setLabel("Status");
        control.setSliceErrorExpression(expression);
        return control;
    }

    private static void check(AppPageControl control) {
        AppCatalogService.validateSliceErrorExpression(control, "Control 'Status'");
    }

    @Test
    void aPieCheckingItsWedgesByName_isTheOrdinaryCase() {
        assertThatCode(() -> check(control("pie", "NAME != SUCCESS && VALUE > 0"))).doesNotThrowAnyException();
    }

    @Test
    void aPieWithGridsCarriesOneToo() {
        // Its wedges come from a call rather than from the designer, which is exactly when a check
        // over them is worth having.
        assertThatCode(() -> check(control("piegrid", "NAME ~ FAIL"))).doesNotThrowAnyException();
    }

    @Test
    void aPieWithNoCheck_judgesNothingAndIsFine() {
        AppPageControl pie = control("pie", null);
        assertThat(pie.getSliceErrorExpression()).isNull();
        assertThatCode(() -> check(pie)).doesNotThrowAnyException();
    }

    @Test
    void blankIsNoCheckRatherThanAnEmptyOne() {
        assertThat(control("pie", "   ").getSliceErrorExpression()).isNull();
    }

    @Test
    void anExpressionIsTrimmedAsItIsStored() {
        assertThat(control("pie", "  VALUE > 0  ").getSliceErrorExpression()).isEqualTo("VALUE > 0");
    }

    @Test
    void aCheckOnAnythingButAPie_isRefused() {
        // Nothing else draws wedges, so it would be a setting nothing would ever read — the rule a
        // grid's own row check follows from the other side.
        assertThatThrownBy(() -> check(control("grid", "NAME != SUCCESS")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a pie checks its slices");
        assertThatThrownBy(() -> check(control("bar", "NAME != SUCCESS")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a pie checks its slices");
    }

    @Test
    void aCheckThatCannotBeRead_isRefused() {
        // Refused here so the message can say which chart and what about the expression was wrong,
        // rather than leaving the browser to fail silently over it.
        assertThatThrownBy(() -> check(control("pie", "NAME !! SUCCESS")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slice check that cannot be read");
        assertThatThrownBy(() -> check(control("pie", "(VALUE > 0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slice check that cannot be read");
    }

    @Test
    void theGrammarIsTheGridsOwn() {
        // Same tests, same joins, same bracketing — so a check reads the same wherever it is written.
        assertThatCode(() -> check(control("pie",
                "(NAME != SUCCESS && VALUE > 0) || NAME ~ 'N/A'"))).doesNotThrowAnyException();
    }
}
