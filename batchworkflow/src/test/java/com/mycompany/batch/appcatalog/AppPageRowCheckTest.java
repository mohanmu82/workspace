package com.mycompany.batch.appcatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sentence a grid judges its rows by, read the way the save reads it.
 *
 * <p>What is being checked here is only whether the expression can be <em>read</em> — the browser is
 * what runs it, over the rows as they arrive. That split is the point of the checks rather than an
 * incidental one: an expression that does not parse leaves a grid that judges nothing while looking
 * as though it does, so it is refused at the save, where the designer is still standing in front of
 * the box they typed it into.
 */
class AppPageRowCheckTest {

    // ── Expressions that read ────────────────────────────────────────────────

    @Test
    void noExpression_isTheOrdinaryGrid() {
        assertThatCode(() -> AppPageRowCheck.check(null)).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("   ")).doesNotThrowAnyException();
    }

    @Test
    void theExampleEveryoneWrites_reads() {
        assertThatCode(() -> AppPageRowCheck.check("STATUS!=SUCCESS || RECORDCOUNT=0"))
                .doesNotThrowAnyException();
    }

    @Test
    void spacingDoesNotMatter() {
        assertThatCode(() -> AppPageRowCheck.check("  STATUS   !=   SUCCESS  ")).doesNotThrowAnyException();
    }

    @Test
    void everyComparison_reads() {
        for (String op : new String[] { "=", "==", "!=", "<>", ">", ">=", "<", "<=", "~", "!~" }) {
            assertThatCode(() -> AppPageRowCheck.check("COUNT " + op + " 0"))
                    .as("the " + op + " test").doesNotThrowAnyException();
        }
    }

    @Test
    void bothWaysOfJoining_read() {
        assertThatCode(() -> AppPageRowCheck.check("A = 1 && B = 2 || C = 3")).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("A = 1 and B = 2 or C = 3")).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("A = 1 AND B = 2 OR C = 3")).doesNotThrowAnyException();
    }

    @Test
    void bracketsAndNegation_read() {
        assertThatCode(() -> AppPageRowCheck.check("!(STATUS = SUCCESS && COUNT > 0)")).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("not (STATUS = SUCCESS)")).doesNotThrowAnyException();
    }

    /** A column standing alone is a test in itself: the row is wrong if the cell holds anything. */
    @Test
    void aBareColumn_isATestOnItsOwn() {
        assertThatCode(() -> AppPageRowCheck.check("ERRORMESSAGE")).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("ERRORMESSAGE || COUNT = 0")).doesNotThrowAnyException();
    }

    /** Quoting is how a value that would otherwise read as a column, or holds a space, is meant. */
    @Test
    void quotedValues_read() {
        assertThatCode(() -> AppPageRowCheck.check("STATUS != 'NOT FOUND'")).doesNotThrowAnyException();
        assertThatCode(() -> AppPageRowCheck.check("MESSAGE ~ \"timed out\"")).doesNotThrowAnyException();
    }

    // ── Expressions that do not ──────────────────────────────────────────────

    @Test
    void aTestWithNothingOnTheRight_isRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("STATUS !="))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stops before");
    }

    @Test
    void anUnclosedBracket_isRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("(STATUS = SUCCESS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bracket");
    }

    @Test
    void anUnclosedQuote_isRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("STATUS = 'SUCCESS"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quote");
    }

    @Test
    void twoTestsWithNothingJoiningThem_areRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("STATUS = SUCCESS COUNT = 0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nothing joins on to");
    }

    @Test
    void aJoiningWordUsedAsAValue_isRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("STATUS = or FAILED"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("joins two tests");
    }

    @Test
    void aJoinWithNothingAfterIt_isRefused() {
        assertThatThrownBy(() -> AppPageRowCheck.check("STATUS = SUCCESS ||"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stops before");
    }

    // ── Where it hangs off a control ─────────────────────────────────────────

    private static AppPageControl control(String type, String expression) {
        AppPageControl control = new AppPageControl();
        control.setControlId("c-results");
        control.setLabel("Results");
        control.setType(type);
        control.setRowErrorExpression(expression);
        return control;
    }

    @Test
    void aGridWithNoCheck_isTheOrdinaryCase() {
        assertThatCode(() -> AppCatalogService.validateRowErrorExpression(control("grid", null), "Control 'Results'"))
                .doesNotThrowAnyException();
    }

    @Test
    void aGridWithAReadableCheck_isKept() {
        assertThatCode(() -> AppCatalogService.validateRowErrorExpression(
                control("grid", "STATUS != SUCCESS || RECORDCOUNT = 0"), "Control 'Results'"))
                .doesNotThrowAnyException();
    }

    /** Only a grid has rows to judge, so a check left behind on something else is dead wiring. */
    @Test
    void aCheckOnSomethingThatIsNotAGrid_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateRowErrorExpression(
                control("text", "STATUS != SUCCESS"), "Control 'Results'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only a grid checks its rows");
    }

    /** The message has to say which grid and what about the expression could not be read. */
    @Test
    void aGridWithACheckThatCannotBeRead_isRefused() {
        assertThatThrownBy(() -> AppCatalogService.validateRowErrorExpression(
                control("grid", "(STATUS = SUCCESS"), "Control 'Results'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Control 'Results'")
                .hasMessageContaining("bracket")
                .hasMessageContaining("(STATUS = SUCCESS");
    }
}
