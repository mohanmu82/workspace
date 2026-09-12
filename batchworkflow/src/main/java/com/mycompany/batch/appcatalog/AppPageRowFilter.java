package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * One test a row of the source grid has to pass before a fan-out will make a call for it.
 *
 * <p>A fan-out already runs only over the rows on screen, because a grid the operator has filtered
 * is one they narrowed on purpose. That leaves the narrowing the <em>page</em> means every time:
 * "re-send only the ones that failed", "chase only today's". Written here it is part of the action
 * rather than something the operator has to remember to type into the grid's filter row before
 * clicking, and it survives the grid being refilled.
 *
 * @param column   which column of the source grid is being tested, by the name the grid shows
 * @param operator one of {@link #OPERATORS}; blank reads as {@link #CONTAINS}, which is what the
 *                 grid's own filter row does
 * @param value    what to test it against — a template, so {@code ${statusPick}} tests each row
 *                 against a control the operator set. A template that resolves to nothing drops
 *                 the filter rather than matching nothing: an empty box under a grid means "any",
 *                 the same way it does in the grid's own filter row
 */
public record AppPageRowFilter(String column, String operator, String value) {

    /** Case-insensitive substring, the same test the grid's own filter row makes. */
    public static final String CONTAINS = "CONTAINS";

    /**
     * Every operator a filter may name. Text tests are case-insensitive, since the cell they read
     * is the text the grid shows and nobody filtering a grid by eye is thinking about case.
     * {@code GT} and {@code LT} compare as numbers when both sides are numbers and as text
     * otherwise, which is the same rule the grid sorts a column by.
     */
    public static final List<String> OPERATORS = List.of(
            CONTAINS, "NOT_CONTAINS", "EQUALS", "NOT_EQUALS", "STARTS_WITH", "ENDS_WITH",
            "GT", "LT", "EMPTY", "NOT_EMPTY", "REGEX");

    /** Operators that test the cell alone, so a blank value is what they are meant to carry. */
    public static final List<String> VALUELESS = List.of("EMPTY", "NOT_EMPTY");

    public String operatorOrDefault() {
        return operator == null || operator.isBlank() ? CONTAINS : operator.trim().toUpperCase();
    }
}
