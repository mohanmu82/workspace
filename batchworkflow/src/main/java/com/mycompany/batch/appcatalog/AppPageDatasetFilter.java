package com.mycompany.batch.appcatalog;

import java.util.List;

/**
 * One condition narrowing which rows of a static dataset a {@link AppPageAction#DATASET} action
 * brings back — see {@link AppPageAction#getDatasetFilters()}.
 *
 * <p>The same three parts the dataset library's own saved filters are made of: a column, how it is
 * tested, and what it is tested against. Written here rather than reusing
 * {@code FilterFavorite.FilterCondition} because a page's condition has something that library's
 * has not — {@link #value} may be a {@code ${fieldName}} template, picked up off the page's controls
 * when the action runs. That is the whole reason a page would write its own condition instead of
 * naming a favourite: a favourite is a fixed question, and a page's filter is usually the operator's
 * pick turned into one.
 *
 * <p>Resolved against the page and then sent to the dataset library, which does the matching — the
 * rows that did not match never reach the browser. A condition whose template resolves to nothing is
 * dropped rather than sent as a test against the empty string, so an untouched box under a grid
 * reads as "don't narrow by this" rather than as "find the rows whose desk is blank".
 *
 * @param attribute the dataset column tested
 * @param op        one of {@link #OPERATORS}; blank reads as {@code equals}
 * @param value     what it is tested against, which may carry {@code ${fieldName}} placeholders
 */
public record AppPageDatasetFilter(String attribute, String op, String value) {

    /**
     * How a column may be tested, exactly as the dataset library spells them — this list is sent to
     * it rather than interpreted here, so a name it does not know would be a filter that silently
     * matched everything.
     */
    public static final List<String> OPERATORS =
            List.of("equals", "notEquals", "contains", "notContains", "startsWith", "endsWith");

    /** The default is the one an operator means by typing a value beside a column and nothing else. */
    public static final String DEFAULT_OPERATOR = "equals";

    /** {@link #op}, with the default filled in — what is actually sent. */
    public String opOrDefault() {
        return op == null || op.isBlank() ? DEFAULT_OPERATOR : op;
    }
}
