package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * A group-by applied to the rows an action binds, before they reach the grid it fills — see
 * {@link AppPageAction#getPivot()} and {@link AppPageBinding#getPivot()}.
 *
 * <p>The rows the call brought back are collected out of sight, grouped down by {@link #rows} and
 * across by {@link #cols}, and each cell works out {@link #values}; the grid is then filled with that
 * table rather than with the rows. It is the arithmetic of the Analyze screen, saved: Analyze is where
 * one is usually built, and its "Add to page as action" writes one of these. The browser does the
 * grouping, with the same code Analyze uses, so the grid shows exactly what Analyze did.
 *
 * <p>The grid's columns are the row fields followed by one column per column group per value —
 * {@code "EMEA / Q3 · Sum of amount"} — and a {@code Total · …} column per value when
 * {@link #grandCol} is on and anything is pivoted across. {@link #grandRow} adds a {@code Total} line
 * when anything is grouped down.
 */
public class AppPagePivot {

    /** {@link Value#agg()} counting rows rather than values of any column; its field is ignored. */
    public static final String ROW_COUNT = "rows";

    /** What a value can work out. Mirrors GridPivot.AGGS in gridpivot.js, plus the row count. */
    public static final List<String> AGGS = List.of("sum", "count", "unique", "avg", "min", "max", ROW_COUNT);

    /** How group keys are ordered: A→Z, or by their total of the first value. */
    public static final List<String> ORDERS = List.of("key", "valueDesc", "valueAsc");

    /**
     * One value each cell works out.
     *
     * @param field the column read; blank for {@link #ROW_COUNT}
     * @param agg   one of {@link #AGGS}
     */
    public record Value(String field, String agg) {
        public boolean countsRows() { return ROW_COUNT.equals(agg); }
    }

    /** Columns grouped down the side, outermost first. */
    private List<String> rows = new ArrayList<>();
    /** Columns pivoted across the top, outermost first. */
    private List<String> cols = new ArrayList<>();
    /** What each cell works out; empty counts rows. */
    private List<Value> values = new ArrayList<>();
    private boolean grandRow = true;
    private boolean grandCol = true;
    /** Off drops every row with a blank group key rather than grouping it under (blank). */
    private boolean blanks = true;
    private String rowOrder = "key";
    private String colOrder = "key";

    /** Whether this groups anything at all; one that does not leaves the rows as they came. */
    public boolean groupsAnything() {
        return !rows.isEmpty() || !cols.isEmpty() || !values.isEmpty();
    }

    public List<String> getRows()                { return rows; }
    public void setRows(List<String> rows)       { this.rows = names(rows); }

    public List<String> getCols()                { return cols; }
    public void setCols(List<String> cols)       { this.cols = names(cols); }

    public List<Value> getValues()               { return values; }
    public void setValues(List<Value> values)    { this.values = values != null ? new ArrayList<>(values) : new ArrayList<>(); }

    public boolean isGrandRow()                  { return grandRow; }
    public void    setGrandRow(boolean grandRow) { this.grandRow = grandRow; }

    public boolean isGrandCol()                  { return grandCol; }
    public void    setGrandCol(boolean grandCol) { this.grandCol = grandCol; }

    public boolean isBlanks()                    { return blanks; }
    public void    setBlanks(boolean blanks)     { this.blanks = blanks; }

    public String getRowOrder()                  { return rowOrder; }
    public void   setRowOrder(String rowOrder)   { this.rowOrder = ORDERS.contains(rowOrder) ? rowOrder : "key"; }

    public String getColOrder()                  { return colOrder; }
    public void   setColOrder(String colOrder)   { this.colOrder = ORDERS.contains(colOrder) ? colOrder : "key"; }

    private static List<String> names(List<String> names) {
        List<String> out = new ArrayList<>();
        if (names == null) return out;
        for (String name : names) {
            if (name != null && !name.isBlank()) out.add(name.trim());
        }
        return out;
    }
}
