package com.mycompany.batch.model;

/**
 * A single filter predicate applied to a DataRow either before (filterInput) or
 * after (filterOutput) activity execution.
 *
 * <ul>
 *   <li>{@code column}    — the DataRow key to test</li>
 *   <li>{@code value}     — the value to compare against</li>
 *   <li>{@code operation} — {@code "eq"} (default) for exact match,
 *                           {@code "like"} for Java regex match</li>
 * </ul>
 *
 * If the column is not present in the DataRow the filter is skipped (the row passes through).
 */
public class FilterRule {

    private String column;
    private String value;
    private String operation = "eq";

    public String getColumn()                   { return column; }
    public void   setColumn(String column)      { this.column = column; }

    public String getValue()                    { return value; }
    public void   setValue(String value)        { this.value = value; }

    public String getOperation()                { return operation; }
    public void   setOperation(String operation){ this.operation = operation; }
}
