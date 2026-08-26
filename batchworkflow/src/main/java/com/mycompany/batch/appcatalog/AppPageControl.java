package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * One element on a page — an input the operator fills in, a button that runs use case instances, or
 * a grid the results land in.
 *
 * <p>Placement is an explicit {@link #row}/{@link #col} on the page's twelve-column grid with a
 * {@link #span} of columns, rather than document order, so the builder can drop a control anywhere
 * and have it stay there. Two controls may legitimately share a cell while one is being moved onto
 * another; nothing depends on them not overlapping.
 *
 * <p>{@link #fieldName} is the name this control's value answers to everywhere else: in a button
 * action's input templates ({@code ${orderId}}), and as the thing {@link #mandatory} is checked on
 * before any action runs. Buttons, grids and labels carry no value and need no field name.
 */
public class AppPageControl {

    /** Stable id, unique within the page — what actions point at when they name a target. */
    private String controlId;
    private String fieldName;
    private String label;
    /** text, textarea, number, date, hidden, select, checkbox, button, link, grid or label. */
    private String type = "text";
    private String defaultValue;
    private boolean mandatory;
    private String placeholder;
    private String helpText;
    /** CSS color applied to the control's label and value when the page runs — "blue", "#d63384". */
    private String color;

    private int row;
    private int col;
    private int span = 3;
    /**
     * How many rows tall — 1 for an input, more for a grid that needs room to show its rows, and
     * down to half a row for something that should sit thinner than a normal field, such as a link.
     * Kept in half-row steps because the layout grid is drawn in half rows.
     */
    private double rowSpan = 1;

    /** Select controls only. */
    private AppPageOptionSource optionSource;
    /** Button controls only — run in order, stopping at the first failure. */
    private List<AppPageAction> actions = new ArrayList<>();
    /** Grid controls only; empty means the columns follow the fields of the returned rows. */
    private List<String> columns = new ArrayList<>();

    public String getControlId()                    { return controlId; }
    public void   setControlId(String controlId)    { this.controlId = controlId; }

    public String getFieldName()                    { return fieldName; }
    public void   setFieldName(String fieldName)    { this.fieldName = fieldName; }

    public String getLabel()              { return label; }
    public void   setLabel(String label)  { this.label = label; }

    public String getType()             { return type; }
    public void   setType(String type)  { this.type = type != null && !type.isBlank() ? type : "text"; }

    public String getDefaultValue()                       { return defaultValue; }
    public void   setDefaultValue(String defaultValue)    { this.defaultValue = defaultValue; }

    public boolean isMandatory()                    { return mandatory; }
    public void    setMandatory(boolean mandatory)  { this.mandatory = mandatory; }

    public String getPlaceholder()                    { return placeholder; }
    public void   setPlaceholder(String placeholder)  { this.placeholder = placeholder; }

    public String getHelpText()                  { return helpText; }
    public void   setHelpText(String helpText)   { this.helpText = helpText; }

    public String getColor()               { return color; }
    public void   setColor(String color)   { this.color = color; }

    public int  getRow()          { return row; }
    public void setRow(int row)   { this.row = Math.max(row, 0); }

    public int  getCol()          { return col; }
    public void setCol(int col)   { this.col = Math.min(Math.max(col, 0), 11); }

    public int  getSpan()          { return span; }
    public void setSpan(int span)  { this.span = Math.min(Math.max(span, 1), 12); }

    public double getRowSpan()                 { return rowSpan; }
    /** Snapped to the nearest half row so the row span always lands on a grid line. */
    public void   setRowSpan(double rowSpan)   { this.rowSpan = Math.min(Math.max(Math.round(rowSpan * 2) / 2.0, 0.5), 12); }

    public AppPageOptionSource getOptionSource()                             { return optionSource; }
    public void setOptionSource(AppPageOptionSource optionSource)            { this.optionSource = optionSource; }

    public List<AppPageAction> getActions()                        { return actions; }
    public void setActions(List<AppPageAction> actions)            { this.actions = actions != null ? actions : new ArrayList<>(); }

    public List<String> getColumns()                     { return columns; }
    public void setColumns(List<String> columns)         { this.columns = columns != null ? columns : new ArrayList<>(); }
}
