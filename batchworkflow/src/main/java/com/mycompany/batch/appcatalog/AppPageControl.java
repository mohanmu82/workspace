package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * One element on a page — an input the operator fills in, a button that runs use case instances, or
 * a grid the results land in.
 *
 * <p>Placement is an explicit {@link #row}/{@link #col} on the page's twenty-four-column grid with a
 * {@link #span} of columns, rather than document order, so the builder can drop a control anywhere
 * and have it stay there. Two controls may legitimately share a cell while one is being moved onto
 * another; nothing depends on them not overlapping.
 *
 * <p>{@link #fieldName} is the name this control's value answers to everywhere else: in a button
 * action's input templates ({@code ${orderId}}), and as the thing {@link #mandatory} is checked on
 * before any action runs. Buttons, grids and labels carry no value and need no field name.
 */
public class AppPageControl {

    /** {@link #trigger}: the control is clicked — a button or a link. */
    public static final String ON_CLICK  = "CLICK";
    /** {@link #trigger}: the operator changes the control's value — typically a select. */
    public static final String ON_CHANGE = "CHANGE";

    /** Stable id, unique within the page — what actions point at when they name a target. */
    private String controlId;
    private String fieldName;
    private String label;
    /** text, textarea, number, date, hidden, select, checkbox, button, link, grid, tabs, pie or label. */
    private String type = "text";
    /**
     * What the control starts the run holding: the value in a box, the text on a label — and, on a
     * link, the address it points at before anything fills one in. Blank on a link leaves it a
     * trigger until an action targeting it binds one.
     */
    private String defaultValue;
    private boolean mandatory;
    private String placeholder;
    private String helpText;
    /** CSS color applied to the control's label and value when the page runs — "blue", "#d63384". */
    private String color;

    private int row;
    private int col;
    private int span = 6;
    /**
     * How many rows tall — 1 for an input, more for a grid that needs room to show its rows, and
     * down to half a row for something that should sit thinner than a normal field, such as a link.
     * Kept in half-row steps because the layout grid is drawn in half rows.
     */
    private double rowSpan = 1;

    /** Select controls only. */
    private AppPageOptionSource optionSource;
    /**
     * Actions written directly onto this control — run in order, stopping at the first failure.
     * Predates the page-level library and is still honoured, so every page saved before it keeps
     * working; a control may carry both, and its {@link #actionIds} run first.
     */
    private List<AppPageAction> actions = new ArrayList<>();
    /**
     * Ids of {@link AppPage#getActions() page-level actions} this control triggers, in order. The
     * same action id may appear on any number of controls — that is the point of the library.
     */
    private List<String> actionIds = new ArrayList<>();
    /**
     * What makes this control's actions run: {@link #ON_CLICK} for a button or link, or
     * {@link #ON_CHANGE} for a value control whose actions fire whenever the operator changes it —
     * picking a different environment in a select and having the grid reload itself.
     *
     * <p>Page load is deliberately not one of these: it belongs to the page, not to a control, and
     * lives on {@link AppPage#getOnLoadActionIds()}.
     */
    private String trigger = ON_CLICK;
    /**
     * Values this control writes into other controls when it is triggered, in order, before any of
     * its {@link #actionIds actions} run — see {@link AppPageAssignment}. Empty for a control that
     * only runs actions, which is the older and still ordinary case.
     */
    private List<AppPageAssignment> assignments = new ArrayList<>();
    /** Grid controls only; empty means the columns follow the fields of the returned rows. */
    private List<String> columns = new ArrayList<>();
    /**
     * Grid controls only: which of its columns drill down when a cell is clicked, and what each
     * click does — see {@link AppPageColumnLink}. Empty for a grid that is only somewhere answers
     * land, which is every grid saved before this existed.
     *
     * <p>The grid itself stays untriggerable, and deliberately: there is no such thing as clicking a
     * grid, only a cell in one, so what runs belongs to the column rather than to the control. That
     * is also why a grid keeps its place in the service's TRIGGERLESS_TYPES while carrying these.
     */
    private List<AppPageColumnLink> columnLinks = new ArrayList<>();
    /**
     * Pie controls only: the slices, in the order they are drawn, each an {@link AppPageOption}
     * whose {@link AppPageOption#key() key} names the slice and whose
     * {@link AppPageOption#value() value} is how big it is.
     *
     * <p>The size is held as text, like every other value a control carries, and is parsed when the
     * page is saved: a slice whose value is not a number has no angle to be drawn at, so the page
     * is refused rather than stored with a slice that could never appear in the chart.
     */
    private List<AppPageOption> slices = new ArrayList<>();
    /**
     * Tabs controls only: the ids of the grid controls this tab set holds, in tab order.
     *
     * <p>A page that answers one question out of ten endpoints used to be ten grids stacked down a
     * screen nobody could see the bottom of. A tabs control takes those grids over: each keeps its
     * own id, columns and the actions aimed at it — an action still targets the grid, never the tab
     * set — but they are laid out inside the tab set rather than on the page, and only the selected
     * one is on screen. A grid named here therefore ignores its own {@link #row}/{@link #col},
     * which is also what lets dropping it back onto the canvas put it where it always was.
     *
     * <p>A grid belongs to at most one tab set; a page where two claim the same grid is refused,
     * since "which tab is this grid in" would otherwise have two answers.
     */
    private List<String> tabControlIds = new ArrayList<>();

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
    public void setCol(int col)   { this.col = Math.min(Math.max(col, 0), 23); }

    public int  getSpan()          { return span; }
    public void setSpan(int span)  { this.span = Math.min(Math.max(span, 1), 24); }

    public double getRowSpan()                 { return rowSpan; }
    /** Snapped to the nearest half row so the row span always lands on a grid line. */
    public void   setRowSpan(double rowSpan)   { this.rowSpan = Math.min(Math.max(Math.round(rowSpan * 2) / 2.0, 0.5), 12); }

    public AppPageOptionSource getOptionSource()                             { return optionSource; }
    public void setOptionSource(AppPageOptionSource optionSource)            { this.optionSource = optionSource; }

    public List<AppPageAction> getActions()                        { return actions; }
    public void setActions(List<AppPageAction> actions)            { this.actions = actions != null ? actions : new ArrayList<>(); }

    public List<String> getActionIds()                             { return actionIds; }
    public void setActionIds(List<String> actionIds)               { this.actionIds = actionIds != null ? actionIds : new ArrayList<>(); }

    public String getTrigger()                { return trigger; }
    public void   setTrigger(String trigger)  { this.trigger = ON_CHANGE.equalsIgnoreCase(trigger) ? ON_CHANGE : ON_CLICK; }

    public List<AppPageAssignment> getAssignments()                          { return assignments; }
    public void setAssignments(List<AppPageAssignment> assignments)          { this.assignments = assignments != null ? assignments : new ArrayList<>(); }

    public List<String> getColumns()                     { return columns; }
    public void setColumns(List<String> columns)         { this.columns = columns != null ? columns : new ArrayList<>(); }

    public List<AppPageColumnLink> getColumnLinks()                        { return columnLinks; }
    public void setColumnLinks(List<AppPageColumnLink> columnLinks)        { this.columnLinks = columnLinks != null ? columnLinks : new ArrayList<>(); }

    public List<AppPageOption> getSlices()                   { return slices; }
    public void setSlices(List<AppPageOption> slices)        { this.slices = slices != null ? slices : new ArrayList<>(); }

    public List<String> getTabControlIds()                      { return tabControlIds; }
    public void setTabControlIds(List<String> tabControlIds)    { this.tabControlIds = tabControlIds != null ? tabControlIds : new ArrayList<>(); }
}
