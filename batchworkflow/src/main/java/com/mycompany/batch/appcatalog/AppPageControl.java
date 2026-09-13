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
    /**
     * text, textarea, number, date, hidden, select, multiselect, checkbox, button, link, grid, tabs,
     * pie, piegrid, bar, label or page.
     */
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
    /**
     * Text area controls only: how a value is laid out before it is put into the box — {@code JSON}
     * or {@code XML} pretty-prints it, {@code NONE} puts it in exactly as it came. A value that does
     * not parse as the format asked for goes in unchanged rather than not at all.
     */
    private String textFormat = "NONE";

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
     * Grid controls only: a static dataset whose rows fill this grid as the page opens, instead of
     * the grid waiting for an action to put something in it.
     *
     * <p>Kept apart from {@link #optionSource}, which is a select's affair, because the two answer
     * different questions: a dropdown needs a key and a label off each row, while a grid shows the
     * rows whole under whatever {@link #columns} says. An action may still target a grid filled this
     * way — the dataset is what it starts the run holding, the way a text box starts it holding its
     * default value.
     */
    private String datasetName;
    /**
     * Link controls only: another page in this catalog, by {@link AppPage#getPageName() name}, that
     * this link opens — the other half of what a link may point at, beside the {@link #defaultValue}
     * address that reaches anywhere on the web.
     *
     * <p>Held as the page's name rather than as the URL it resolves to, and that is the point of the
     * field existing at all: a name can be checked against the catalog when the page is saved, so a
     * link aimed at a page that is not there is refused where it can be fixed rather than discovered
     * by an operator clicking it. The running page turns it into the standalone link — the same
     * address the designer's "Open standalone" offers — so the reader lands on the other page in its
     * running form rather than in the builder.
     *
     * <p>Set, it wins over {@link #defaultValue} and over any address an action binds: it is the more
     * specific of the two, and a link that names a page is one somebody meant to go to that page.
     */
    private String linkPageName;
    /**
     * Page controls only: another page in this catalog, by {@link AppPage#getPageName() name}, that
     * runs inside this one. The running page draws it in its own frame, and keeps a tally of every
     * action the child page executes — how many succeeded, how many failed and why — so a parent
     * made of several child pages answers "did it all work" without opening each one.
     *
     * <p>Checked on save like {@link #linkPageName}: the page has to exist, cannot be this page, and
     * cannot lead back here through its own child pages, which would nest frames without end.
     */
    private String childPageName;
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
     * Grid controls only: a test every row is put through as the grid fills, written in the small
     * expression language {@link AppPageRowCheck} reads — {@code STATUS != SUCCESS || RECORDCOUNT = 0}.
     * A row the expression calls true is an error and is drawn in red; the grid's name carries the
     * tally of how many of each there were. Blank — which is every grid saved before this existed —
     * is a grid that judges nothing and shows its rows exactly as it always did.
     *
     * <p>A different question from whether the call worked, and that is the whole point of it: an
     * endpoint that answers 200 with fifteen rows, three of which reconciled to nothing, is a
     * successful call and a failed run, and until now the only way to see that was to read the rows.
     */
    private String rowErrorExpression;
    /**
     * Grid controls only: what counts as success for the grid as a whole, judged on how many rows it
     * holds once filtered — one of {@link #STATUS_CONDITIONS}. Blank is a grid with no status.
     *
     * <p>{@code ROWCOUNT=0} is the one that reads backwards and is the reason this is a choice rather
     * than a fixed rule: a grid of errors pulled out of some output is a success when it is empty.
     */
    private String statusCondition;
    /**
     * Grid controls only: the name the grid's status answers to in templates — {@code ${ordersOk}}
     * reads {@code SUCCESS} or {@code FAILED}. Blank still shows the verdict on the grid, but nothing
     * else on the page can read it.
     */
    private String statusVariable;

    /** The conditions {@link #statusCondition} may hold, spelled the way they are stored. */
    public static final List<String> STATUS_CONDITIONS = List.of("ROWCOUNT>0", "ROWCOUNT>=0", "ROWCOUNT=0");
    /**
     * Grid controls only: the column its rows are ordered by the moment they arrive, and whether
     * that order runs up or down — {@link #sortDirection} being {@code ASC} or {@code DESC}.
     *
     * <p>Blank, which is every grid saved before this existed, leaves the rows in the order the call
     * returned them and waits for the operator to click a heading. Named rather than positional
     * because the columns a grid shows are a list of field names, and a grid that names none of them
     * shows whatever the rows carry — a column number would mean something different on every run.
     *
     * <p>Only a starting order: clicking any heading still re-sorts the grid, including this column,
     * where the first click turns the order round rather than setting it again.
     */
    private String sortColumn;
    /** Grid controls only: ASC or DESC, and only read when {@link #sortColumn} names a column. */
    private String sortDirection = "ASC";
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
    /**
     * Tabs controls only: which of {@link #tabControlIds} is the tab showing when the page opens,
     * and the one the strip marks as the default. Blank — every tab set saved before this existed —
     * opens on the first tab, as it always did. Only meaningful once a set holds more than one grid.
     */
    private String defaultTabControlId;
    /**
     * Pie-with-grids controls only: the tab set the chart puts a grid per pie into, holding the rows
     * the chart was drawn from. Clicking a slice opens that pie's tab, filtered to the slice.
     */
    private String tabsControlId;
    /**
     * Bar chart controls only: {@code VERTICAL} — columns rising from a baseline, the default — or
     * {@code HORIZONTAL}, bars running across from a left-hand baseline.
     */
    private String orientation = "VERTICAL";

    public String getDefaultTabControlId()                         { return defaultTabControlId; }
    public void   setDefaultTabControlId(String defaultTabControlId) {
        this.defaultTabControlId = defaultTabControlId == null || defaultTabControlId.isBlank() ? null : defaultTabControlId.trim();
    }

    public String getTabsControlId()                     { return tabsControlId; }
    public void   setTabsControlId(String tabsControlId) {
        this.tabsControlId = tabsControlId == null || tabsControlId.isBlank() ? null : tabsControlId.trim();
    }

    public String getOrientation()                   { return orientation; }
    /** Anything but an explicit HORIZONTAL is vertical. */
    public void   setOrientation(String orientation) {
        this.orientation = orientation != null && "HORIZONTAL".equalsIgnoreCase(orientation.trim()) ? "HORIZONTAL" : "VERTICAL";
    }

    public String getControlId()                   { return controlId; }
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

    public String getTextFormat()          { return textFormat; }
    /** Anything but JSON or XML is NONE, so an unset or unreadable value leaves the text alone. */
    public void   setTextFormat(String textFormat) {
        String f = textFormat == null ? "" : textFormat.trim().toUpperCase();
        this.textFormat = f.equals("JSON") || f.equals("XML") ? f : "NONE";
    }

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

    public String getDatasetName()                     { return datasetName; }
    public void   setDatasetName(String datasetName)   { this.datasetName = datasetName == null || datasetName.isBlank() ? null : datasetName.trim(); }

    public String getLinkPageName()                       { return linkPageName; }
    public void   setLinkPageName(String linkPageName)    { this.linkPageName = linkPageName == null || linkPageName.isBlank() ? null : linkPageName.trim(); }

    public String getChildPageName()                       { return childPageName; }
    public void   setChildPageName(String childPageName)   { this.childPageName = childPageName == null || childPageName.isBlank() ? null : childPageName.trim(); }

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

    public String getRowErrorExpression()   { return rowErrorExpression; }
    public void   setRowErrorExpression(String rowErrorExpression) {
        this.rowErrorExpression = rowErrorExpression == null || rowErrorExpression.isBlank()
                ? null : rowErrorExpression.trim();
    }

    public String getStatusCondition()   { return statusCondition; }
    /** Spaces are dropped and case ignored, so "rowcount > 0" is stored as ROWCOUNT>0. */
    public void   setStatusCondition(String statusCondition) {
        String c = statusCondition == null ? "" : statusCondition.replaceAll("\\s+", "").toUpperCase();
        this.statusCondition = c.isEmpty() ? null : c;
    }

    public String getStatusVariable()    { return statusVariable; }
    public void   setStatusVariable(String statusVariable) {
        this.statusVariable = statusVariable == null || statusVariable.isBlank() ? null : statusVariable.trim();
    }

    public String getSortColumn()                     { return sortColumn; }
    public void   setSortColumn(String sortColumn)    { this.sortColumn = sortColumn == null || sortColumn.isBlank() ? null : sortColumn.trim(); }

    public String getSortDirection()                  { return sortDirection; }
    /** Anything but an explicit DESC is ascending, so an unset or unreadable value still sorts. */
    public void   setSortDirection(String sortDirection) {
        this.sortDirection = sortDirection != null && "DESC".equalsIgnoreCase(sortDirection.trim()) ? "DESC" : "ASC";
    }

    public List<AppPageOption> getSlices()                   { return slices; }
    public void setSlices(List<AppPageOption> slices)        { this.slices = slices != null ? slices : new ArrayList<>(); }

    public List<String> getTabControlIds()                      { return tabControlIds; }
    public void setTabControlIds(List<String> tabControlIds)    { this.tabControlIds = tabControlIds != null ? tabControlIds : new ArrayList<>(); }
}
