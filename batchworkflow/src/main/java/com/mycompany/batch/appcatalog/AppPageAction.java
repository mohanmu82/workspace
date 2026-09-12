package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One step of a trigger: run a use case instance with values taken off the page, then put what comes
 * back somewhere on the page.
 *
 * <p>An action lives either inline on the control that runs it, or — since page-level actions — in
 * {@link AppPage#getActions()}, where it is named by {@link #actionId} and can be attached to any
 * number of controls at once (see {@link AppPageControl#getActionIds()}). The two forms behave
 * identically once they run; the library form simply lets one action be reused rather than copied.
 *
 * <p>{@link #inputs} maps a use case input name to a template resolved against the page's control
 * values — {@code ${orderId}} picks up the control whose field name is {@code orderId}, and a
 * literal is passed through as written. Whatever the map does not name keeps the value the instance
 * was saved with, so a page only has to supply what it actually varies.
 *
 * <p>{@link #targetControlId} names a grid, select, text, text area, link or pie chart control on
 * the same page,
 * or the {@link #NEW_GRID} sentinel to stack a freshly built grid above the previous ones instead of
 * reusing a placed control. Blank runs the instance for its effect alone and reports only success
 * or failure. A link takes the address it points at rather than text to show, which is what makes a
 * metadata action binding {@code url} worth having: the operator clicks through to the endpoint the
 * call actually went to.
 *
 * <p>{@link #source} decides what the paths are read out of: the response payload as before, or the
 * call's own metadata — the URL, the status code, how long it took. See {@link #METADATA}.
 *
 * <p>{@link #rowSourceControlId} turns all of the above into a fan-out: instead of running once
 * against the page's controls, the action runs once per row of a grid already on the page, with each
 * row's own columns answering the {@code ${name}} placeholders. See {@link #ROWS} and {@link #TABS}
 * for where the answers land.
 *
 * <p>{@link #transformNames} optionally reshapes that root with the page's named transforms first,
 * in the order given — XML into JSON, then JSONata over the result, then more JSONata if that is
 * what it takes — so an endpoint whose response two actions read differently is written once per
 * reading rather than being flattened by hand in each path.
 */
public class AppPageAction {

    /**
     * Target that means "don't reuse a control" — every run adds another grid to the page, newest
     * on top, so successive clicks can be compared against each other instead of overwriting.
     */
    public static final String NEW_GRID = "__new_grid__";

    /**
     * {@link #actionKind}: run a use case instance and bind what comes back — every action before
     * the special kinds existed, and every action that names an endpoint.
     */
    public static final String USECASE = "USECASE";

    /**
     * {@link #actionKind}: fill a grid with how this server's endpoints have been performing —
     * a row per app / environment / use case, carrying {@code app}, {@code environment},
     * {@code useCase}, {@code requestCount}, {@code avgTimeTaken} and {@code maxTimeTaken}.
     *
     * <p>It runs no instance, which is what makes it special: the numbers are summarised out of the
     * run history this server already holds, so the action calls nothing outward and needs no
     * endpoint configured. What it does take is the same {@code ${field}} filters an ordinary
     * action's inputs use — {@code appName}, {@code environment} and {@code useCase} — so a page can
     * put a performance grid under an app picker and have it narrow as the operator chooses.
     *
     * <p>Its answers are rows, so it targets a grid or {@link #NEW_GRID} and nothing else; a text
     * box has no room for a table and a link has no address in one.
     */
    public static final String PERFORMANCE = "PERFORMANCE";

    /** {@link #source}: bind from the response body, which is what an action has always done. */
    public static final String PAYLOAD = "PAYLOAD";

    /**
     * {@link #rowOutputMode}: every row's answer lands in one grid, a row of it per call, so a
     * hundred calls read as a hundred-row table that sorts, filters and exports like any other.
     */
    public static final String ROWS = "ROWS";

    /**
     * {@link #rowOutputMode}: every row's answer gets a grid of its own inside a tab set, named
     * after the row that produced it — for when each answer is itself a table and stacking them
     * into one would lose which rows came from where.
     */
    public static final String TABS = "TABS";

    /**
     * {@link #source}: bind from the execution's metadata instead of its body — {@code url},
     * {@code statusCode}, {@code status}, {@code httpMethod}, {@code timeTaken}, {@code appName},
     * {@code environment}, {@code envClass}, {@code useCase}, {@code executionId},
     * {@code requestSize}, {@code responseSize}, {@code executedVia}, {@code startedAt} and the
     * rest of the flat record the run produced.
     *
     * <p>Two things follow from picking this, and both are the point of it. The response body is
     * never shipped to the browser at all, so a metadata action over a huge response costs nothing
     * to run; and a call that failed still binds, because a 500 and the URL that produced it are
     * exactly what someone reads metadata for. The action chain still stops after it, as it would
     * for any failure.
     */
    public static final String METADATA = "METADATA";

    /** Stable id, unique within the page — how a control names a page-level action it triggers. */
    private String actionId;
    private String actionLabel;
    /**
     * What this action does when it runs: {@link #USECASE} — the default, and what every action
     * saved before this existed is — or one of the special kinds, which produce their answers from
     * what this server already knows rather than by calling an endpoint. See {@link #PERFORMANCE}.
     */
    private String actionKind = USECASE;
    private String appUseCaseInstanceId;
    /**
     * Overrides which of the instance's environments this run calls — the same {@code ${field}}
     * template grammar as {@link #inputs}. Typically points at a select control sourced from
     * {@code ENVIRONMENTS}, so the operator's pick on the page decides where the call goes instead
     * of the instance's own default. Blank (or a placeholder nothing on the page answers to) leaves
     * the instance's configured environment untouched.
     */
    private String environmentOverride;
    private Map<String, String> inputs = new LinkedHashMap<>();
    private String targetControlId;
    /**
     * Path to the array inside the response; blank when the response is the array. Segments are
     * separated by {@code .} or {@code /}, may index ({@code items[0]}) or filter
     * ({@code data[id="A-7"]}), and may carry {@code ${fieldName}} placeholders that pick up the
     * page's own control values — {@code data[id="${auctionId}"]/referenceEntities} reads the row the
     * operator selected. Reading a property off an array maps over it, as JSONata does.
     */
    private String arrayPath;
    /**
     * Path to the single attribute a text, text area or link target is filled from —
     * {@code data.order.id} or {@code $.items[0].name}. Used instead of {@link #arrayPath}, which
     * only makes sense for a target that shows many rows.
     *
     * <p>For a link this is the address it will point at. Only {@code http}, {@code https} and a
     * path rooted on this server are taken; anything else is reported on the page and the link is
     * left without an address, since an address bound out of a response is data and a
     * {@code javascript:} one in an href would be that data running as the page.
     */
    private String valuePath;
    /** Which element fields become a select's value and text; ignored for a grid target. */
    private String keyField;
    private String labelField;
    /**
     * Grid and {@link #NEW_GRID} targets only. Normally the value at {@link #arrayPath} must be an
     * array; setting this shows a JSON object there as a two-column key/value grid instead of
     * failing the action.
     */
    private boolean keyValueGrid;
    /** {@link #PAYLOAD} or {@link #METADATA}; anything unrecognised reads as PAYLOAD. */
    private String source = PAYLOAD;
    /**
     * Names the page's {@link AppPageTransform}s to run over what this action bound, in order, before
     * {@link #arrayPath} or {@link #valuePath} is read out of the last one's output. Empty binds the
     * response exactly as it arrived, which is what every action did before transforms existed.
     *
     * <p>A chain rather than a single name because reshaping a response is routinely more than one
     * move: an XML body has to become JSON before any JSONata can touch it, and the expression that
     * flattens the result is a different, separately reusable thing from the one that converted it.
     */
    private List<String> transformNames = new ArrayList<>();
    /**
     * Keeps this action off the per-trigger call cache. Two actions that resolve to the same
     * instance, environment and inputs normally share one HTTP call — the usual reason to write them
     * twice is to process one response two ways. Set this when the call is the point rather than the
     * response: a POST that re-sends a confirmation should go out as many times as it is asked to.
     */
    private boolean ownCall;

    /**
     * Pie chart targets only: which field of each row sizes its wedge, beside {@link #keyField},
     * which names it. Every other kind of target ignores it — a grid shows the row whole, and a
     * select wants a value and a label rather than a number.
     *
     * <p>Blank means the row itself is the number, which is what a bare array of numbers wants;
     * paired with a blank {@link #keyField}, which then falls back to the row's place in the list, a
     * chart can be filled from {@code [4, 9, 2]} with no field names given at all.
     */
    private String valueField;

    /**
     * The action this one waits for, by {@link #actionId}; blank — the default — waits for nothing.
     *
     * <p>A trigger normally sends every action it runs at once and binds them in the order they are
     * listed, so none of them can read what another is about to write. Naming one here holds this
     * action back until that one has come back and bound, and only then are this action's inputs and
     * {@link #environmentOverride} resolved against the page — which is exactly what lets a second
     * call be sent carrying a value the first one has just put there. Actions waiting on the same
     * action still go out together, so a chain costs a round trip per level rather than one per
     * action, and an action waiting for nothing never waits at all.
     *
     * <p>An action written on a control may wait for anything else that control runs: another action
     * on the same control, or a page action it triggers. A page-level action may only wait for
     * another page-level action, since it runs wherever it happens to be attached and one particular
     * control's own action is not there to be waited for from the next control along. Neither may
     * wait for itself, and a circle of actions waiting on each other is refused outright: nothing in
     * one could go first, so nothing in one would ever go at all.
     *
     * <p>If the action waited for fails, or is itself never sent, this one is not sent either and
     * says so where its rows or wedges would have been.
     */
    private String dependsOnActionId;

    public String getActionId()                    { return actionId; }
    public void   setActionId(String actionId)     { this.actionId = actionId == null || actionId.isBlank() ? null : actionId.trim(); }

    public String getActionLabel()                     { return actionLabel; }
    public void   setActionLabel(String actionLabel)   { this.actionLabel = actionLabel; }

    public String getActionKind()                    { return actionKind; }
    /** Anything unrecognised reads as {@link #USECASE}, which is what a page saved without one is. */
    public void   setActionKind(String actionKind)   { this.actionKind = PERFORMANCE.equalsIgnoreCase(actionKind) ? PERFORMANCE : USECASE; }

    public String getAppUseCaseInstanceId()                              { return appUseCaseInstanceId; }
    public void   setAppUseCaseInstanceId(String appUseCaseInstanceId)   { this.appUseCaseInstanceId = appUseCaseInstanceId; }

    public String getEnvironmentOverride()                                  { return environmentOverride; }
    public void   setEnvironmentOverride(String environmentOverride)        { this.environmentOverride = environmentOverride; }

    public Map<String, String> getInputs()                        { return inputs; }
    public void setInputs(Map<String, String> inputs)             { this.inputs = inputs != null ? inputs : new LinkedHashMap<>(); }

    public String getTargetControlId()                             { return targetControlId; }
    public void   setTargetControlId(String targetControlId)       { this.targetControlId = targetControlId; }

    public String getArrayPath()                 { return arrayPath; }
    public void   setArrayPath(String arrayPath) { this.arrayPath = arrayPath; }

    public String getValuePath()                 { return valuePath; }
    public void   setValuePath(String valuePath) { this.valuePath = valuePath; }

    public String getKeyField()                  { return keyField; }
    public void   setKeyField(String keyField)   { this.keyField = keyField; }

    public String getLabelField()                    { return labelField; }
    public void   setLabelField(String labelField)   { this.labelField = labelField; }

    public boolean isKeyValueGrid()                    { return keyValueGrid; }
    public void    setKeyValueGrid(boolean keyValueGrid) { this.keyValueGrid = keyValueGrid; }

    public String getSource()                { return source; }
    public void   setSource(String source)   { this.source = METADATA.equalsIgnoreCase(source) ? METADATA : PAYLOAD; }

    public List<String> getTransformNames()                          { return transformNames; }
    public void setTransformNames(List<String> transformNames) {
        this.transformNames = new ArrayList<>();
        if (transformNames == null) return;
        for (String name : transformNames) {
            if (name != null && !name.isBlank()) this.transformNames.add(name.trim());
        }
    }

    /**
     * Reads the single {@code transformName} pages written before chains existed carried, folding it
     * into {@link #transformNames} as the one step it always was. Deserialize-only — there is no
     * getter, so a page saved from here on carries the list alone and the old key retires with the
     * pages that hold it.
     */
    public void setTransformName(String transformName) {
        if (transformName != null && !transformName.isBlank()) this.transformNames.add(transformName.trim());
    }

    public boolean isOwnCall()                    { return ownCall; }
    public void    setOwnCall(boolean ownCall)    { this.ownCall = ownCall; }

    public String getValueField()                    { return valueField; }
    public void   setValueField(String valueField)   { this.valueField = valueField; }

    public String getRowSourceControlId()          { return rowSourceControlId; }
    public void   setRowSourceControlId(String rowSourceControlId) {
        this.rowSourceControlId = rowSourceControlId == null || rowSourceControlId.isBlank()
                ? null : rowSourceControlId.trim();
    }

    public String getRowOutputMode()                     { return rowOutputMode; }
    public void   setRowOutputMode(String rowOutputMode) { this.rowOutputMode = TABS.equalsIgnoreCase(rowOutputMode) ? TABS : ROWS; }

    /**
     * Narrows the rows this fan-out runs over, beyond the narrowing the operator has already done
     * on the grid itself — see {@link AppPageRowFilter}. Every filter has to pass for a row to be
     * called for. Empty, and ignored entirely, while {@link #rowSourceControlId} is blank.
     */
    private List<AppPageRowFilter> rowFilters = new ArrayList<>();

    /**
     * The columns of the grid a collected fan-out fills — see {@link AppPageResultColumn}. Empty,
     * which is what every fan-out written before this existed is, keeps the old shape: a leading
     * {@code source} column and then whatever fields each answer happened to carry.
     *
     * <p>Only {@link #ROWS} has one grid to lay out this way. Under {@link #TABS} every call has a
     * grid of its own holding that call's whole answer, which is a different question with a
     * different answer already: the target grid's own columns.
     */
    private List<AppPageResultColumn> rowColumns = new ArrayList<>();

    public List<AppPageRowFilter> getRowFilters()                        { return rowFilters; }
    public void setRowFilters(List<AppPageRowFilter> rowFilters)         { this.rowFilters = rowFilters != null ? rowFilters : new ArrayList<>(); }

    public List<AppPageResultColumn> getRowColumns()                     { return rowColumns; }
    public void setRowColumns(List<AppPageResultColumn> rowColumns)      { this.rowColumns = rowColumns != null ? rowColumns : new ArrayList<>(); }

    public String getRowLabelTemplate()                        { return rowLabelTemplate; }
    public void   setRowLabelTemplate(String rowLabelTemplate)  { this.rowLabelTemplate = rowLabelTemplate; }

    public int  getRowLimit()             { return rowLimit; }
    public void setRowLimit(int rowLimit) { this.rowLimit = Math.max(rowLimit, 0); }

    /** Whether this action runs once per row of a grid rather than once against the page. */
    public boolean isRowFanOut()  { return rowSourceControlId != null && !rowSourceControlId.isBlank(); }

    /** Whether a fan-out's answers go to a tab apiece rather than into one grid. */
    public boolean isTabsPerRow() { return isRowFanOut() && TABS.equals(rowOutputMode); }

    /**
     * The grid whose rows drive this action, by {@link AppPageControl#getControlId() control id};
     * blank — the default — runs the action once, as it always did.
     *
     * <p>Set, the action is sent once per row of that grid as it stands on the page, all of them at
     * once rather than one after another, and each call resolves its {@link #inputs}, its
     * {@link #environmentOverride} and its paths against <em>that row</em> first: a template writes
     * {@code ${orderId}} for the row's {@code orderId} column exactly as it writes {@code ${orderId}}
     * for a control of that field name, and falls back to the page's own controls for whatever the
     * row has no column for. Which is the point of it — the grid a lookup filled is usually the list
     * of things the next call has to be made for, one at a time.
     *
     * <p>The rows taken are the ones on screen: a grid the operator has filtered is one they have
     * narrowed on purpose, and calling for the hidden rows anyway would be sending calls nobody
     * asked for. {@link #rowLimit} caps how many go out however the grid was filtered.
     */
    private String rowSourceControlId;

    /**
     * Where a fan-out's answers land: {@link #ROWS} — the default — collects them into the one grid
     * this action targets, a row per call; {@link #TABS} gives each call a grid of its own inside
     * the tab set this action targets. Ignored entirely while {@link #rowSourceControlId} is blank.
     */
    private String rowOutputMode = ROWS;

    /**
     * What each call is called where its answer appears — the tab's name under {@link #TABS}, and
     * the value of the leading {@code source} column under {@link #ROWS}. Resolved against the row
     * that produced it, so {@code ${orderId}} names each tab after its own order. Blank falls back
     * to the row's place in the grid, which at least tells two tabs apart.
     */
    private String rowLabelTemplate;

    /**
     * As many rows as this action will ever fan out over; 0 means no cap of its own. A grid of
     * twenty-seven thousand rows is an ordinary thing for one of these endpoints to return and a
     * catastrophic thing to make twenty-seven thousand calls out of, so the cap is on by default and
     * the running page says how many rows it left alone.
     */
    private int rowLimit = 25;

    public String getDependsOnActionId()             { return dependsOnActionId; }
    public void   setDependsOnActionId(String dependsOnActionId) {
        this.dependsOnActionId = dependsOnActionId == null || dependsOnActionId.isBlank()
                ? null : dependsOnActionId.trim();
    }

    /** Whether this action fills a grid with performance figures instead of running an instance. */
    public boolean isPerformance() { return PERFORMANCE.equals(actionKind); }

    /** Whether this action reads the call's metadata rather than its response body. */
    public boolean isMetadata()   { return METADATA.equals(source); }
}
