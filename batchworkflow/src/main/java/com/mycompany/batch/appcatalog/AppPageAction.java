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
 * <p>{@link #actionKind} can also take the action out of that shape entirely: a {@link #PERFORMANCE}
 * action summarises this server's own run history and makes no call, and a {@link #COMPARE} action
 * makes two — one instance against two environments — and fills a grid with what differs.
 *
 * <p>{@link #rowSourceControlId} turns all of the above into a fan-out: instead of running once
 * against the page's controls, the action runs once per row of a grid already on the page, with each
 * row's own columns answering the {@code ${name}} placeholders. See {@link #ROWS} and {@link #TABS}
 * for where the answers land — collected, they fill one grid or one dropdown; a tab apiece, they
 * fill a tab set.
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

    /**
     * {@link #actionKind}: run one use case instance twice — once against each of two environments —
     * and fill a grid with what differs between the two answers.
     *
     * <p>It is the question every release asks and nothing here answered: <em>does UAT still say
     * what production says?</em> Until now that meant running the instance twice by hand, saving two
     * responses and diffing them in something else, which loses the inputs, the environments and the
     * moment as soon as the files are closed. This makes it a button: the same instance, the same
     * inputs, two environments, one table of differences.
     *
     * <p>What is compared is not necessarily the whole response. {@link #comparePath} narrows it to
     * a field, and the action's {@link #transformNames} run over each side first, so two endpoints
     * that answer with the same facts in different wrappers can still be held against each other.
     * {@link #compareType} says how each body is read — JSON as it stands, XML converted first —
     * and {@link #compareTolerancePercent} says how far two numbers may drift before the difference
     * is worth reporting.
     *
     * <p>Order is never a difference. Two arrays holding the same elements in a different sequence
     * compare equal, because an endpoint that returns its rows in whatever order the database handed
     * them is not a regression, and a diff that says all two hundred rows changed is a diff nobody
     * reads twice.
     *
     * <p>Its answer is a table, so like {@link #PERFORMANCE} it targets a grid and nothing else.
     */
    public static final String COMPARE = "COMPARE";

    /**
     * {@link #actionKind}: fill a control from the rows of a static dataset instead of from a
     * response — the reference lists this server already holds, put on a page behind a button.
     *
     * <p>A great deal of what a page needs to show is not an endpoint's answer at all. The desks,
     * the regions, the books, the services being watched, the accounts in scope for a release — all
     * of it is already maintained as a static dataset, and until now a page could only reach one in
     * two fixed ways: a grid wired straight to a dataset, which fills itself as the page opens and
     * shows the whole of it, or a dropdown sourced from one, which does the same for its options.
     * Neither is triggered by anything and neither can be narrowed, so a page wanting "the desks in
     * the region the operator just picked" had to wrap the list in a use case and call it over HTTP
     * to ask a question this server could already answer out of what it holds.
     *
     * <p>This is that question as an ordinary action: it runs when something is clicked, like any
     * other, and it narrows. {@link #datasetFilters} are conditions written against the dataset's
     * own columns whose values may be {@code ${field}} templates, so a grid under a region dropdown
     * reloads to that region; {@link #datasetFavorite} names a filter already saved on the dataset,
     * so a question the library has already been taught is asked by name rather than copied. Both
     * may be given, and are then AND-ed.
     *
     * <p>The filtering happens in the dataset library rather than here, so a page pulling twelve
     * rows out of a dataset of fifty thousand is handed twelve. Like {@link #PERFORMANCE} it calls
     * nothing outward and names no instance, so there is no environment to send it to, no response
     * for a path to be read out of, and nothing to transform: its answer is already rows.
     *
     * <p>Rows are what most of a page holds, so unlike the other special kinds this one is not
     * confined to a grid — a select or a chart takes a list as readily, under the same
     * {@link #keyField}, {@link #labelField} and {@link #valueField} that name the parts of a row
     * everywhere else.
     */
    public static final String DATASET = "DATASET";

    /** {@link #compareType}: each response is read as JSON — the default. */
    public static final String COMPARE_JSON = "JSON";

    /**
     * {@link #compareType}: each response is parsed as an XML document and compared as the tree it
     * describes — elements as keys, a repeated tag as a list, attributes under {@code @attributes}.
     * Attribute order, whitespace between elements and the order sibling elements appear in are all
     * outside that tree, so none of them can show up as a difference.
     */
    public static final String COMPARE_XML = "XML";

    /** {@link #compareReport}: only what differs by more than the threshold — the default. */
    public static final String REPORT_DIFFERENCES = "DIFFERENCES";

    /**
     * {@link #compareReport}: what differs, and also what differed by less than the threshold — so
     * the report can be read as "these six moved, and four of them moved within tolerance" rather
     * than leaving the tolerated ones indistinguishable from the identical ones.
     */
    public static final String REPORT_TOLERATED = "TOLERATED";

    /** {@link #compareReport}: every field compared, matching or not — the full side-by-side. */
    public static final String REPORT_EVERYTHING = "EVERYTHING";

    /** {@link #source}: bind from the response body, which is what an action has always done. */
    public static final String PAYLOAD = "PAYLOAD";

    /**
     * {@link #rowOutputMode}: every row's answer lands in one control, a row of it per call — a grid,
     * so a hundred calls read as a hundred-row table that sorts, filters and exports like any other,
     * or a select or multi-select, so those same rows become the options of one dropdown under
     * {@link #keyField} and {@link #labelField}. Which of the two is the target's to say.
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
    /** {@link #DATASET} only — which static dataset's rows this action binds. */
    private String datasetName;
    /**
     * {@link #DATASET} only — the name of a filter saved on that dataset, whose conditions are
     * applied before the rows come back. Blank asks for the dataset unfiltered, or for
     * {@link #datasetFilters} alone.
     */
    private String datasetFavorite;
    /**
     * {@link #DATASET} only — conditions written on the page itself, AND-ed with each other and with
     * whatever {@link #datasetFavorite} carries. Empty asks for every row the favourite left.
     */
    private List<AppPageDatasetFilter> datasetFilters = new ArrayList<>();
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
    /**
     * Which element fields become a select's value and text; ignored for a grid target. Read the
     * same way where a fan-out collects its answers into a select: each collected row is an option,
     * and these name the two columns of it that matter.
     */
    private String keyField;
    private String labelField;
    /**
     * Grid and {@link #NEW_GRID} targets only. Normally the value at {@link #arrayPath} must be an
     * array; setting this shows a JSON object there as a two-column key/value grid instead of
     * failing the action.
     */
    private boolean keyValueGrid;
    /**
     * Grid and {@link #NEW_GRID} targets reading an object: leave out the properties that are
     * themselves objects or arrays, rather than putting each one into a cell as the JSON text it is.
     *
     * <p>The cell a nested object makes is unreadable and unsortable — a line of braces in a column
     * sized for a status — and on the records these grids are usually pointed at it is most of what
     * is on screen: the twenty scalars worth reading, buried among six sub-documents nobody opened
     * the grid for. Off, which is every action saved before this existed, every property is a row
     * exactly as it always was.
     */
    private boolean scalarsOnly;
    /**
     * Placed grid targets only: add what this binds to the rows the grid already holds rather than
     * replacing them, so a button clicked once per order — or an action run again over a different
     * environment — builds one table out of several answers instead of showing only the last.
     *
     * <p>Nothing to {@link #NEW_GRID}, which already keeps every run: it draws another grid per run,
     * which is this same idea as a stack rather than as one table. Off, which is every action saved
     * before this existed, each run replaces the grid's rows exactly as it always did.
     */
    private boolean appendRows;
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

    /**
     * Further places the one call's answer is bound, after the target written on the action itself —
     * see {@link AppPageBinding}. Each reads the same response its own way, with its own source,
     * transforms, target and paths, so a single call can fill a grid, a select, a pie chart and a text
     * area together. Empty — the default, and every action saved before this existed — binds only
     * the action's own target.
     *
     * <p>Only an ordinary action run once has them. A fan-out has one answer per row and a
     * performance action has a table and no call; neither has a single response to read twice.
     */
    private List<AppPageBinding> extraBindings = new ArrayList<>();

    /**
     * Columns added to every row of the grid this action fills, as it fills it — a field of the call
     * record, a response header, a lookup into a static dataset or into another grid on the page, and
     * a regex over one column making another. See {@link AppPageEnrichColumn}.
     *
     * <p>Only a grid has rows to add them to, so they are taken only where the target is a grid, a
     * {@link #NEW_GRID}, or — for a fan-out giving each row a tab — the tab set whose grids those are.
     * A fan-out collecting into one grid enriches each call's rows with that call's own record and
     * headers. Empty — the default — leaves the rows exactly as they were bound.
     */
    private List<AppPageEnrichColumn> enrichColumns = new ArrayList<>();

    /**
     * A group-by applied to the rows before they fill the grid — see {@link AppPagePivot}. Null, the
     * default and every action saved before this existed, fills the grid with the rows as bound.
     *
     * <p>Only a grid has rows to group, so it is taken where the target is a grid or a
     * {@link #NEW_GRID}, and on a fan-out collecting its answers into one grid; a tab per row has a
     * grid per call and no one table to group.
     */
    private AppPagePivot pivot;

    /**
     * {@link #COMPARE} only: the environment the comparison treats as the baseline — the side a
     * difference is reported as being <em>from</em>. Same {@code ${field}} template grammar as
     * {@link #environmentOverride}, and usually pointed at a select sourced from the app's
     * environments, so the operator picks the pair.
     */
    private String compareEnvironmentA;

    /** {@link #COMPARE} only: the environment held against the baseline. See {@link #compareEnvironmentA}. */
    private String compareEnvironmentB;

    /** {@link #COMPARE} only: {@link #COMPARE_JSON} or {@link #COMPARE_XML}; anything else reads as JSON. */
    private String compareType = COMPARE_JSON;

    /**
     * {@link #COMPARE} only: what inside each response is compared. Blank, or {@code $}, compares
     * the response whole; anything else is the same path grammar {@link #arrayPath} uses, read out
     * of each side after that side has been through {@link #transformNames} — so a comparison can be
     * narrowed to one field, or widened back to the reshaped document a JSONata step produced.
     */
    private String comparePath;

    /**
     * {@link #COMPARE} only: how far two numbers may differ, as a percentage of the larger of them,
     * before the difference is reported. 0 — the default — reports any difference at all.
     *
     * <p>Relative rather than absolute, and measured against the larger side rather than the
     * baseline, so the same threshold means the same thing on a price and on a notional and reads
     * the same whichever environment happens to be the bigger number. It applies to numbers only:
     * two strings are equal or they are not, and a "0.1% different" identifier is a different
     * identifier.
     */
    private double compareTolerancePercent;

    /**
     * {@link #COMPARE} only: how much of the comparison ends up in the grid — {@link #REPORT_DIFFERENCES},
     * {@link #REPORT_TOLERATED} or {@link #REPORT_EVERYTHING}. Anything unrecognised reads as differences only.
     */
    private String compareReport = REPORT_DIFFERENCES;

    public String getCompareEnvironmentA()                              { return compareEnvironmentA; }
    public void   setCompareEnvironmentA(String compareEnvironmentA)    { this.compareEnvironmentA = compareEnvironmentA; }

    public String getCompareEnvironmentB()                              { return compareEnvironmentB; }
    public void   setCompareEnvironmentB(String compareEnvironmentB)    { this.compareEnvironmentB = compareEnvironmentB; }

    public String getCompareType()                    { return compareType; }
    public void   setCompareType(String compareType)  { this.compareType = COMPARE_XML.equalsIgnoreCase(compareType) ? COMPARE_XML : COMPARE_JSON; }

    public String getComparePath()                    { return comparePath; }
    public void   setComparePath(String comparePath)  { this.comparePath = comparePath; }

    public double getCompareTolerancePercent()        { return compareTolerancePercent; }
    /** A negative threshold would accept nothing and mean nothing, so it reads as no threshold at all. */
    public void   setCompareTolerancePercent(double compareTolerancePercent) {
        this.compareTolerancePercent = compareTolerancePercent > 0 ? compareTolerancePercent : 0;
    }

    public String getCompareReport()                      { return compareReport; }
    public void   setCompareReport(String compareReport)  {
        this.compareReport = REPORT_EVERYTHING.equalsIgnoreCase(compareReport) ? REPORT_EVERYTHING
                : REPORT_TOLERATED.equalsIgnoreCase(compareReport) ? REPORT_TOLERATED
                : REPORT_DIFFERENCES;
    }

    /** Whether this action compares one instance across two environments rather than calling one. */
    public boolean isCompare() { return COMPARE.equals(actionKind); }

    /** Whether this action reads each response as an XML document rather than as JSON. */
    public boolean isCompareXml() { return COMPARE_XML.equals(compareType); }

    public AppPagePivot getPivot()              { return pivot; }
    public void setPivot(AppPagePivot pivot)    { this.pivot = pivot; }

    /** Whether the rows this action binds are grouped before they reach the grid. */
    public boolean hasPivot() { return pivot != null && pivot.groupsAnything(); }

    public List<AppPageBinding> getExtraBindings()                   { return extraBindings; }
    public void setExtraBindings(List<AppPageBinding> extraBindings) { this.extraBindings = extraBindings != null ? extraBindings : new ArrayList<>(); }

    public List<AppPageEnrichColumn> getEnrichColumns()                        { return enrichColumns; }
    public void setEnrichColumns(List<AppPageEnrichColumn> enrichColumns)      { this.enrichColumns = enrichColumns != null ? enrichColumns : new ArrayList<>(); }

    public String getActionId()                   { return actionId; }
    public void   setActionId(String actionId)     { this.actionId = actionId == null || actionId.isBlank() ? null : actionId.trim(); }

    public String getActionLabel()                     { return actionLabel; }
    public void   setActionLabel(String actionLabel)   { this.actionLabel = actionLabel; }

    public String getActionKind()                    { return actionKind; }
    /** Anything unrecognised reads as {@link #USECASE}, which is what a page saved without one is. */
    public void   setActionKind(String actionKind)   {
        this.actionKind = PERFORMANCE.equalsIgnoreCase(actionKind) ? PERFORMANCE
                : COMPARE.equalsIgnoreCase(actionKind) ? COMPARE
                : DATASET.equalsIgnoreCase(actionKind) ? DATASET
                : USECASE;
    }

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

    public boolean isScalarsOnly()                     { return scalarsOnly; }
    public void    setScalarsOnly(boolean scalarsOnly) { this.scalarsOnly = scalarsOnly; }

    public boolean isAppendRows()                    { return appendRows; }
    public void    setAppendRows(boolean appendRows) { this.appendRows = appendRows; }

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
     * Columns added to the grid a collected fan-out fills, beside the ones its answers bring — see
     * {@link AppPageResultColumn}. Empty, and the grid is what it has always been: a leading
     * {@code source} column and then whatever fields each answer happened to carry.
     *
     * <p>Only {@link #ROWS} has one grid to lay out this way. Under {@link #TABS} every call has a
     * grid of its own holding that call's whole answer, which is a different question with a
     * different answer already: the target grid's own columns.
     */
    private List<AppPageResultColumn> rowColumns = new ArrayList<>();

    /**
     * Narrows what is bound, after it has been read and before it fills the control — see
     * {@link AppPageRowFilter}. Every filter has to pass for a row to be kept.
     *
     * <p>A grid and a dropdown both take a list, and neither always wants all of it. The endpoint
     * that answers with every order is the grid of the open ones; the instance list that answers with
     * every environment is the dropdown of the production ones. Without this the choices were to
     * write a JSONata transform for each such reading, or to bind the lot and leave the operator to
     * type into the grid's filter row every time — and the second is not open to a dropdown at all,
     * which has no filter row to type into.
     *
     * <p>It is the same test, written the same way, as the filters a fan-out narrows its rows with,
     * and it runs at the same point in the reading: after the transforms and the path, after the
     * enriched columns are added — so a filter may test a column that came off the call record
     * rather than out of the body — and before a group-by, so what is grouped is what passed. Values
     * are templates, and one resolving to nothing drops its filter rather than matching nothing, so
     * a filter written against a dropdown the operator has not touched means <em>any</em>.
     *
     * <p>Only a grid, a new grid or a select reads these: the other targets take one value rather
     * than a list of rows, and a filter on one of those is refused rather than saved as a test
     * nothing would ever make.
     */
    private List<AppPageRowFilter> bindFilters = new ArrayList<>();

    public List<AppPageRowFilter> getRowFilters()                        { return rowFilters; }
    public void setRowFilters(List<AppPageRowFilter> rowFilters)         { this.rowFilters = rowFilters != null ? rowFilters : new ArrayList<>(); }

    public List<AppPageRowFilter> getBindFilters()                       { return bindFilters; }
    public void setBindFilters(List<AppPageRowFilter> bindFilters)       { this.bindFilters = bindFilters != null ? bindFilters : new ArrayList<>(); }

    public List<AppPageResultColumn> getRowColumns()                     { return rowColumns; }
    public void setRowColumns(List<AppPageResultColumn> rowColumns)      { this.rowColumns = rowColumns != null ? rowColumns : new ArrayList<>(); }

    /**
     * {@link #TABS} only: the error check every tab's grid puts its rows through as it fills — the
     * same grammar as {@link AppPageControl#getRowErrorExpression()}, see {@link AppPageRowCheck}. A
     * fanned-out tab is no control and has nowhere else to carry one. A tab whose call worked but
     * holds a row the check calls true reads ERROR rather than SUCCESS. Blank checks nothing.
     */
    private String rowErrorExpression;

    public String getRowErrorExpression()   { return rowErrorExpression; }
    public void   setRowErrorExpression(String rowErrorExpression) {
        this.rowErrorExpression = rowErrorExpression == null || rowErrorExpression.isBlank()
                ? null : rowErrorExpression.trim();
    }

    /**
     * {@link #TABS} only: the rows of each tab's response its grid keeps — those the expression comes
     * out true for, in the same grammar as {@link #rowErrorExpression}. Blank keeps every row.
     */
    private String displayFilterExpression;

    public String getDisplayFilterExpression()   { return displayFilterExpression; }
    public void   setDisplayFilterExpression(String displayFilterExpression) {
        this.displayFilterExpression = displayFilterExpression == null || displayFilterExpression.isBlank()
                ? null : displayFilterExpression.trim();
    }

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
     * or dropdown this action targets, a row per call; {@link #TABS} gives each call a grid of its own inside
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

    /** {@link #rowElements}: only the options the operator has picked. */
    public static final String ELEMENTS_SELECTED = "SELECTED";
    /** {@link #rowElements}: every option in the list, picked or not. */
    public static final String ELEMENTS_ALL = "ALL";

    /**
     * Where the fan-out's rows come from a select or a multi-select rather than a grid: which of its
     * elements it runs over. Blank takes the control's own default — the picks on a multi-select,
     * every option on a select, since a select only ever has one pick. Ignored for a grid.
     */
    private String rowElements;

    public String getRowElements()                   { return rowElements; }
    public void   setRowElements(String rowElements) {
        String e = rowElements == null ? "" : rowElements.trim().toUpperCase();
        this.rowElements = ELEMENTS_SELECTED.equals(e) || ELEMENTS_ALL.equals(e) ? e : null;
    }

    public String getDependsOnActionId()             { return dependsOnActionId; }
    public void   setDependsOnActionId(String dependsOnActionId) {
        this.dependsOnActionId = dependsOnActionId == null || dependsOnActionId.isBlank()
                ? null : dependsOnActionId.trim();
    }

    /** Whether this action fills a grid with performance figures instead of running an instance. */
    public boolean isPerformance() { return PERFORMANCE.equals(actionKind); }

    /** Whether this action binds a static dataset's rows instead of running an instance. */
    public boolean isDataset() { return DATASET.equals(actionKind); }

    public String getDatasetName()                     { return datasetName; }
    public void   setDatasetName(String datasetName)   {
        this.datasetName = datasetName == null || datasetName.isBlank() ? null : datasetName.trim();
    }

    public String getDatasetFavorite()                       { return datasetFavorite; }
    public void   setDatasetFavorite(String datasetFavorite) {
        this.datasetFavorite = datasetFavorite == null || datasetFavorite.isBlank() ? null : datasetFavorite.trim();
    }

    public List<AppPageDatasetFilter> getDatasetFilters()    { return datasetFilters; }
    public void setDatasetFilters(List<AppPageDatasetFilter> datasetFilters) {
        this.datasetFilters = datasetFilters != null ? datasetFilters : new ArrayList<>();
    }

    /** Whether this action reads the call's metadata rather than its response body. */
    public boolean isMetadata()   { return METADATA.equals(source); }
}
