package com.mycompany.batch.appcatalog;

import java.util.LinkedHashMap;
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
 * <p>{@link #targetControlId} names a grid, select, text or text area control on the same page, or
 * the {@link #NEW_GRID} sentinel to stack a freshly built grid above the previous ones instead of
 * reusing a placed control. Blank runs the instance for its effect alone and reports only success
 * or failure.
 *
 * <p>{@link #source} decides what the paths are read out of: the response payload as before, or the
 * call's own metadata — the URL, the status code, how long it took. See {@link #METADATA}.
 */
public class AppPageAction {

    /**
     * Target that means "don't reuse a control" — every run adds another grid to the page, newest
     * on top, so successive clicks can be compared against each other instead of overwriting.
     */
    public static final String NEW_GRID = "__new_grid__";

    /** {@link #source}: bind from the response body, which is what an action has always done. */
    public static final String PAYLOAD = "PAYLOAD";

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
    /** Dotted path to the array inside the response; blank when the response is the array. */
    private String arrayPath;
    /**
     * Path to the single attribute a text or text area target is filled from — {@code data.order.id}
     * or {@code $.items[0].name}. Used instead of {@link #arrayPath}, which only makes sense for a
     * target that shows many rows.
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

    public String getActionId()                    { return actionId; }
    public void   setActionId(String actionId)     { this.actionId = actionId == null || actionId.isBlank() ? null : actionId.trim(); }

    public String getActionLabel()                     { return actionLabel; }
    public void   setActionLabel(String actionLabel)   { this.actionLabel = actionLabel; }

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

    /** Whether this action reads the call's metadata rather than its response body. */
    public boolean isMetadata()   { return METADATA.equals(source); }
}
