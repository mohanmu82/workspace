package com.mycompany.batch.appcatalog;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One step of a button's click: run a use case instance with values taken off the page, then put
 * what comes back somewhere on the page.
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
 */
public class AppPageAction {

    /**
     * Target that means "don't reuse a control" — every run adds another grid to the page, newest
     * on top, so successive clicks can be compared against each other instead of overwriting.
     */
    public static final String NEW_GRID = "__new_grid__";

    private String actionLabel;
    private String appUseCaseInstanceId;
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

    public String getActionLabel()                     { return actionLabel; }
    public void   setActionLabel(String actionLabel)   { this.actionLabel = actionLabel; }

    public String getAppUseCaseInstanceId()                              { return appUseCaseInstanceId; }
    public void   setAppUseCaseInstanceId(String appUseCaseInstanceId)   { this.appUseCaseInstanceId = appUseCaseInstanceId; }

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
}
