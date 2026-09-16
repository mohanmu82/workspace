package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * One more place an action puts what its call brought back, beyond the target written on the action
 * itself — see {@link AppPageAction#getExtraBindings()}.
 *
 * <p>The point is one call, many readings. An endpoint that answers with a list of orders is the
 * grid's rows, the status dropdown's options, the pie of how many are in each status and the text
 * box showing the total, all at once; writing four actions against one instance got there only by
 * leaning on the call cache, and made the one call look like four. A binding carries everything that
 * decides how the response is read — where it is read from, how it is reshaped, which path picks
 * out of it and which fields a select or a chart needs — and nothing about how the call is made,
 * which stays with the action.
 *
 * <p>Every field means exactly what the field of the same name on {@link AppPageAction} means, and is
 * read by the same code: at run time a binding is bound as the action with these fields laid over
 * its own.
 */
public class AppPageBinding {

    /** {@link AppPageAction#PAYLOAD} or {@link AppPageAction#METADATA}; anything unrecognised reads as PAYLOAD. */
    private String source = AppPageAction.PAYLOAD;
    /** The page's transforms to run over what this binding reads, in order. */
    private List<String> transformNames = new ArrayList<>();
    /** The grid, select, text, text area, link or pie chart filled, or {@link AppPageAction#NEW_GRID}. */
    private String targetControlId;
    /** Path to the array a grid, select or pie is filled from. */
    private String arrayPath;
    /** Path to the single attribute a text, text area or link is filled from. */
    private String valuePath;
    /** Select: which element field is the option's value. Pie: which names the wedge. */
    private String keyField;
    /** Select: which element field is the option's text. */
    private String labelField;
    /** Pie: which element field sizes the wedge. */
    private String valueField;
    /** Grid targets: show a JSON object as a two-column key/value grid. */
    private boolean keyValueGrid;
    /** Grid targets: leave the object and array properties out — see {@link AppPageAction#isScalarsOnly()}. */
    private boolean scalarsOnly;
    /** Grid targets: columns added to every row as it is bound — see {@link AppPageEnrichColumn}. */
    private List<AppPageEnrichColumn> enrichColumns = new ArrayList<>();

    /** Grid targets: the rows grouped before they fill the grid — see {@link AppPagePivot}. */
    private AppPagePivot pivot;

    public AppPagePivot getPivot()             { return pivot; }
    public void setPivot(AppPagePivot pivot)   { this.pivot = pivot; }

    public List<AppPageEnrichColumn> getEnrichColumns()                   { return enrichColumns; }
    public void setEnrichColumns(List<AppPageEnrichColumn> enrichColumns) { this.enrichColumns = enrichColumns != null ? enrichColumns : new ArrayList<>(); }

    public String getSource()               { return source; }
    public void   setSource(String source)   { this.source = AppPageAction.METADATA.equalsIgnoreCase(source) ? AppPageAction.METADATA : AppPageAction.PAYLOAD; }

    public List<String> getTransformNames()  { return transformNames; }
    public void setTransformNames(List<String> transformNames) {
        this.transformNames = new ArrayList<>();
        if (transformNames == null) return;
        for (String name : transformNames) {
            if (name != null && !name.isBlank()) this.transformNames.add(name.trim());
        }
    }

    public String getTargetControlId()                        { return targetControlId; }
    public void   setTargetControlId(String targetControlId)  { this.targetControlId = targetControlId; }

    public String getArrayPath()                 { return arrayPath; }
    public void   setArrayPath(String arrayPath) { this.arrayPath = arrayPath; }

    public String getValuePath()                 { return valuePath; }
    public void   setValuePath(String valuePath) { this.valuePath = valuePath; }

    public String getKeyField()                  { return keyField; }
    public void   setKeyField(String keyField)   { this.keyField = keyField; }

    public String getLabelField()                    { return labelField; }
    public void   setLabelField(String labelField)   { this.labelField = labelField; }

    public String getValueField()                    { return valueField; }
    public void   setValueField(String valueField)   { this.valueField = valueField; }

    public boolean isKeyValueGrid()                      { return keyValueGrid; }
    public void    setKeyValueGrid(boolean keyValueGrid) { this.keyValueGrid = keyValueGrid; }

    public boolean isScalarsOnly()                     { return scalarsOnly; }
    public void    setScalarsOnly(boolean scalarsOnly) { this.scalarsOnly = scalarsOnly; }
}
