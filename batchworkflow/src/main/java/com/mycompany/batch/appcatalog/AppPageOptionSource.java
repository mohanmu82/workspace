package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * Where a select control gets its options from: nothing at all, a fixed key/value list, the JSON
 * array a use case instance returns, or every environment configured for an app.
 *
 * <p>The {@code USECASE} mode runs the instance when the page opens and reads
 * {@link #arrayPath} out of the transformed response — blank meaning the response is itself the
 * array — then takes {@link #keyField}/{@link #labelField} off each element. Naming the fields
 * rather than assuming key/value means any endpoint's array can back a dropdown without a
 * bespoke transform.
 *
 * <p>The {@code ENVIRONMENTS} mode instead lists {@link #appName}'s environments straight out of
 * the catalog — no instance involved — so a page can offer "which environment" as a dropdown
 * without wiring up a use case just to enumerate them.
 */
public class AppPageOptionSource {

    /** NONE, STATIC, USECASE or ENVIRONMENTS. */
    private String mode = "NONE";
    private List<AppPageOption> staticOptions = new ArrayList<>();
    private String appUseCaseInstanceId;
    /** Dotted path to the array inside the response; blank when the response is the array. */
    private String arrayPath;
    private String keyField;
    private String labelField;
    /** ENVIRONMENTS mode only — which app's environments to list. */
    private String appName;

    public String getMode()             { return mode; }
    public void   setMode(String mode)  { this.mode = mode != null && !mode.isBlank() ? mode : "NONE"; }

    public List<AppPageOption> getStaticOptions()                          { return staticOptions; }
    public void setStaticOptions(List<AppPageOption> staticOptions)        { this.staticOptions = staticOptions != null ? staticOptions : new ArrayList<>(); }

    public String getAppUseCaseInstanceId()                                { return appUseCaseInstanceId; }
    public void   setAppUseCaseInstanceId(String appUseCaseInstanceId)     { this.appUseCaseInstanceId = appUseCaseInstanceId; }

    public String getArrayPath()                 { return arrayPath; }
    public void   setArrayPath(String arrayPath) { this.arrayPath = arrayPath; }

    public String getKeyField()                  { return keyField; }
    public void   setKeyField(String keyField)   { this.keyField = keyField; }

    public String getLabelField()                    { return labelField; }
    public void   setLabelField(String labelField)   { this.labelField = labelField; }

    public String getAppName()                { return appName; }
    public void   setAppName(String appName)  { this.appName = appName; }
}
