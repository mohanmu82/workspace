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
 *
 * <p>{@link #sortOrder} is asked of every mode alike, because it is a question about the list the
 * operator reads rather than about where the list came from: a dropdown of two hundred desks is
 * unusable in whatever order the endpoint happened to return them, and the fix should not depend on
 * whether they arrived from a use case, a dataset or a typed-in list. {@link #selectFirst} is asked
 * of every mode for the same reason — whether the dropdown opens on something or on the blank row is
 * a question about the list, not about where it came from.
 *
 * <p>The {@code DATASET} mode reads {@link #datasetName}'s rows out of the static dataset library
 * and takes {@link #keyField}/{@link #labelField} off each, exactly as {@code USECASE} does — the
 * difference is only where the rows come from. A list of desks, books or regions that is already
 * maintained as a dataset is then a dropdown without an endpoint standing in front of it.
 */
public class AppPageOptionSource {

    /** NONE, STATIC, USECASE, ENVIRONMENTS or DATASET. */
    private String mode = "NONE";
    private List<AppPageOption> staticOptions = new ArrayList<>();
    private String appUseCaseInstanceId;
    /** Dotted path to the array inside the response; blank when the response is the array. */
    private String arrayPath;
    private String keyField;
    private String labelField;
    /** ENVIRONMENTS mode only — which app's environments to list. */
    private String appName;
    /** DATASET mode only — which static dataset's rows back this dropdown. */
    private String datasetName;
    /**
     * How the options are ordered once they have been gathered: {@code NONE} — which is every page
     * saved before this existed — leaves them in the order they arrived, while {@code ASC} and
     * {@code DESC} sort them by the value the operator reads.
     *
     * <p>By the shown value rather than by the key, because the order is for the person scanning the
     * list and the key is routinely an id they never see. Numbers compare as numbers, so a list of
     * amounts does not run 1, 10, 2.
     */
    private String sortOrder = "NONE";
    /**
     * Whether the dropdown opens on its first option instead of on the empty row — asked of every
     * mode alike, for the same reason {@link #sortOrder} is: it is a question about the list the
     * operator reads rather than about where the list came from.
     *
     * <p>Off by default, which is every page saved before this existed: a select opens on the blank
     * row and the operator picks. On, a list that came back with anything in it is opened on the
     * first of them, so a dropdown with one sensible answer does not have to be pointed at before
     * the page can be used. The first option is the first <em>after</em> sorting, since that is the
     * one at the top of the list the operator reads.
     *
     * <p>It is a default rather than a lock: the operator may pick something else, and it never
     * wins over a value that is already there — one carried in on a link, or the pick that survived
     * the list being refreshed under it — because a value somebody chose beats one nobody did.
     */
    private boolean selectFirst;

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

    public String getDatasetName()                     { return datasetName; }
    public void   setDatasetName(String datasetName)   { this.datasetName = datasetName; }

    public boolean isSelectFirst()                      { return selectFirst; }
    public void    setSelectFirst(boolean selectFirst)  { this.selectFirst = selectFirst; }

    public String getSortOrder()                 { return sortOrder; }
    /** Anything but an explicit ASC or DESC is "leave them as they came". */
    public void   setSortOrder(String sortOrder) {
        String wanted = sortOrder == null ? "" : sortOrder.trim().toUpperCase();
        this.sortOrder = "ASC".equals(wanted) || "DESC".equals(wanted) ? wanted : "NONE";
    }
}

