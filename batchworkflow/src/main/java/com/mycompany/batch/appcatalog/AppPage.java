package com.mycompany.batch.appcatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * A screen assembled out of an app's use case instances: inputs the operator fills in, buttons that
 * run instances with those values, and grids and selects the results land in.
 *
 * <p>Not tied to an app: a screen routinely reaches across apps — look an order up in one, then
 * re-send its confirmation from another — so its buttons may wire up any instance in the catalog.
 * Identified by {@link #pageName}, unique across the catalog, which is what the standalone run link
 * addresses.
 *
 * <p>{@link #actions} is the page's library of named actions. An action defined there is attached to
 * as many controls as want it (see {@link AppPageControl#getActionIds()}) instead of being copied
 * onto each, which is what lets one "reload the grid" be triggered by a button, by a dropdown
 * changing, and by the page opening, and stay one thing when it is edited.
 */
public class AppPage {

    private String pageName;
    /**
     * Left over from when pages hung off a single app. Still carried so existing pages round-trip
     * and {@code GET /appcatalog/pages?appName=} can narrow, but nothing requires it any more.
     */
    private String appName;
    /** Heading shown when the page runs; falls back to the page name. */
    private String title;
    private String description;
    private List<AppPageControl> controls = new ArrayList<>();
    /** The page's named actions, each addressable by {@link AppPageAction#getActionId()}. */
    private List<AppPageAction> actions = new ArrayList<>();
    /**
     * Actions run once as soon as the page opens, in this order — the "on load of page" trigger.
     * A page event rather than a control's, so it is held here rather than on a control that would
     * only be standing in for the page.
     */
    private List<String> onLoadActionIds = new ArrayList<>();

    public String getPageName()                  { return pageName; }
    public void   setPageName(String pageName)   { this.pageName = pageName; }

    public String getAppName()                 { return appName; }
    public void   setAppName(String appName)   { this.appName = appName; }

    public String getTitle()              { return title; }
    public void   setTitle(String title)  { this.title = title; }

    public String getDescription()                    { return description; }
    public void   setDescription(String description)  { this.description = description; }

    public List<AppPageControl> getControls()                        { return controls; }
    public void setControls(List<AppPageControl> controls)           { this.controls = controls != null ? controls : new ArrayList<>(); }

    public List<AppPageAction> getActions()                          { return actions; }
    public void setActions(List<AppPageAction> actions)              { this.actions = actions != null ? actions : new ArrayList<>(); }

    public List<String> getOnLoadActionIds()                         { return onLoadActionIds; }
    public void setOnLoadActionIds(List<String> onLoadActionIds)     { this.onLoadActionIds = onLoadActionIds != null ? onLoadActionIds : new ArrayList<>(); }
}
