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
 *
 * <p>{@link #transforms} is the same idea for reshaping: named steps — JSONata expressions and
 * XML-to-JSON conversions — that any action can chain, in whatever order it needs, to rework a
 * response before it lands in a control.
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
    /**
     * Named reshaping steps actions may chain over what they bound. Held on the page rather than on
     * the action for the same reason {@link #actions} is: one reshaping of one endpoint's response
     * is one step, however many actions want it — and a step written for one chain (turning a SOAP
     * body into JSON, say) is the same step the next chain starts with.
     */
    private List<AppPageTransform> transforms = new ArrayList<>();
    /**
     * Named values the page's templates may read beside its controls — see {@link AppPageVariable}.
     * Held on the page for the same reason {@link #actions} is: a value every action on the screen
     * carries is one fact about the page, and writing it into a hidden control per page and a
     * placeholder per action is the same fact written many times over.
     *
     * <p>These are the ad-hoc ones alone. The built-ins — the machine, the date, the time, a fresh
     * UUID, the row number inside a fan-out — are worked out when a trigger runs and are never
     * stored, so they are not here and may not be redefined here either.
     */
    private List<AppPageVariable> variables = new ArrayList<>();
    /**
     * Whether this page keeps the detail behind the calls it makes — the request and response bodies
     * and what the server made of them. Read in a template as <code>${DEBUG}</code>, and the one
     * built-in that is stored rather than computed, because it is a decision about the page rather
     * than a fact about the run: see {@link AppPageVariable#BUILT_IN}.
     *
     * <p>On, which is what a page gets until somebody says otherwise, every call a trigger makes is
     * kept whole — that is what fills Technical Details and what lets a body be pulled back out of
     * the server afterwards. Off, the call still goes out and its answer is still bound into the
     * grid exactly as before; what stops is the <em>keeping</em>. The browser holds the metadata of
     * each call and drops the bodies, and the server is told not to file the run away for later
     * retrieval. Nothing about the page's behaviour changes, only what it costs to have run it —
     * which is the trade worth making where a page runs all day against production and nobody is
     * going to read the sixth-last response.
     *
     * <p>Defaults to true rather than false so a page saved before this switch existed opens
     * behaving as it always did: the detail is there until it is turned off deliberately.
     */
    private boolean debug = true;

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

    public List<AppPageTransform> getTransforms()                    { return transforms; }
    public void setTransforms(List<AppPageTransform> transforms)     { this.transforms = transforms != null ? transforms : new ArrayList<>(); }

    public List<AppPageVariable> getVariables()                      { return variables; }
    public void setVariables(List<AppPageVariable> variables)        { this.variables = variables != null ? variables : new ArrayList<>(); }

    public boolean isDebug()                                         { return debug; }
    public void setDebug(boolean debug)                              { this.debug = debug; }
}
