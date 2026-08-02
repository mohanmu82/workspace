package com.mycompany.batch.dashboardview;

import java.util.ArrayList;
import java.util.List;

/**
 * A named, saved combination of group-by drill-down levels, swimlane orientation, and the
 * active filter (search text + prod-only) on the Service Dashboard's Services grid. Persisted
 * server-side (not localStorage) so a view set up by one operator — e.g. drilled down by
 * region then environment, prod-only, horizontal swimlanes — is reusable by anyone loading
 * the dashboard, across browsers/machines.
 */
public class DashboardView {

    private String name;
    /** Which page this view belongs to — lets other pages reuse the same store without colliding. */
    private String page = "serviceDashboard";
    /** Ordered compound group-by keys, e.g. ["__status", "attr:region"] — same shape as the
     *  dashboard's groupByKeys array, applied in order to form swimlane keys. */
    private List<String> groupByKeys = new ArrayList<>();
    /** "vertical" (cards stacked top-to-bottom within a lane) or "horizontal" (side-scrolling lane). */
    private String orientation = "vertical";
    private String search = "";
    private boolean prodOnly = true;

    public String getName()                       { return name; }
    public void   setName(String name)            { this.name = name; }

    public String getPage()                       { return page; }
    public void   setPage(String page)             { this.page = page != null ? page : "serviceDashboard"; }

    public List<String> getGroupByKeys()                        { return groupByKeys; }
    public void          setGroupByKeys(List<String> groupByKeys) { this.groupByKeys = groupByKeys != null ? groupByKeys : new ArrayList<>(); }

    public String getOrientation()                { return orientation; }
    public void   setOrientation(String orientation) { this.orientation = orientation != null ? orientation : "vertical"; }

    public String getSearch()                      { return search; }
    public void   setSearch(String search)          { this.search = search != null ? search : ""; }

    public boolean isProdOnly()                     { return prodOnly; }
    public void    setProdOnly(boolean prodOnly)    { this.prodOnly = prodOnly; }
}
