package com.mycompany.batch.pagepreference;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A saved "load from static dataset" field mapping for one page + dataset combination — e.g.
 * on the Service Dashboard loading from the "prod-services" dataset, field {@code url} maps to
 * dataset attribute {@code HTTPURL} and field {@code type} falls back to {@code http}. Lets an
 * operator's mapping choice on {@link com.mycompany.batch.staticdataset.StaticDatasetDef}-backed
 * pages (see {@code staticdataset-widget.js}) survive reloads and be shared across everyone
 * opening that page, instead of re-guessing attribute names on every visit.
 *
 * <p>Persisted server-side (not localStorage) as one JSON array in
 * {@code pagepreferences.json}, mirroring {@link com.mycompany.batch.dashboardview.DashboardView}'s
 * save pattern. Editable directly from the Admin page.
 */
public class PagePreference {

    /** Stable page id, e.g. "serviceDashboard", "httpschecker", "solaceListener". */
    private String page;
    /** Static dataset this mapping applies to — the same page can be mapped differently per dataset. */
    private String datasetName;
    /** Field key (as declared by the page's widget config) -> dataset attribute name. */
    private Map<String, String> mapping = new LinkedHashMap<>();
    /** Field key -> fallback value, for fields that declare a {@code fallback} (e.g. default type). */
    private Map<String, String> fallbacks = new LinkedHashMap<>();

    public String getPage()                    { return page; }
    public void   setPage(String page)         { this.page = page; }

    public String getDatasetName()                  { return datasetName; }
    public void   setDatasetName(String datasetName) { this.datasetName = datasetName; }

    public Map<String, String> getMapping()                       { return mapping; }
    public void                setMapping(Map<String, String> mapping) { this.mapping = mapping != null ? mapping : new LinkedHashMap<>(); }

    public Map<String, String> getFallbacks()                         { return fallbacks; }
    public void                setFallbacks(Map<String, String> fallbacks) { this.fallbacks = fallbacks != null ? fallbacks : new LinkedHashMap<>(); }
}
