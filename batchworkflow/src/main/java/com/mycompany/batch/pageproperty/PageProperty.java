package com.mycompany.batch.pageproperty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Admin-editable key/value settings for one page — e.g. on websocketproxy.html, key
 * {@code defaultPort} overrides the listen port the "Start a Proxy" form pre-fills. Pages fall
 * back to their own hardcoded default when no entry (or key) exists here.
 *
 * <p>Persisted server-side as one JSON array in {@code pageproperties.json}, mirroring
 * {@link com.mycompany.batch.pagepreference.PagePreference}'s save pattern. Editable directly
 * from the Admin page.
 */
public class PageProperty {

    /** Stable page id, e.g. "websocketproxy". */
    private String page;
    /** Setting key -> value, both page-defined; values are always stored as strings. */
    private Map<String, String> properties = new LinkedHashMap<>();

    public String getPage()             { return page; }
    public void   setPage(String page)  { this.page = page; }

    public Map<String, String> getProperties()                       { return properties; }
    public void                setProperties(Map<String, String> properties) { this.properties = properties != null ? properties : new LinkedHashMap<>(); }
}
