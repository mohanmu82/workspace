package com.mycompany.batch.enricher;

/**
 * Describes how to load a lookup dataset — either from a delimited file or an HTTP endpoint.
 * Supports optional caching via {@link DatasetCacheDef}.
 */
public class DataSourceDef {

    /** {@code FILE}, {@code HTTPCONFIG}, or {@code CACHE}. */
    private String          inputSource  = "FILE";
    /** Path to a delimited (comma or pipe) file. Supports {@code ${DATESTAMP}}. */
    private String          inputFilePath;
    /** URL whose JSON response contains a {@code "data"} array. Supports {@code ${DATESTAMP}}. */
    private String          inputHttpUrl;
    /** Name of the CacheFactory cache to read rows from when {@code inputSource=CACHE}. */
    private String          cacheName;
    /** Optional cache — when present the loaded dataset is stored/retrieved from CacheFactory. */
    private DatasetCacheDef cache;

    public String          getInputSource()                    { return inputSource; }
    public void            setInputSource(String s)            { this.inputSource = s; }

    public String          getInputFilePath()                  { return inputFilePath; }
    public void            setInputFilePath(String p)          { this.inputFilePath = p; }

    public String          getInputHttpUrl()                   { return inputHttpUrl; }
    public void            setInputHttpUrl(String u)           { this.inputHttpUrl = u; }

    public String          getCacheName()                      { return cacheName; }
    public void            setCacheName(String c)              { this.cacheName = c; }

    public DatasetCacheDef getCache()                          { return cache; }
    public void            setCache(DatasetCacheDef c)         { this.cache = c; }
}
