package com.mycompany.batch.enricher;

/** Cache configuration for a lookup dataset. Mirrors the HTTP activity cache config. */
public class DatasetCacheDef {

    /** Name of the shared cache store (same as used by CacheFactory). */
    private String name = "";
    /** Cache key template — supports {@code ${DATESTAMP}} and row-level {@code ${...}} placeholders. */
    private String key  = "";

    public String getName()            { return name; }
    public void   setName(String name) { this.name = name; }

    public String getKey()             { return key; }
    public void   setKey(String key)   { this.key = key; }
}
