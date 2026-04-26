package com.mycompany.batch.model;

/**
 * Instructs the batch engine to write each result row into a named cache entry.
 *
 * <ul>
 *   <li>{@code name} — CacheFactory cache name to write into</li>
 *   <li>{@code key}  — column name whose value is used as the cache entry key</li>
 * </ul>
 */
public class CacheOutput {

    private String name;
    private String key;

    public String getName()             { return name; }
    public void   setName(String name)  { this.name = name; }

    public String getKey()              { return key; }
    public void   setKey(String key)    { this.key = key; }
}
