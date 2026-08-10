package com.mycompany.batch.staticdataset;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of a named static dataset. All datasets are persisted together as one JSON
 * array in {@code staticdatasets.json} on the classpath and reloaded on startup. Backs
 * pages such as the Service Dashboard, which pulls its target list from a dataset's rows
 * instead of manual entry.
 *
 * <p>{@code source=file} — {@code location} is a delimited (comma or pipe) file path;
 * the header row supplies the attribute names.
 * <p>{@code source=http} — {@code location} is a URL returning JSON; {@code arrayElement}
 * is a JSONPath (default {@code $}) selecting the array of objects, whose keys become
 * the attributes.
 * <p>{@code source=paste} — {@code location} holds tab-separated rows pasted directly
 * from Excel (header row first), stored and reparsed verbatim on reload.
 */
public class StaticDatasetDef {

    private String       name;
    /** {@code file}, {@code http} or {@code paste}. */
    private String       source;
    /** File path (source=file), URL (source=http), or raw pasted TSV text (source=paste). */
    private String       location;
    /** JSONPath into the HTTP response selecting the row array. Ignored for source=file. */
    private String       arrayElement;
    /** Attribute (column) names — discovered from the data on load, persisted for display. */
    private List<String> attributes = new ArrayList<>();
    /** Named, saved filter combinations against this dataset's rows — reusable across pages. */
    private List<FilterFavorite> favorites = new ArrayList<>();

    public String getName()                       { return name; }
    public void   setName(String name)            { this.name = name; }

    public String getSource()                     { return source; }
    public void   setSource(String source)        { this.source = source; }

    public String getLocation()                   { return location; }
    public void   setLocation(String location)     { this.location = location; }

    public String getArrayElement()                { return arrayElement; }
    public void   setArrayElement(String arrayElement) { this.arrayElement = arrayElement; }

    public List<String> getAttributes()                        { return attributes; }
    public void          setAttributes(List<String> attributes) { this.attributes = attributes != null ? attributes : new ArrayList<>(); }

    public List<FilterFavorite> getFavorites()                          { return favorites; }
    public void                 setFavorites(List<FilterFavorite> favorites) { this.favorites = favorites != null ? favorites : new ArrayList<>(); }
}
