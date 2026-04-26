package com.mycompany.batch.model;

/**
 * Free-text search filter applied to DataRows after inputCount and before filterInput.
 * A row passes if any column value matches the keyword according to {@code type}.
 *
 * <ul>
 *   <li>{@code contains}   — (default) case-insensitive substring match</li>
 *   <li>{@code startsWith} — case-insensitive prefix match</li>
 *   <li>{@code endsWith}   — case-insensitive suffix match</li>
 *   <li>{@code eq}         — case-insensitive exact match</li>
 *   <li>{@code regex}      — full Java regex match (case-sensitive)</li>
 * </ul>
 */
public class SearchKeyword {

    private String value;
    private String type = "contains";

    public String getValue()              { return value; }
    public void   setValue(String value)  { this.value = value; }

    public String getType()               { return type; }
    public void   setType(String type)    { this.type = type; }
}
