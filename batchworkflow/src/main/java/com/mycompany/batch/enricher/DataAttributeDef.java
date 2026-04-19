package com.mycompany.batch.enricher;

/**
 * Defines one derived attribute added to a DataRow by the enricher.
 *
 * <p>Supported {@code type} values:
 * <ul>
 *   <li>{@code replace}  — string interpolation: {@code "${attr1} in ${attr2}"}</li>
 *   <li>{@code sum}      — numeric addition:      {@code "${attr1}+${attr2}"}</li>
 *   <li>{@code divide}   — numeric division:      {@code "${attr1}/${attr2}"}</li>
 *   <li>{@code lookup}   — dataset lookup:        requires {@code key}, {@code dataset}, {@code attribute}</li>
 * </ul>
 *
 * <p>{@code onError} — value written when evaluation fails; supports {@code ${...}} references.
 * Omit to leave the attribute absent on error.
 */
public class DataAttributeDef {

    private String name;
    private String type;
    /** Expression string for replace / sum / divide / filecontents / springexl. Supports {@code ${...}} placeholders. */
    private String value;
    /** (lookup only) Expression that resolves to the key value to look up. */
    private String key;
    /** (lookup only) Expression that resolves to the dataset name to search in. */
    private String dataset;
    /** (lookup only) Expression that resolves to the column name to retrieve from the matched row. */
    private String attribute;
    /** (tokenize / base64decode) Input expression. Supports {@code ${...}} placeholders. */
    private String string;
    /** (tokenize only) Delimiter to split on. */
    private String delimiter;
    /** (tokenize only) 1-based index of the token to return. */
    private String token;
    /** Value written when evaluation fails. Supports {@code ${...}} placeholders. Null = skip attribute on error. */
    private String onError;

    public String getName()               { return name; }
    public void   setName(String v)       { this.name = v; }

    public String getType()               { return type; }
    public void   setType(String v)       { this.type = v; }

    public String getValue()              { return value; }
    public void   setValue(String v)      { this.value = v; }

    public String getKey()                { return key; }
    public void   setKey(String v)        { this.key = v; }

    public String getDataset()            { return dataset; }
    public void   setDataset(String v)    { this.dataset = v; }

    public String getAttribute()          { return attribute; }
    public void   setAttribute(String v)  { this.attribute = v; }

    public String getString()             { return string; }
    public void   setString(String v)     { this.string = v; }

    public String getDelimiter()          { return delimiter; }
    public void   setDelimiter(String v)  { this.delimiter = v; }

    public String getToken()              { return token; }
    public void   setToken(String v)      { this.token = v; }

    public String getOnError()            { return onError; }
    public void   setOnError(String v)    { this.onError = v; }
}
