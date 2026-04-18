package com.mycompany.batch.enricher;

/**
 * Defines a named lookup dataset loaded once per enricher execution.
 * The resolved dataset is keyed by the value of the {@code key} column,
 * making per-row lookups O(1).
 *
 * <pre>
 * {
 *   "name"      : "FXRATES.${DATESTAMP}",
 *   "key"       : "currency",
 *   "dataSource": { "inputSource":"FILE", "inputFilePath":"C:/data/fx_${DATESTAMP}.txt" }
 * }
 * </pre>
 *
 * {@code ${DATESTAMP}} in {@code name}, {@code dataSource.inputFilePath} and
 * {@code dataSource.inputHttpUrl} is replaced with today's date (yyyyMMdd) at load time.
 */
public class DatasetDef {

    private String        name;
    private String        key;
    private DataSourceDef dataSource = new DataSourceDef();

    public String        getName()               { return name; }
    public void          setName(String name)    { this.name = name; }

    public String        getKey()                { return key; }
    public void          setKey(String key)      { this.key = key; }

    public DataSourceDef getDataSource()                       { return dataSource; }
    public void          setDataSource(DataSourceDef ds)       { this.dataSource = ds; }
}
