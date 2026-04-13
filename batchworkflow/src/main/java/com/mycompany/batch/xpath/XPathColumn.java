package com.mycompany.batch.xpath;

public class XPathColumn {

    private String columnName;
    private String xpath;
    private String displayName;
    private String columnType;

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getXpath() { return xpath; }
    public void setXpath(String xpath) { this.xpath = xpath; }

    /** Returns {@code displayName} if set, otherwise falls back to {@code columnName}. */
    public String getDisplayName() {
        return (displayName != null && !displayName.isBlank()) ? displayName : columnName;
    }
    public void setDisplayName(String displayName) { this.displayName = displayName; }

    /** Returns {@code columnType} if set, otherwise defaults to {@code "string"}. */
    public String getColumnType() {
        return (columnType != null && !columnType.isBlank()) ? columnType : "string";
    }
    public void setColumnType(String columnType) { this.columnType = columnType; }
}
