package com.mycompany.batch.jsonpath;

/**
 * One entry in a JSONPATH extraction config file.
 *
 * <pre>
 * [
 *   { "column": "name",        "jsonpath": "root.path.name"           },
 *   { "column": "description", "jsonpath": "root.path.description[0]" }
 * ]
 * </pre>
 *
 * Paths that start with {@code $} are used as-is (standard JSONPath).
 * Paths without {@code $} are automatically prefixed with {@code $.} so that
 * {@code root.path.name} becomes {@code $.root.path.name}.
 */
public class JsonPathColumn {

    private String column;
    private String jsonpath;

    public String getColumn()              { return column; }
    public void   setColumn(String column) { this.column = column; }

    public String getJsonpath()                { return jsonpath; }
    public void   setJsonpath(String jsonpath) { this.jsonpath = jsonpath; }

    /** Returns the path normalised to standard JSONPath notation (prefixed with {@code $.}). */
    public String normalizedPath() {
        if (jsonpath == null || jsonpath.isBlank()) return "$";
        return jsonpath.startsWith("$") ? jsonpath : "$." + jsonpath;
    }
}
