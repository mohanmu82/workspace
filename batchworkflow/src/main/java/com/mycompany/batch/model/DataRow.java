package com.mycompany.batch.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents one unit of work flowing through the batch pipeline.
 *
 * <ul>
 *   <li>{@code data} — input columns + extraction results; always included in the REST response.</li>
 *   <li>{@code metadata} — per-activity timing and URL; included only when {@code debugMode=1}.</li>
 *   <li>{@code responseBody} — HTTP response body passed from an HTTP activity to the following
 *       extraction activity; never exposed in the REST response.</li>
 *   <li>{@code expandedRows} — populated by a JSON extraction activity that produces more than one
 *       output row per input row; the outer pipeline expands them into separate result entries.</li>
 * </ul>
 */
public class DataRow {

    private final Map<String, Object> data     = new LinkedHashMap<>();
    private final Map<String, Object> metadata = new LinkedHashMap<>();
    private String responseBody;
    private List<Map<String, Object>> expandedRows; // non-null only for multi-row JSON extraction

    public DataRow() {}

    public DataRow(Map<String, Object> initialData) {
        this.data.putAll(initialData);
    }

    public Map<String, Object> getData()     { return data; }
    public Map<String, Object> getMetadata() { return metadata; }

    public String getResponseBody()               { return responseBody; }
    public void   setResponseBody(String body)    { this.responseBody = body; }

    public List<Map<String, Object>> getExpandedRows()                      { return expandedRows; }
    public void                      setExpandedRows(List<Map<String, Object>> rows) { this.expandedRows = rows; }

    /**
     * Returns a response-ready copy of this row's data.
     * When {@code includeMetadata} is {@code true} the activity timing/URL fields
     * from {@link #getMetadata()} are merged in after the data fields.
     */
    public Map<String, Object> toResponseMap(boolean includeMetadata) {
        Map<String, Object> result = new LinkedHashMap<>(data);
        if (includeMetadata) result.putAll(metadata);
        return result;
    }
}
