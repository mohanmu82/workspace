package com.mycompany.batch.jsonpath;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates a list of {@link JsonPathColumn} expressions against a JSON response body
 * and returns a {@code column → value} map.
 *
 * <p>Each column expression is a JSONPath (standard {@code $}-prefixed or simple
 * dot-notation auto-normalised to {@code $.path}).  If a path is not found in the
 * document the column is mapped to an empty string rather than throwing.
 */
@Component
public class JsonPathExtractor {

    /** Suppress exceptions for missing paths — return null instead. */
    private static final Configuration CONF = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS);

    public Map<String, String> extract(String responseBody, List<JsonPathColumn> columns) {
        if (responseBody == null || columns == null || columns.isEmpty()) {
            return Map.of();
        }

        Object document = JsonPath.using(CONF).parse(responseBody).json();
        Map<String, String> result = new LinkedHashMap<>();

        for (JsonPathColumn col : columns) {
            if (col.getColumn() == null || col.getColumn().isBlank()) continue;
            try {
                Object value = JsonPath.using(CONF).parse(document).read(col.normalizedPath());
                result.put(col.getColumn(), value != null ? value.toString() : "");
            } catch (PathNotFoundException e) {
                result.put(col.getColumn(), "");
            } catch (Exception e) {
                result.put(col.getColumn(), "ERROR: " + e.getMessage());
            }
        }
        return result;
    }
}
