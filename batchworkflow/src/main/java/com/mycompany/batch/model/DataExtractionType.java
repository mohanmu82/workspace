package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum DataExtractionType {
    XPATH, JSON, JSONATA, JSONPATH;

    @JsonCreator
    public static DataExtractionType from(String value) {
        if (value == null || value.isBlank()) return null;
        for (DataExtractionType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown dataExtraction type: '" + value
                + "'. Valid values: XPATH, JSON, JSONATA, JSONPATH");
    }
}
