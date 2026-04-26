package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum OutputDataType {
    HTTP, FILE;

    @JsonCreator
    public static OutputDataType from(String value) {
        if (value == null || value.isBlank()) return null;
        for (OutputDataType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown outputData type: '" + value
                + "'. Valid values: HTTP, FILE");
    }
}
