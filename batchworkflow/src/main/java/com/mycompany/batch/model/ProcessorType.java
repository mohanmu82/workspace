package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ProcessorType {
    JSONATA, XML2JSON;

    @JsonCreator
    public static ProcessorType from(String value) {
        if (value == null || value.isBlank()) return null;
        for (ProcessorType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown processor type: '" + value
                + "'. Valid values: JSONATA, XML2JSON");
    }
}
