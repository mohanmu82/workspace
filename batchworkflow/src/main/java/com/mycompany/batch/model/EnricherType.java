package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum EnricherType {
    PRE, POST;

    @JsonCreator
    public static EnricherType from(String value) {
        if (value == null || value.isBlank()) return POST;
        for (EnricherType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown enricher type: '" + value
                + "'. Valid values: PRE, POST");
    }
}
