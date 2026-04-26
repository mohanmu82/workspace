package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ActivityType {
    HTTP, DATAEXTRACTION, DB;

    @JsonCreator
    public static ActivityType from(String value) {
        if (value == null || value.isBlank()) return null;
        for (ActivityType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown activity type: '" + value
                + "'. Valid values: HTTP, DATAEXTRACTION, DB");
    }
}
