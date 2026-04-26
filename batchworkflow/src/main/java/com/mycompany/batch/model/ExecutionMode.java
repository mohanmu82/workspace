package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ExecutionMode {
    SYNC, ASYNC;

    @JsonCreator
    public static ExecutionMode from(String value) {
        if (value == null || value.isBlank()) return null;
        for (ExecutionMode m : values()) {
            if (m.name().equalsIgnoreCase(value.trim())) return m;
        }
        throw new IllegalArgumentException("Unknown executionMode: '" + value
                + "'. Valid values: SYNC, ASYNC");
    }
}
