package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum InputSourceType {
    FILE, REQUEST, HTTP, HTTPGET, HTTPPOST, HTTPCONFIG, JSON, ALIAS, HTTPLOCAL, CACHE, SPECIFIC, TEMPLATE;

    @JsonCreator
    public static InputSourceType from(String value) {
        if (value == null || value.isBlank()) return null;
        for (InputSourceType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) return t;
        }
        throw new IllegalArgumentException("Unknown inputSource type: '" + value + "'");
    }
}
