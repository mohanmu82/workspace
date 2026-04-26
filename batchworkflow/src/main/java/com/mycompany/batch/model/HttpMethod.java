package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum HttpMethod {
    GET, POST, PUT, DELETE, PATCH, HEAD;

    @JsonCreator
    public static HttpMethod from(String value) {
        if (value == null || value.isBlank()) return GET;
        for (HttpMethod m : values()) {
            if (m.name().equalsIgnoreCase(value.trim())) return m;
        }
        throw new IllegalArgumentException("Unknown HTTP method: '" + value
                + "'. Valid values: GET, POST, PUT, DELETE, PATCH, HEAD");
    }
}
