package com.mycompany.batch.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum AuthMethod {
    NONE, BASIC, JWT, DIGEST, KERBEROS;

    @JsonCreator
    public static AuthMethod from(String value) {
        if (value == null || value.isBlank()) return NONE;
        for (AuthMethod m : values()) {
            if (m.name().equalsIgnoreCase(value.trim())) return m;
        }
        throw new IllegalArgumentException("Unknown auth method: '" + value
                + "'. Valid values: NONE, BASIC, JWT, DIGEST, KERBEROS");
    }
}
