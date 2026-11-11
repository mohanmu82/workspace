package com.mycompany.batch.appcatalog;

/**
 * One variable a use case expects the caller to supply per instance.
 *
 * @param name         variable name, referenced in the URL/headers/body as {@code $name} or {@code ${name}}
 * @param type         string, number, boolean, json or xml — controls how the value is coerced before substitution
 * @param defaultValue optional pre-filled value shown when a new instance is created
 * @param required     a run fails unless the default, the instance or the caller supplies a value
 */
public record AppUseCaseInput(String name, String type, String defaultValue, boolean required) {

    public AppUseCaseInput {
        type = (type == null || type.isBlank()) ? "string" : type.trim().toLowerCase();
    }

    public AppUseCaseInput(String name, String type, String defaultValue) {
        this(name, type, defaultValue, false);
    }
}
