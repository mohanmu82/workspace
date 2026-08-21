package com.mycompany.batch.appcatalog;

/**
 * One variable a use case expects the caller to supply per instance.
 *
 * @param name         variable name, referenced in the URL/headers/body as {@code $name} or {@code ${name}}
 * @param type         string, number, boolean or json — controls how the value is coerced before substitution
 * @param defaultValue optional pre-filled value shown when a new instance is created
 */
public record AppUseCaseInput(String name, String type, String defaultValue) {

    public AppUseCaseInput {
        type = (type == null || type.isBlank()) ? "string" : type.trim().toLowerCase();
    }
}
