package com.mycompany.batch.model;

/**
 * Optional JSONata transform applied to the response JSON just before returning to the client.
 * Exactly one of {@code key} or {@code value} should be set:
 * <ul>
 *   <li>{@code key}    — loads the expression from {@code classpath:transforms/{key}.jsonata}</li>
 *   <li>{@code value}  — uses the string directly as the JSONata expression</li>
 *   <li>{@code source} — optional JSONata path evaluated against the full response to extract the
 *       input for the transform (e.g. {@code "data[0].RESPONSEBODY"}). When the extracted value is
 *       a JSON string it is parsed before the transform runs. Defaults to the full response object.</li>
 * </ul>
 */
public record JsonataTransform(String key, String value, String source) {}
