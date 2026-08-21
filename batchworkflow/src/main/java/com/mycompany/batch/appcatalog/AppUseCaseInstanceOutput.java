package com.mycompany.batch.appcatalog;

import java.util.List;
import java.util.Map;

/**
 * The record of one instance execution. Deliberately flat and self-describing: a group run
 * returns an array of these straight into the results grid, where every field is available as a
 * Group By / drill-down dimension.
 *
 * <p>The bodies and headers are the only fields that can be arbitrarily large, so they are not
 * shipped with the grid — see {@link #withoutPayload()}. The full record stays on the server under
 * {@link #executionId} and is fetched per row on demand.
 *
 * @param status      SUCCESS, FAILED (HTTP status 400 or above) or ERROR (nothing came back at all)
 * @param executedVia {@code LOCAL} when this server made the call, or {@code AGENT:<agentId>} when a
 *                    remote agent did
 */
public record AppUseCaseInstanceOutput(
        String executionId,
        String appUseCaseInstanceId,
        String instanceLabel,
        String appName,
        String environment,
        String envClass,
        String useCase,
        Map<String, Object> appUseCaseInstanceInputs,
        String status,
        Integer statusCode,
        long timeTaken,
        String url,
        String httpMethod,
        String executedVia,
        String requestBody,
        String responseBody,
        Map<String, String> requestHeaders,
        Map<String, List<String>> responseHeaders,
        Object transformedResponse,
        String error,
        Integer requestBodySize,
        Integer responseBodySize) {

    /**
     * The same result with every payload field dropped, keeping only the sizes. This is what a run
     * returns to the browser: a multi-megabyte response body rendered into the results grid freezes
     * the page, and most rows are never expanded. The error message is kept — it is short, and it is
     * the one payload-ish field worth seeing without a second click.
     */
    public AppUseCaseInstanceOutput withoutPayload() {
        return new AppUseCaseInstanceOutput(
                executionId, appUseCaseInstanceId, instanceLabel, appName, environment, envClass, useCase,
                appUseCaseInstanceInputs, status, statusCode, timeTaken, url, httpMethod, executedVia,
                null, null, null, null, null, error,
                requestBody == null ? 0 : requestBody.length(),
                responseBody == null ? 0 : responseBody.length());
    }
}
