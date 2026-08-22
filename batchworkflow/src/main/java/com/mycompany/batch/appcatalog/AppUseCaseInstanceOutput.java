package com.mycompany.batch.appcatalog;

import java.util.List;
import java.util.Map;

/**
 * The record of one instance execution. Deliberately flat and self-describing: a group run
 * returns an array of these straight into the results grid, where every field is available as a
 * Group By / drill-down dimension.
 *
 * <p>The bodies are the only fields that can be arbitrarily large, so they are not shipped with
 * the grid — see {@link #withoutPayload()}. The full record stays on the server under
 * {@link #executionId} and is fetched per row on demand.
 *
 * @param startedAt   epoch milliseconds the call left — the ordering key for the Global Runs page,
 *                    which shows executions from every app together and has no other way to say
 *                    which of two runs came first
 * @param status      SUCCESS, FAILED (HTTP status 400 or above) or ERROR (nothing came back at all)
 * @param statusCode  the HTTP response code exactly as the endpoint returned it (200, 404, 503…),
 *                    or null when the call never got far enough to receive one
 * @param runIndex    1-based position within this instance's run count, so the repeats of a
 *                    regression sweep stay distinguishable in the grid
 * @param runCount    how many repeats this execution was one of — 1 for an ordinary single run
 * @param executedVia {@code LOCAL} when this server made the call, or {@code AGENT:<agentId>} when a
 *                    remote agent did
 * @param jwtToken    the raw bearer token JWT auth obtained for this run — the same value the
 *                    templates see as {@code $jwtToken}. Null for every other auth method. Reported
 *                    so a token can be lifted straight out of a run and replayed by hand; it is a
 *                    live credential, so the page shows it masked until asked.
 * @param unresolvedVariables
 *                    the placeholders substitution left in {@link #url} because no variable
 *                    answered to their name — every one of them went on the wire verbatim. Empty
 *                    when the URL resolved cleanly, and <em>null</em> when substitution never ran
 *                    at all, in which case {@link #url} is the configured template rather than a
 *                    real endpoint. The distinction matters: a template full of placeholders after
 *                    an auth failure says nothing about whether the inputs were supplied.
 */
public record AppUseCaseInstanceOutput(
        String executionId,
        long startedAt,
        String appUseCaseInstanceId,
        String instanceLabel,
        String appName,
        String environment,
        String envClass,
        String useCase,
        Map<String, Object> appUseCaseInstanceInputs,
        String status,
        Integer statusCode,
        int runIndex,
        int runCount,
        long timeTaken,
        String url,
        List<String> unresolvedVariables,
        String httpMethod,
        String executedVia,
        String jwtToken,
        String requestBody,
        String responseBody,
        Map<String, String> requestHeaders,
        Map<String, List<String>> responseHeaders,
        Object transformedResponse,
        String error,
        Integer requestBodySize,
        Integer responseBodySize) {

    /**
     * The same result with the bodies dropped, keeping only their sizes. This is what a run returns
     * to the browser: a multi-megabyte response body rendered into the results grid freezes the
     * page, and most rows are never expanded.
     *
     * <p>The headers stay. They are bounded — a few hundred bytes even on a chatty endpoint — and
     * they are the first thing an operator looks at when a call comes back wrong, so making them
     * cost a second round trip only hid them. The error message is kept for the same reason.
     */
    public AppUseCaseInstanceOutput withoutPayload() {
        return new AppUseCaseInstanceOutput(
                executionId, startedAt, appUseCaseInstanceId, instanceLabel, appName, environment, envClass, useCase,
                appUseCaseInstanceInputs, status, statusCode, runIndex, runCount, timeTaken, url, unresolvedVariables,
                httpMethod, executedVia, jwtToken,
                null, null, requestHeaders, responseHeaders, null, error,
                requestBody == null ? 0 : requestBody.length(),
                responseBody == null ? 0 : responseBody.length());
    }
}
