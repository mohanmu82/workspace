package com.mycompany.batch.model;

import java.util.List;
import java.util.Map;

/**
 * Unified request DTO used by both the REST JSON-body endpoint and the WebSocket handler.
 *
 * <pre>
 * {
 *   "operation"      : "pubmed",
 *   "alias"          : "TECH",
 *   "inputSource"    : "FILE | REQUEST | HTTPCONFIG",
 *   "inputFilePath"  : "/path/to/ids.csv",
 *   "inputHttpUrl"   : "http://some-api/ids",
 *   "ids"            : ["123", "456"],
 *   "raw"            : [{"id":"123","year":"2024"}, {"id":"456","year":"2024"}],
 *   "inputCount"     : 100,
 *   "outputData"     : "HTTP | FILE",
 *   "outputFilePath" : "/path/to/output.psv",
 *   "debugMode"      : 0,
 *   "httpThreadCount": 10,
 *   "httpTimeoutMs"  : 3000,
 *   "filterInput"    : [{"column":"id","value":"08550","operation":"eq"}],
 *   "filterOutput"   : [{"column":"status","value":"ACTIVE","operation":"like"}]
 * }
 * </pre>
 *
 * <p>For {@code inputSource=REQUEST}, the UI can send either:
 * <ul>
 *   <li>{@code ids} — a flat list of string identifiers (each becomes a DataRow with key {@code "id"})</li>
 *   <li>{@code raw} — a list of pre-parsed row maps (sent when the textarea content is a multi-column
 *       CSV; each map becomes a DataRow directly)</li>
 * </ul>
 * When both are present {@code raw} takes precedence.
 *
 * <p>{@code debugMode} values:
 * <ul>
 *   <li>0 — normal execution</li>
 *   <li>-1 — skip HTTP and extraction; return input DataRows as-is</li>
 *   <li>1 — normal execution + include per-activity timing/URL metadata in each result row</li>
 * </ul>
 *
 * <p>{@code filterInput} — optional list of {@link FilterRule}s applied to DataRows
 * <em>before</em> activity execution. Rows that do not match are dropped.
 *
 * <p>{@code filterOutput} — optional list of {@link FilterRule}s applied to result rows
 * <em>after</em> activity execution. Rows that do not match are excluded from the response.
 */
public record RunRequest(
        String operation,
        String inputSource,
        String inputFilePath,
        /**
         * Mandatory when {@code inputSource=HTTPCONFIG}: the URL that returns the input rows
         * as a JSON object containing a {@code "data"} array. Overrides any URL configured in
         * {@code operations.json} for that operation.
         */
        String inputHttpUrl,
        List<String> ids,
        /** Pre-parsed rows sent from the REQUEST textarea (multi-column CSV). Takes precedence over {@code ids}. */
        List<Map<String, Object>> raw,
        Integer inputCount,
        String outputData,
        String outputFilePath,
        Integer debugMode,
        /** Per-run override for HTTP thread pool size. Uses operation default when null. */
        Integer httpThreadCount,
        /** Per-run override for HTTP request timeout in milliseconds. Uses operation default when null. */
        Integer httpTimeoutMs,
        /** Rows not matching all rules are dropped before activity execution. */
        List<FilterRule> filterInput,
        /** Rows not matching all rules are excluded from the response after activity execution. */
        List<FilterRule> filterOutput,
        /**
         * "SYNC" (default) or "ASYNC".
         * ASYNC: the WebSocket handler sends an immediate ACK then streams each result row
         * individually as it completes, followed by a final "done" metadata message.
         * Only meaningful when requestMode=WS; ignored for REST requests.
         */
        String executionMode,
        /**
         * Optional name of a pre-configured request preset defined in {@code operations.json}
         * under {@code alias[].name}. When supplied, the alias's {@code request} fields are
         * used as defaults for any fields not explicitly set in this request.
         * Incoming request fields always take precedence over alias values.
         */
        String alias,
        String responseProcessor,
        /** When {@code outputData=FILE}, append to the file instead of overwriting. Default is overwrite. */
        Boolean appendOutput,
        /** Path to a JSON file used when {@code inputSource=JSON}. Content returned as-is (no data-array unwrapping). */
        String inputJsonPath,
        /** Optional inline key-value pairs merged into operation properties (highest priority). */
        Map<String, String> properties) {
}
