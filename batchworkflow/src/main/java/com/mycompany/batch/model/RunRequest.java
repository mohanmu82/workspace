package com.mycompany.batch.model;

import java.util.List;

/**
 * Unified request DTO used by both the REST JSON-body endpoint and the WebSocket handler.
 *
 * <pre>
 * {
 *   "operationType"  : "pubmed",
 *   "inputSource"    : "FILE | HTTPGET | HTTPPOST | HTTPCONFIG",  // overrides operation default
 *   "inputFilePath"  : "/path/to/ids.csv",                        // required for FILE
 *   "ids"            : ["123", "456"],                            // required for HTTPGET / HTTPPOST
 *   "inputCount"     : 100,                                       // optional limit
 *   "outputData"     : "HTTP | FILE",                             // overrides operation default
 *   "outputFilePath" : "/path/to/output.psv"                      // required for FILE output
 * }
 * </pre>
 */
public record RunRequest(
        String operationType,
        String inputSource,
        String inputFilePath,
        List<String> ids,
        Integer inputCount,
        String outputData,
        String outputFilePath,
        /**
         * Debug mode flag.
         * <ul>
         *   <li>0 (default) — normal execution</li>
         *   <li>-1 — skip HTTP calls and data extraction; return the resolved
         *       input identifiers directly in the {@code data} array</li>
         * </ul>
         */
        Integer debugMode) {
}
