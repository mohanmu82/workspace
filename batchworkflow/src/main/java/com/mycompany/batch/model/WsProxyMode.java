package com.mycompany.batch.model;

/**
 * REFLECT  — build the envelope and hand it straight back to the caller, no outbound connection.
 * PROXY    — forward the envelope to one target, chosen by round robin among the currently
 *            connected targets, and relay its response back.
 * BROADCAST — forward the envelope to every configured target and relay all responses back
 *             as an array.
 */
public enum WsProxyMode {
    REFLECT, PROXY, BROADCAST
}
