package com.mycompany.batch.model;

/**
 * One row in the in-memory WebSocket proxy request/response log.
 * Public fields are serialized as-is by Jackson (default field visibility is PUBLIC_ONLY).
 */
public class WsProxyLogEntry {
    public String uuid;
    public long   timestamp;      // request time, epoch millis
    public int    port;
    public String mode;
    public String source;
    public String target;
    public Object requestPayload;
    public Object responsePayload;
    public String status;
    public String message;
    public Long   timeTakenMs;    // null if no response was ever obtained
}
