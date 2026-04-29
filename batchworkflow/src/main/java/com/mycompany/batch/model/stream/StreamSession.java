package com.mycompany.batch.model.stream;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public class StreamSession {

    private final String sessionId;
    private final String streamUri;
    private final String createdBy;
    private final Instant createdAt;
    private final SseEmitter emitter;
    private final ConcurrentHashMap<String, DatasetSubscription> datasets = new ConcurrentHashMap<>();
    private volatile String status = "active";

    public StreamSession(String sessionId, String streamUri, String createdBy,
                         Instant createdAt, SseEmitter emitter) {
        this.sessionId = sessionId;
        this.streamUri = streamUri;
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.emitter   = emitter;
    }

    public String      getSessionId()     { return sessionId; }
    public String      getStreamUri()     { return streamUri; }
    public String      getCreatedBy()     { return createdBy; }
    public Instant     getCreatedAt()     { return createdAt; }
    public SseEmitter  getEmitter()       { return emitter; }
    public String      getStatus()        { return status; }
    public void        setStatus(String v){ this.status = v; }

    public ConcurrentHashMap<String, DatasetSubscription> getDatasets() { return datasets; }
}
