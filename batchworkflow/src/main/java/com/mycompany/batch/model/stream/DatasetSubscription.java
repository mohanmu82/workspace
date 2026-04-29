package com.mycompany.batch.model.stream;

import java.time.Instant;

public class DatasetSubscription {

    private final String      datasetAlias;
    private final String      requestId;
    private final String      requestedBy;
    private final Instant     requestedAt;
    private final int         timeToLive;
    private final Instant     expiresAt;
    private final String      url;
    private final String      destination;
    private final String      streamType;
    private       String      reloadUrl;
    private       LiveKeyInfo liveKey;
    private volatile String   status = "active";

    public DatasetSubscription(String datasetAlias, String requestId, String requestedBy,
                               Instant requestedAt, int timeToLive, Instant expiresAt,
                               String url, String destination, String streamType,
                               String reloadUrl, LiveKeyInfo liveKey) {
        this.datasetAlias = datasetAlias;
        this.requestId    = requestId;
        this.requestedBy  = requestedBy;
        this.requestedAt  = requestedAt;
        this.timeToLive   = timeToLive;
        this.expiresAt    = expiresAt;
        this.url          = url;
        this.destination  = destination;
        this.streamType   = streamType;
        this.reloadUrl    = reloadUrl;
        this.liveKey      = liveKey;
    }

    public String      getDatasetAlias()             { return datasetAlias; }
    public String      getRequestId()                { return requestId; }
    public String      getRequestedBy()              { return requestedBy; }
    public Instant     getRequestedAt()              { return requestedAt; }
    public int         getTimeToLive()               { return timeToLive; }
    public Instant     getExpiresAt()                { return expiresAt; }
    public String      getUrl()                      { return url; }
    public String      getDestination()              { return destination; }
    public String      getStreamType()               { return streamType; }

    public String      getReloadUrl()                { return reloadUrl; }
    public void        setReloadUrl(String v)        { this.reloadUrl = v; }

    public LiveKeyInfo getLiveKey()                  { return liveKey; }
    public void        setLiveKey(LiveKeyInfo v)     { this.liveKey = v; }

    public String      getStatus()                   { return status; }
    public void        setStatus(String v)           { this.status = v; }
}
