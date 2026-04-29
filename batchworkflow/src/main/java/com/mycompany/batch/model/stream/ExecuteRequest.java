package com.mycompany.batch.model.stream;

public class ExecuteRequest {

    private String requestId;
    private String url;
    /** HTTP method used when the server fetches data from {@code url}. Defaults to GET. */
    private String urlMethod;
    /** Request body forwarded when {@code urlMethod} is POST. */
    private String urlBody;
    private String requestedBy;
    private String requestedAt;
    private int    timeToLive;
    private StreamInfo  stream;
    private DatasetInfo dataset;

    public String      getRequestId()              { return requestId; }
    public void        setRequestId(String v)      { this.requestId = v; }

    public String      getUrl()                    { return url; }
    public void        setUrl(String v)            { this.url = v; }

    public String      getUrlMethod()              { return urlMethod; }
    public void        setUrlMethod(String v)      { this.urlMethod = v; }

    public String      getUrlBody()                { return urlBody; }
    public void        setUrlBody(String v)        { this.urlBody = v; }

    public String      getRequestedBy()            { return requestedBy; }
    public void        setRequestedBy(String v)    { this.requestedBy = v; }

    public String      getRequestedAt()            { return requestedAt; }
    public void        setRequestedAt(String v)    { this.requestedAt = v; }

    public int         getTimeToLive()             { return timeToLive; }
    public void        setTimeToLive(int v)        { this.timeToLive = v; }

    public StreamInfo  getStream()                 { return stream; }
    public void        setStream(StreamInfo v)     { this.stream = v; }

    public DatasetInfo getDataset()                { return dataset; }
    public void        setDataset(DatasetInfo v)   { this.dataset = v; }

    // -------------------------------------------------------------------------

    public static class StreamInfo {
        private String streamUri;
        private String destination; // response | broadcast
        private String streamType;  // static | live

        public String getStreamUri()             { return streamUri; }
        public void   setStreamUri(String v)     { this.streamUri = v; }

        public String getDestination()           { return destination; }
        public void   setDestination(String v)   { this.destination = v; }

        public String getStreamType()            { return streamType; }
        public void   setStreamType(String v)    { this.streamType = v; }
    }

    public static class DatasetInfo {
        private String      datasetAlias;
        private String      reloadUrl;
        private LiveKeyInfo liveKey;
        private boolean     inheritMetadata;

        public String      getDatasetAlias()              { return datasetAlias; }
        public void        setDatasetAlias(String v)      { this.datasetAlias = v; }

        public String      getReloadUrl()                 { return reloadUrl; }
        public void        setReloadUrl(String v)         { this.reloadUrl = v; }

        public LiveKeyInfo getLiveKey()                   { return liveKey; }
        public void        setLiveKey(LiveKeyInfo v)      { this.liveKey = v; }

        public boolean     isInheritMetadata()            { return inheritMetadata; }
        public void        setInheritMetadata(boolean v)  { this.inheritMetadata = v; }
    }
}
