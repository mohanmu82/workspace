package com.mycompany.batch.model.stream;

public class UnsubscribeRequest {
    private String sessionId;
    private String datasetAlias;

    public String getSessionId()             { return sessionId; }
    public void   setSessionId(String v)     { this.sessionId = v; }

    public String getDatasetAlias()          { return datasetAlias; }
    public void   setDatasetAlias(String v)  { this.datasetAlias = v; }
}
