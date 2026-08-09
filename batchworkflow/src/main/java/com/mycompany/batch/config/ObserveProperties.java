package com.mycompany.batch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Connection details for polling Observe's OPAL export/query API
 * (see {@code observe.*} in application.properties). Polling stays off until
 * baseUrl/customerId/apiToken/datasetId/opalQuery are all non-blank.
 */
@Component
@ConfigurationProperties(prefix = "observe")
public class ObserveProperties {

    private boolean enabled = false;
    /** e.g. https://CUSTOMERID.observeinc.com */
    private String baseUrl = "";
    private String customerId = "";
    private String apiToken = "";
    private String datasetId = "";
    /** OPAL pipeline text, e.g. "statsby avg(cpu_utilization), group_by(host)". */
    private String opalQuery = "";
    /** Query time window, e.g. "5m". */
    private String lookback = "5m";
    private long pollIntervalMs = 60000;
    private long timeoutMs = 10000;

    public boolean isEnabled()                { return enabled; }
    public void    setEnabled(boolean e)      { this.enabled = e; }

    public String getBaseUrl()                { return baseUrl; }
    public void   setBaseUrl(String baseUrl)  { this.baseUrl = baseUrl != null ? baseUrl.trim() : ""; }

    public String getCustomerId()                  { return customerId; }
    public void   setCustomerId(String customerId) { this.customerId = customerId != null ? customerId.trim() : ""; }

    public String getApiToken()                { return apiToken; }
    public void   setApiToken(String apiToken) { this.apiToken = apiToken != null ? apiToken.trim() : ""; }

    public String getDatasetId()                { return datasetId; }
    public void   setDatasetId(String datasetId) { this.datasetId = datasetId != null ? datasetId.trim() : ""; }

    public String getOpalQuery()               { return opalQuery; }
    public void   setOpalQuery(String opalQuery) { this.opalQuery = opalQuery != null ? opalQuery : ""; }

    public String getLookback()                { return lookback; }
    public void   setLookback(String lookback) { this.lookback = lookback != null && !lookback.isBlank() ? lookback : "5m"; }

    public long getPollIntervalMs()               { return pollIntervalMs; }
    public void setPollIntervalMs(long ms)        { this.pollIntervalMs = ms; }

    public long getTimeoutMs()                    { return timeoutMs; }
    public void setTimeoutMs(long ms)             { this.timeoutMs = ms; }

    /** True once every field the export/query call needs has been supplied. */
    public boolean isConfigured() {
        return !baseUrl.isBlank() && !customerId.isBlank() && !apiToken.isBlank()
                && !datasetId.isBlank() && !opalQuery.isBlank();
    }
}
