package com.mycompany.batch.apm;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Bootstrap defaults for the APM forwarder (see {@code apm.*} in application.properties).
 * Registration can also happen at runtime via {@code POST /apm/register} — that call takes
 * priority over whatever is configured here; these properties only matter if {@code apm.enabled}
 * is true at startup, in which case {@link ApmForwardingService} auto-registers with them.
 */
@Component
@ConfigurationProperties(prefix = "apm")
public class ApmProperties {

    private boolean enabled = false;
    private String serverUrl = "";
    private String secretToken = "";
    private String apiKey = "";
    private String serviceName = "batchworkflow";
    private String serviceVersion = "";
    private String environment = "";
    private int historyCap = 200;
    private long timeoutMs = 5000;

    public boolean isEnabled()           { return enabled; }
    public void    setEnabled(boolean e) { this.enabled = e; }

    public String getServerUrl()                { return serverUrl; }
    public void   setServerUrl(String serverUrl) { this.serverUrl = serverUrl != null ? serverUrl.trim() : ""; }

    public String getSecretToken()                  { return secretToken; }
    public void   setSecretToken(String secretToken) { this.secretToken = secretToken != null ? secretToken.trim() : ""; }

    public String getApiKey()             { return apiKey; }
    public void   setApiKey(String apiKey) { this.apiKey = apiKey != null ? apiKey.trim() : ""; }

    public String getServiceName()                 { return serviceName; }
    public void   setServiceName(String serviceName) { this.serviceName = serviceName != null && !serviceName.isBlank() ? serviceName.trim() : "batchworkflow"; }

    public String getServiceVersion()                  { return serviceVersion; }
    public void   setServiceVersion(String serviceVersion) { this.serviceVersion = serviceVersion != null ? serviceVersion.trim() : ""; }

    public String getEnvironment()                { return environment; }
    public void   setEnvironment(String environment) { this.environment = environment != null ? environment.trim() : ""; }

    public int  getHistoryCap()          { return historyCap; }
    public void setHistoryCap(int cap)   { this.historyCap = cap; }

    public long getTimeoutMs()           { return timeoutMs; }
    public void setTimeoutMs(long ms)    { this.timeoutMs = ms; }

    /** True once a server URL is present — the only hard requirement to attempt forwarding. */
    public boolean isConfigured() {
        return !serverUrl.isBlank();
    }
}
