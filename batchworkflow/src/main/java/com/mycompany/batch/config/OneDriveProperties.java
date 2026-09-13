package com.mycompany.batch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Azure AD app registration used to read files from work/school OneDrive through Microsoft
 * Graph (see {@code onedrive.*} in application.properties). Authenticates as the app itself
 * (client credentials), so the registration needs the {@code Files.Read.All} application
 * permission with admin consent.
 */
@Component
@ConfigurationProperties(prefix = "onedrive")
public class OneDriveProperties {

    private String tenantId = "";
    private String clientId = "";
    private String clientSecret = "";
    /** UPN whose OneDrive is read when a location does not name one, e.g. jane@mycompany.com. */
    private String defaultUser = "";
    private long timeoutMs = 30000;

    public String getTenantId()                    { return tenantId; }
    public void   setTenantId(String tenantId)     { this.tenantId = tenantId != null ? tenantId.trim() : ""; }

    public String getClientId()                    { return clientId; }
    public void   setClientId(String clientId)     { this.clientId = clientId != null ? clientId.trim() : ""; }

    public String getClientSecret()                  { return clientSecret; }
    public void   setClientSecret(String clientSecret) { this.clientSecret = clientSecret != null ? clientSecret.trim() : ""; }

    public String getDefaultUser()                   { return defaultUser; }
    public void   setDefaultUser(String defaultUser) { this.defaultUser = defaultUser != null ? defaultUser.trim() : ""; }

    public long getTimeoutMs()                     { return timeoutMs; }
    public void setTimeoutMs(long ms)              { this.timeoutMs = ms; }

    /** True once the app registration credentials have all been supplied. */
    public boolean isConfigured() {
        return !tenantId.isBlank() && !clientId.isBlank() && !clientSecret.isBlank();
    }
}
