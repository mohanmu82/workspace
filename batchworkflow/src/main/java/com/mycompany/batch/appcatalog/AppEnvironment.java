package com.mycompany.batch.appcatalog;

/**
 * One deployment of an {@link AppDefinition} — the {@link #urlPrefix} a use case's
 * {@code urlSuffix} is appended to, plus whatever credentials that particular environment
 * needs for the app's auth method.
 *
 * <p>Unique by ({@link #appName}, {@link #environment}).
 */
public class AppEnvironment {

    private String appName;
    /** Unique per app, e.g. "uat-emea". */
    private String environment;
    /** PROD, UAT or DEV — a coarse classification used for filtering and for prod warnings. */
    private String envClass = "DEV";
    /** Everything before the use case's urlSuffix, e.g. "https://host:8443/api". */
    private String urlPrefix;
    /** JWT auth only — the endpoint returning JSON containing the token; the token lands in $jwtToken. */
    private String jwtUrl;
    /** Used by JWT, USERNAMEPASSWORD and DIGEST auth. */
    private String username;
    private String password;
    /** ACTIVE or INACTIVE — INACTIVE environments are blocked from executing. */
    private String envStatus = "ACTIVE";

    public String getAppName()                { return appName; }
    public void   setAppName(String appName)  { this.appName = appName; }

    public String getEnvironment()                    { return environment; }
    public void   setEnvironment(String environment)  { this.environment = environment; }

    public String getEnvClass()                   { return envClass; }
    public void   setEnvClass(String envClass)    { this.envClass = envClass != null ? envClass : "DEV"; }

    public String getUrlPrefix()                    { return urlPrefix; }
    public void   setUrlPrefix(String urlPrefix)    { this.urlPrefix = urlPrefix; }

    public String getJwtUrl()                 { return jwtUrl; }
    public void   setJwtUrl(String jwtUrl)    { this.jwtUrl = jwtUrl; }

    public String getUsername()                   { return username; }
    public void   setUsername(String username)    { this.username = username; }

    public String getPassword()                   { return password; }
    public void   setPassword(String password)    { this.password = password; }

    public String getEnvStatus()                    { return envStatus; }
    public void   setEnvStatus(String envStatus)    { this.envStatus = envStatus != null ? envStatus : "ACTIVE"; }
}
