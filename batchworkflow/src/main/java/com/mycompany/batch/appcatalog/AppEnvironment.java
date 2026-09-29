package com.mycompany.batch.appcatalog;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One deployment of an {@link AppDefinition} â the {@link #urlPrefix} a use case's
 * {@code urlSuffix} is appended to, plus whatever credentials that particular environment
 * needs for the app's auth method.
 *
 * <p>An app that exposes its admin or health endpoints on a second address states that address as
 * {@link #monitoringUrlPrefix}; a use case then picks which of the two it hangs off through its own
 * {@link AppUseCase#getUrlPrefixType() urlPrefixType}, defaulting to the main one.
 *
 * <p>Unique by ({@link #appName}, {@link #environment}).
 *
 * <p>{@link #envVariables} is the second layer of the execution-time variable merge, sitting on top
 * of the app's own defaults. A value that differs per deployment â a region code, the id of the desk
 * this particular environment serves â is stated where it actually varies, instead of being pushed
 * down into every use case and instance that happens to run there.
 */
public class AppEnvironment {

    private String appName;
    /** Unique per app, e.g. "uat-emea". */
    private String environment;
    /** PROD, UAT or DEV â a coarse classification used for filtering and for prod warnings. */
    private String envClass = "DEV";
    /** Everything before the use case's urlSuffix, e.g. "https://host:8443/api". */
    private String urlPrefix;
    /**
     * Optional second prefix for the admin side of the same deployment, e.g.
     * "https://host:9443/admin". Only the use cases that ask for it by setting their
     * {@code urlPrefixType} to MONITORING are built against it. Leaving it blank means this
     * environment simply has no such address, and a monitoring use case run against it fails
     * saying so rather than quietly going out on the main prefix.
     */
    private String monitoringUrlPrefix;
    /** JWT auth only â the endpoint returning JSON containing the token; the token lands in $jwtToken. */
    private String jwtUrl;
    /**
     * How {@link #jwtUrl} is called â GET or POST. POST sends the credentials as a JSON body and is
     * the default, since that is what every environment configured before this field existed did.
     */
    private String jwtMethod = "POST";
    /** Used by JWT, USERNAMEPASSWORD and DIGEST auth. */
    private String username;
    private String password;
    /** ACTIVE or INACTIVE â INACTIVE environments are blocked from executing. */
    private String envStatus = "ACTIVE";
    /**
     * Variables every use case run against this environment sees. Overrides the app's
     * {@link AppDefinition#getAppVariables() appVariables} of the same name, and is in turn
     * overridden by a use case's own variables and an instance's inputs â see
     * {@code AppExecutionService#mergeVariables}.
     */
    private Map<String, Object> envVariables = new LinkedHashMap<>();

    /**
     * Free text about this deployment — who owns it, what it is pointed at, when it is refreshed,
     * why its credentials differ from its neighbour's. Nothing reads it at execution time; it is
     * there so the environment summary can carry the context that a URL and a variable list cannot,
     * for whoever is being asked to run against it.
     */
    private String comments;

    public String getAppName()                { return appName; }
    public void   setAppName(String appName)  { this.appName = appName; }

    public String getEnvironment()                    { return environment; }
    public void   setEnvironment(String environment)  { this.environment = environment; }

    public String getEnvClass()                   { return envClass; }
    public void   setEnvClass(String envClass)    { this.envClass = envClass != null ? envClass : "DEV"; }

    public String getUrlPrefix()                    { return urlPrefix; }
    public void   setUrlPrefix(String urlPrefix)    { this.urlPrefix = urlPrefix; }

    public String getMonitoringUrlPrefix()                              { return monitoringUrlPrefix; }
    public void   setMonitoringUrlPrefix(String monitoringUrlPrefix)    { this.monitoringUrlPrefix = monitoringUrlPrefix; }

    /**
     * The prefix a use case of the given kind hangs off, or null when this environment has not been
     * given one. Anything other than MONITORING â including a use case saved before the field
     * existed, which reads as null -- means the main application prefix.
     */
    public String prefixFor(String urlPrefixType) {
        String prefix = AppUseCase.PREFIX_TYPE_MONITORING.equalsIgnoreCase(nullToEmpty(urlPrefixType))
                ? monitoringUrlPrefix : urlPrefix;
        return prefix != null && !prefix.isBlank() ? prefix : null;
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    public String getJwtUrl()                 { return jwtUrl; }
    public void   setJwtUrl(String jwtUrl)    { this.jwtUrl = jwtUrl; }

    public String getJwtMethod()                     { return jwtMethod; }
    public void   setJwtMethod(String jwtMethod)     { this.jwtMethod = jwtMethod != null && !jwtMethod.isBlank() ? jwtMethod.trim().toUpperCase() : "POST"; }

    public String getUsername()                   { return username; }
    public void   setUsername(String username)    { this.username = username; }

    public String getPassword()                   { return password; }
    public void   setPassword(String password)    { this.password = password; }

    public String getEnvStatus()                    { return envStatus; }
    public void   setEnvStatus(String envStatus)    { this.envStatus = envStatus != null ? envStatus : "ACTIVE"; }

    public Map<String, Object> getEnvVariables()                    { return envVariables; }
    public void setEnvVariables(Map<String, Object> envVariables)   { this.envVariables = envVariables != null ? envVariables : new LinkedHashMap<>(); }

    public String getComments()                   { return comments; }
    public void   setComments(String comments)    { this.comments = comments; }
}
