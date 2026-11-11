package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A registered application — the root of the App Catalog. Everything else hangs off
 * {@link #appName}: environments say <em>where</em> to call it, use cases say <em>what</em>
 * to call, and use case instances pin concrete inputs for a repeatable run.
 *
 * <p>{@link #appVariables} is the base layer of the three-level variable merge applied at
 * execution time (app &rarr; use case &rarr; instance inputs), so shared values such as a
 * region code or a client id are declared once here instead of on every use case.
 */
public class AppDefinition {

    /** Unique key across the catalog; referenced by environments, use cases and instances. */
    private String appName;
    private String appDescription;
    /**
     * HTTP, SOLACE, WS, ORACLE or POSTGRES. HTTP use cases are called once per run; WS use cases are
     * WebSocket subscriptions, opened from a page's WEBSOCKET action — the environment's URL prefix is
     * then the {@code ws://} or {@code wss://} address. ORACLE and POSTGRES use cases are SQL statements
     * run over JDBC: the environment's URL prefix is the JDBC URL, its username and password the login,
     * and the use case's body the statement. SOLACE is not executable server-side.
     */
    private String appMode = "HTTP";
    /** ACTIVE or INACTIVE — INACTIVE apps stay in the catalog but are blocked from executing. */
    private String appStatus = "ACTIVE";
    /** NONE, JWT, KERBEROS, USERNAMEPASSWORD, DIGEST or NTLM — the credentials come from the environment. */
    private String authMethod = "NONE";
    /** Default variables for every use case of this app; lowest precedence at execution time. */
    private Map<String, Object> appVariables = new LinkedHashMap<>();

    public String getAppName()                    { return appName; }
    public void   setAppName(String appName)      { this.appName = appName; }

    public String getAppDescription()                        { return appDescription; }
    public void   setAppDescription(String appDescription)   { this.appDescription = appDescription; }

    /** ORACLE or POSTGRES — a use case is SQL run over JDBC rather than an HTTP call. */
    @JsonIgnore
    public boolean isDatabase() { return "ORACLE".equalsIgnoreCase(appMode) || "POSTGRES".equalsIgnoreCase(appMode); }

    public String getAppMode()                    { return appMode; }
    public void   setAppMode(String appMode)      { this.appMode = appMode != null ? appMode : "HTTP"; }

    public String getAppStatus()                  { return appStatus; }
    public void   setAppStatus(String appStatus)  { this.appStatus = appStatus != null ? appStatus : "ACTIVE"; }

    public String getAuthMethod()                     { return authMethod; }
    public void   setAuthMethod(String authMethod)    { this.authMethod = authMethod != null ? authMethod : "NONE"; }

    public Map<String, Object> getAppVariables()                                { return appVariables; }
    public void setAppVariables(Map<String, Object> appVariables)               { this.appVariables = appVariables != null ? appVariables : new LinkedHashMap<>(); }
}
