package com.mycompany.batch.appcatalog;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A use case bound to one environment with a concrete set of input values — the unit that is
 * actually executed, and the unit that gets dropped into an {@link AppUseCaseInstanceGroup} to
 * run a whole suite of touch points in one go.
 *
 * <p>Identified by a server-generated {@link #appUseCaseInstanceId} so the same use case can be
 * instanced many times over (different environments, different inputs) and cloned freely.
 */
public class AppUseCaseInstance {

    /** Server-generated, unique across the catalog. */
    private String appUseCaseInstanceId;
    /** Optional human label — the UI falls back to useCaseName @ environment when blank. */
    private String instanceLabel;
    private String appName;
    private String appUseCaseName;
    private String appEnvironment;
    /** Values for the declared use case inputs; highest precedence in the variable merge. */
    private Map<String, Object> appUseCaseInstanceInputs = new LinkedHashMap<>();

    public String getAppUseCaseInstanceId()                                 { return appUseCaseInstanceId; }
    public void   setAppUseCaseInstanceId(String appUseCaseInstanceId)      { this.appUseCaseInstanceId = appUseCaseInstanceId; }

    public String getInstanceLabel()                          { return instanceLabel; }
    public void   setInstanceLabel(String instanceLabel)      { this.instanceLabel = instanceLabel; }

    public String getAppName()                { return appName; }
    public void   setAppName(String appName)  { this.appName = appName; }

    public String getAppUseCaseName()                             { return appUseCaseName; }
    public void   setAppUseCaseName(String appUseCaseName)        { this.appUseCaseName = appUseCaseName; }

    public String getAppEnvironment()                             { return appEnvironment; }
    public void   setAppEnvironment(String appEnvironment)        { this.appEnvironment = appEnvironment; }

    public Map<String, Object> getAppUseCaseInstanceInputs()                                    { return appUseCaseInstanceInputs; }
    public void setAppUseCaseInstanceInputs(Map<String, Object> appUseCaseInstanceInputs)       { this.appUseCaseInstanceInputs = appUseCaseInstanceInputs != null ? appUseCaseInstanceInputs : new LinkedHashMap<>(); }
}
