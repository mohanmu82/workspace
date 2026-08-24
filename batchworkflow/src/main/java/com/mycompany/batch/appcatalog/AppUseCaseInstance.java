package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A use case bound to one or more environments with a concrete set of input values — the unit that
 * is actually executed, and the unit that gets dropped into an {@link AppUseCaseInstanceGroup} to
 * run a whole suite of touch points in one go.
 *
 * <p>Identified by a server-generated {@link #appUseCaseInstanceId} so the same use case can be
 * instanced many times over (different environments, different inputs) and cloned freely.
 *
 * <p>One instance can expand into many executions: {@link #appEnvironments} may name several
 * environments, and it runs against every one of them. How many times to repeat that — once for a
 * regression pass, a hundred times for a load test — is a choice made when the instance is run, not
 * part of what gets saved here; see the {@code runCount} parameter on the execute endpoints.
 */
public class AppUseCaseInstance {

    /** Server-generated, unique across the catalog. */
    private String appUseCaseInstanceId;
    /** Optional human label — the UI falls back to useCaseName @ environment when blank. */
    private String instanceLabel;
    private String appName;
    private String appUseCaseName;
    /**
     * The primary environment. Kept as the first entry of {@link #appEnvironments} so instances
     * saved before multi-environment support — and anything reading a single environment off an
     * instance, such as the generated id — keep working unchanged.
     */
    private String appEnvironment;
    /**
     * Every environment this instance runs against. Empty means "just {@link #appEnvironment}",
     * which is how every instance written before this field existed reads back.
     */
    private List<String> appEnvironments = new ArrayList<>();
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

    public List<String> getAppEnvironments()                                { return appEnvironments; }
    public void setAppEnvironments(List<String> appEnvironments)            { this.appEnvironments = appEnvironments != null ? appEnvironments : new ArrayList<>(); }

    public Map<String, Object> getAppUseCaseInstanceInputs()                                    { return appUseCaseInstanceInputs; }
    public void setAppUseCaseInstanceInputs(Map<String, Object> appUseCaseInstanceInputs)       { this.appUseCaseInstanceInputs = appUseCaseInstanceInputs != null ? appUseCaseInstanceInputs : new LinkedHashMap<>(); }

    /**
     * The environments one run of this instance covers, de-duplicated and in declared order.
     * Falls back to {@link #appEnvironment} so an instance saved before multi-environment support
     * still names exactly the one environment it was pinned to.
     */
    @JsonIgnore
    public List<String> getEffectiveEnvironments() {
        List<String> out = appEnvironments.stream()
                .filter(e -> e != null && !e.isBlank())
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (out.isEmpty() && appEnvironment != null && !appEnvironment.isBlank()) out.add(appEnvironment);
        return out;
    }
}
