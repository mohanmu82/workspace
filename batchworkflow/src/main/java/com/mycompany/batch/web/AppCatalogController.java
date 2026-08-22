package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.AppCatalogService;
import com.mycompany.batch.appcatalog.AppDefinition;
import com.mycompany.batch.appcatalog.AppEnvironment;
import com.mycompany.batch.appcatalog.AppExecutionService;
import com.mycompany.batch.appcatalog.AppUseCase;
import com.mycompany.batch.appcatalog.AppUseCaseInstance;
import com.mycompany.batch.appcatalog.AppUseCaseInstanceGroup;
import com.mycompany.batch.appcatalog.AppUseCaseInstanceOutput;
import com.mycompany.batch.appcatalog.ExecuteInstancesRequest;
import com.mycompany.batch.appcatalog.ExecutionTarget;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST surface behind {@code app.html} and its sub-pages — CRUD over the App Catalog
 * (apps, environments, use cases, instances, instance groups) plus the endpoints that execute
 * instances singly or as a group — locally or on a remote agent — and hand back the payload of a
 * single past execution.
 */
@RestController
@RequestMapping("/appcatalog")
public class AppCatalogController {

    private final AppCatalogService service;
    private final AppExecutionService execution;
    private final ObjectMapper objectMapper;

    public AppCatalogController(AppCatalogService service, AppExecutionService execution, ObjectMapper objectMapper) {
        this.service = service;
        this.execution = execution;
        this.objectMapper = objectMapper;
    }

    // -------------------------------------------------------------------------
    // Apps
    // -------------------------------------------------------------------------

    @GetMapping("/apps")
    public ResponseEntity<List<AppDefinition>> listApps() {
        return ResponseEntity.ok(service.listApps());
    }

    @GetMapping("/apps/{appName}")
    public ResponseEntity<?> getApp(@PathVariable String appName) {
        AppDefinition app = service.getApp(appName);
        return app == null ? notFound("App", appName) : ResponseEntity.ok(app);
    }

    @PostMapping(value = "/apps", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createApp(@RequestBody AppDefinition app) {
        if (app.getAppName() != null && service.getApp(app.getAppName()) != null)
            return badRequest("App '" + app.getAppName() + "' already exists");
        return save(() -> service.saveApp(app));
    }

    @PutMapping(value = "/apps/{appName}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> updateApp(@PathVariable String appName, @RequestBody AppDefinition app) {
        if (service.getApp(appName) == null) return notFound("App", appName);
        app.setAppName(appName);
        return save(() -> service.saveApp(app));
    }

    @DeleteMapping("/apps/{appName}")
    public ResponseEntity<?> deleteApp(@PathVariable String appName) {
        if (service.getApp(appName) == null) return notFound("App", appName);
        return save(() -> { service.deleteApp(appName); return Map.of("status", "deleted", "appName", appName); });
    }

    // -------------------------------------------------------------------------
    // Environments
    // -------------------------------------------------------------------------

    @GetMapping("/environments")
    public ResponseEntity<List<AppEnvironment>> listEnvironments(@RequestParam(required = false) String appName) {
        return ResponseEntity.ok(service.listEnvironments(appName));
    }

    @PostMapping(value = "/environments", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createEnvironment(@RequestBody AppEnvironment env) {
        if (env.getAppName() != null && env.getEnvironment() != null
                && service.getEnvironment(env.getAppName(), env.getEnvironment()) != null)
            return badRequest("Environment '" + env.getEnvironment() + "' already exists for app '" + env.getAppName() + "'");
        return save(() -> service.saveEnvironment(env));
    }

    @PutMapping(value = "/environments/{appName}/{environment}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> updateEnvironment(@PathVariable String appName, @PathVariable String environment,
                                               @RequestBody AppEnvironment env) {
        if (service.getEnvironment(appName, environment) == null) return notFound("Environment", appName + "/" + environment);
        env.setAppName(appName);
        env.setEnvironment(environment);
        return save(() -> service.saveEnvironment(env));
    }

    @DeleteMapping("/environments/{appName}/{environment}")
    public ResponseEntity<?> deleteEnvironment(@PathVariable String appName, @PathVariable String environment) {
        if (service.getEnvironment(appName, environment) == null) return notFound("Environment", appName + "/" + environment);
        return save(() -> {
            service.deleteEnvironment(appName, environment);
            return Map.of("status", "deleted", "environment", environment);
        });
    }

    /**
     * Fetches a token from the posted environment's JWT URL and hands it straight back, so the
     * Environments tab can verify the setup on its own instead of inferring it from a failed run.
     * The environment comes in the body rather than by name so unsaved edits can be tried.
     *
     * <p>Always 200 — a failed token fetch is reported as {@code ok:false} with the reason, which
     * is the whole point of the button.
     */
    @PostMapping(value = "/environments/test-jwt", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> testJwt(@RequestBody AppEnvironment environment) {
        return ResponseEntity.ok(execution.testJwt(environment));
    }

    // -------------------------------------------------------------------------
    // Use cases
    // -------------------------------------------------------------------------

    @GetMapping("/usecases")
    public ResponseEntity<List<AppUseCase>> listUseCases(@RequestParam(required = false) String appName) {
        return ResponseEntity.ok(service.listUseCases(appName));
    }

    @GetMapping("/usecases/{appName}/{useCaseName}")
    public ResponseEntity<?> getUseCase(@PathVariable String appName, @PathVariable String useCaseName) {
        AppUseCase useCase = service.getUseCase(appName, useCaseName);
        return useCase == null ? notFound("Use case", appName + "/" + useCaseName) : ResponseEntity.ok(useCase);
    }

    @PostMapping(value = "/usecases", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createUseCase(@RequestBody AppUseCase useCase) {
        if (useCase.getAppName() != null && useCase.getUseCaseName() != null
                && service.getUseCase(useCase.getAppName(), useCase.getUseCaseName()) != null)
            return badRequest("Use case '" + useCase.getUseCaseName() + "' already exists for app '" + useCase.getAppName() + "'");
        return save(() -> service.saveUseCase(useCase));
    }

    @PutMapping(value = "/usecases/{appName}/{useCaseName}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> updateUseCase(@PathVariable String appName, @PathVariable String useCaseName,
                                           @RequestBody AppUseCase useCase) {
        if (service.getUseCase(appName, useCaseName) == null) return notFound("Use case", appName + "/" + useCaseName);
        useCase.setAppName(appName);
        useCase.setUseCaseName(useCaseName);
        return save(() -> service.saveUseCase(useCase));
    }

    @DeleteMapping("/usecases/{appName}/{useCaseName}")
    public ResponseEntity<?> deleteUseCase(@PathVariable String appName, @PathVariable String useCaseName) {
        if (service.getUseCase(appName, useCaseName) == null) return notFound("Use case", appName + "/" + useCaseName);
        return save(() -> {
            service.deleteUseCase(appName, useCaseName);
            return Map.of("status", "deleted", "useCaseName", useCaseName);
        });
    }

    /**
     * Copies an existing use case under a new name so a near-identical variant (a different URL
     * suffix, one extra header) can be set up without re-entering everything.
     */
    @PostMapping(value = "/usecases/{appName}/{useCaseName}/clone", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> cloneUseCase(@PathVariable String appName, @PathVariable String useCaseName,
                                          @RequestBody Map<String, String> body) {
        AppUseCase source = service.getUseCase(appName, useCaseName);
        if (source == null) return notFound("Use case", appName + "/" + useCaseName);

        String newName = body.get("newUseCaseName");
        if (newName == null || newName.isBlank()) return badRequest("newUseCaseName is required");
        if (service.getUseCase(appName, newName) != null)
            return badRequest("Use case '" + newName + "' already exists for app '" + appName + "'");

        return save(() -> {
            AppUseCase copy = deepCopy(source, AppUseCase.class);
            copy.setUseCaseName(newName);
            return service.saveUseCase(copy);
        });
    }

    // -------------------------------------------------------------------------
    // Use case instances
    // -------------------------------------------------------------------------

    @GetMapping("/instances")
    public ResponseEntity<List<AppUseCaseInstance>> listInstances(@RequestParam(required = false) String appName) {
        return ResponseEntity.ok(service.listInstances(appName));
    }

    @GetMapping("/instances/{instanceId}")
    public ResponseEntity<?> getInstance(@PathVariable String instanceId) {
        AppUseCaseInstance instance = service.getInstance(instanceId);
        return instance == null ? notFound("Instance", instanceId) : ResponseEntity.ok(instance);
    }

    @PostMapping(value = "/instances", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createInstance(@RequestBody AppUseCaseInstance instance) {
        instance.setAppUseCaseInstanceId(null);   // ids are always server-generated on create
        return save(() -> service.saveInstance(instance));
    }

    @PutMapping(value = "/instances/{instanceId}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> updateInstance(@PathVariable String instanceId, @RequestBody AppUseCaseInstance instance) {
        if (service.getInstance(instanceId) == null) return notFound("Instance", instanceId);
        instance.setAppUseCaseInstanceId(instanceId);
        return save(() -> service.saveInstance(instance));
    }

    @DeleteMapping("/instances/{instanceId}")
    public ResponseEntity<?> deleteInstance(@PathVariable String instanceId) {
        if (service.getInstance(instanceId) == null) return notFound("Instance", instanceId);
        return save(() -> {
            service.deleteInstance(instanceId);
            return Map.of("status", "deleted", "appUseCaseInstanceId", instanceId);
        });
    }

    /** Copies an instance, keeping its inputs but taking a fresh id (and optionally a new label). */
    @PostMapping("/instances/{instanceId}/clone")
    public ResponseEntity<?> cloneInstance(@PathVariable String instanceId,
                                           @RequestBody(required = false) Map<String, String> body) {
        AppUseCaseInstance source = service.getInstance(instanceId);
        if (source == null) return notFound("Instance", instanceId);

        return save(() -> {
            AppUseCaseInstance copy = deepCopy(source, AppUseCaseInstance.class);
            copy.setAppUseCaseInstanceId(null);
            if (body != null) {
                if (body.get("instanceLabel") != null)  copy.setInstanceLabel(body.get("instanceLabel"));
                if (body.get("appEnvironment") != null) copy.setAppEnvironment(body.get("appEnvironment"));
            }
            return service.saveInstance(copy);
        });
    }

    // -------------------------------------------------------------------------
    // Instance groups
    // -------------------------------------------------------------------------

    @GetMapping("/groups")
    public ResponseEntity<List<AppUseCaseInstanceGroup>> listGroups() {
        return ResponseEntity.ok(service.listGroups());
    }

    @GetMapping("/groups/{groupName}")
    public ResponseEntity<?> getGroup(@PathVariable String groupName) {
        AppUseCaseInstanceGroup group = service.getGroup(groupName);
        return group == null ? notFound("Group", groupName) : ResponseEntity.ok(group);
    }

    @PostMapping(value = "/groups", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> saveGroup(@RequestBody AppUseCaseInstanceGroup group) {
        return save(() -> service.saveGroup(group));
    }

    @DeleteMapping("/groups/{groupName}")
    public ResponseEntity<?> deleteGroup(@PathVariable String groupName) {
        if (service.getGroup(groupName) == null) return notFound("Group", groupName);
        return save(() -> {
            service.deleteGroup(groupName);
            return Map.of("status", "deleted", "groupName", groupName);
        });
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    /**
     * Every execute endpoint answers with payload-free results — see
     * {@link AppUseCaseInstanceOutput#withoutPayload()}. A grid of results is what the caller
     * actually wants back; the bodies are fetched a row at a time from {@code /executions/{id}}.
     *
     * @param target  {@code LOCAL} (default) to call from this server, or {@code AGENT} to have a
     *                connected remote agent make the call
     * @param agentId with {@code AGENT}, pins the call to one agent; blank round-robins
     */
    @PostMapping("/execute/{instanceId}")
    public ResponseEntity<List<AppUseCaseInstanceOutput>> executeInstance(
            @PathVariable String instanceId,
            @RequestParam(required = false) String target,
            @RequestParam(required = false) String agentId) {
        // A list even for one instance: it expands over every environment it names and repeats
        // itself run-count times, so "run this instance" is a batch as soon as either is set.
        return ResponseEntity.ok(withoutPayloads(
                execution.executeInstance(instanceId, ExecutionTarget.from(target), agentId)));
    }

    /** Runs an ad-hoc selection of instances — the multi-select path on the instances page. */
    @PostMapping(value = "/execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> executeInstances(@RequestBody ExecuteInstancesRequest request) {
        List<String> ids = request.appUseCaseInstanceIds();
        if (ids == null || ids.isEmpty()) return badRequest("appUseCaseInstanceIds is required");
        return ResponseEntity.ok(withoutPayloads(
                execution.executeAll(ids, ExecutionTarget.from(request.target()), request.agentId())));
    }

    @PostMapping("/execute/group/{groupName}")
    public ResponseEntity<?> executeGroup(@PathVariable String groupName,
                                          @RequestParam(required = false) String target,
                                          @RequestParam(required = false) String agentId) {
        AppUseCaseInstanceGroup group = service.getGroup(groupName);
        if (group == null) return notFound("Group", groupName);
        return ResponseEntity.ok(withoutPayloads(
                execution.executeAll(group.getAppUseCaseInstanceIds(), ExecutionTarget.from(target), agentId)));
    }

    /**
     * Every execution this server has made recently, newest first and payload-free — the feed
     * behind {@code app.html#global}. Not scoped to an app: that is the whole point of the page,
     * which puts a run of the Orders catalogue next to one of Payments.
     *
     * @param limit  how many of the most recent to return; the rest of the window stays on the
     *               server rather than being shipped to a grid nobody scrolls that far down
     */
    @GetMapping("/executions")
    public ResponseEntity<List<AppUseCaseInstanceOutput>> listExecutions(
            @RequestParam(required = false, defaultValue = "1000") int limit) {
        List<AppUseCaseInstanceOutput> all = execution.history();
        return ResponseEntity.ok(limit > 0 && all.size() > limit ? all.subList(0, limit) : all);
    }

    /** Drops the whole run history, payloads included. */
    @DeleteMapping("/executions")
    public ResponseEntity<?> clearExecutions() {
        execution.clearHistory();
        return ResponseEntity.ok(Map.of("status", "cleared"));
    }

    /**
     * Repeats past executions, each against the environment it originally ran on rather than
     * whatever its instance names today — a re-run that quietly moved to a different environment
     * would not be a re-run. Ordering follows the request.
     *
     * <p>An execution whose instance has since been deleted comes back as an ERROR row saying so,
     * because dropping it silently would make a re-run of ten rows return nine with no explanation.
     */
    @PostMapping(value = "/executions/rerun", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> rerunExecutions(@RequestBody RerunRequest request) {
        List<String> ids = request.executionIds();
        if (ids == null || ids.isEmpty()) return badRequest("executionIds is required");

        ExecutionTarget target = ExecutionTarget.from(request.target());
        List<AppUseCaseInstanceOutput> results = new ArrayList<>(ids.size());
        for (String executionId : ids) {
            AppUseCaseInstanceOutput past = execution.getExecution(executionId);
            if (past == null) {
                past = execution.history().stream()
                        .filter(o -> o.executionId().equals(executionId))
                        .findFirst().orElse(null);
            }
            if (past == null) return notFound("Execution", executionId);
            results.add(execution.executeOnce(past.appUseCaseInstanceId(), past.environment(),
                    target, request.agentId()).withoutPayload());
        }
        return ResponseEntity.ok(results);
    }

    /**
     * Body for {@code POST /appcatalog/executions/rerun}.
     *
     * @param target  {@code LOCAL} (default) or {@code AGENT} — where the repeat runs from, which
     *                need not be where the original ran
     */
    public record RerunRequest(List<String> executionIds, String target, String agentId) {}

    /**
     * The full result of one execution — request/response bodies, headers and the transformed
     * response. Fetched when a results row is expanded, so a large body only ever crosses the wire
     * for the row someone actually wants to read.
     */
    @GetMapping("/executions/{executionId}")
    public ResponseEntity<?> getExecution(@PathVariable String executionId) {
        AppUseCaseInstanceOutput output = execution.getExecution(executionId);
        return output == null
                ? notFound("Execution", executionId + " — results are kept only for the most recent runs")
                : ResponseEntity.ok(output);
    }

    /** The raw response body as text, for opening a large payload in its own browser tab. */
    @GetMapping(value = "/executions/{executionId}/responsebody", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getExecutionResponseBody(@PathVariable String executionId) {
        AppUseCaseInstanceOutput output = execution.getExecution(executionId);
        if (output == null) return ResponseEntity.status(404).body("Execution not found: " + executionId);
        return ResponseEntity.ok(output.responseBody() == null ? "" : output.responseBody());
    }

    private static List<AppUseCaseInstanceOutput> withoutPayloads(List<AppUseCaseInstanceOutput> outputs) {
        return outputs.stream().map(AppUseCaseInstanceOutput::withoutPayload).toList();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /** Anything that mutates the catalog and can fail with a validation or IO error. */
    @FunctionalInterface
    private interface CatalogAction {
        Object run() throws Exception;
    }

    private ResponseEntity<?> save(CatalogAction action) {
        try {
            return ResponseEntity.ok(action.run());
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    /** Round-trips through JSON so a clone shares no mutable state with its source. */
    private <T> T deepCopy(T source, Class<T> type) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsBytes(source), type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Clone failed: " + e.getMessage());
        }
    }

    private ResponseEntity<Map<String, Object>> notFound(String what, String key) {
        return ResponseEntity.status(404).body(Map.of("error", what + " not found: " + key));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
