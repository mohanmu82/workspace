package com.mycompany.batch.appcatalog;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.auth.BasicAuthProvider;
import com.mycompany.batch.auth.DigestAuthProvider;
import com.mycompany.batch.auth.HttpAuthProvider;
import com.mycompany.batch.auth.JwtAuthProvider;
import com.mycompany.batch.auth.KerberosAuthProvider;
import com.mycompany.batch.model.HttpBatchRequest;
import com.mycompany.batch.model.HttpMethod;
import com.mycompany.batch.model.JsonataTransform;
import com.mycompany.batch.service.AgentHttpDispatchService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes {@link AppUseCaseInstance}s.
 *
 * <p>The heart of this class is the three-layer variable merge: an app's {@code appVariables} are
 * the base, a use case's {@code appUseCaseVariables} override them, and an instance's inputs
 * override both. The merged map is then substituted into the URL, headers and body, so the same
 * use case definition can be pointed at different data purely by changing an instance.
 *
 * <p>Each instance first expands into the cartesian product of the environments it names and the
 * run count the caller asked for, so one instance can be a hundred calls against each of three
 * environments. A run executes that expansion in parallel and returns one
 * {@link AppUseCaseInstanceOutput} per call, in the expansion's own order — never throwing for a
 * failed call, since a failure is itself a result worth showing in the grid.
 *
 * <p>The call itself goes out either from this server ({@link ExecutionTarget#LOCAL}) or from a
 * connected remote agent ({@link ExecutionTarget#AGENT}) — everything around it, from the variable
 * merge to the JSONata transform, is identical either way.
 *
 * <p>Every result is also kept here under its {@code executionId} (see {@link #getExecution}) so
 * the browser can take the payload-free version into its grid and pull the bodies back for one row
 * at a time.
 */
@Service
public class AppExecutionService {

    /** Matches {@code ${name}} and bare {@code $name} placeholders in a URL, header or body. */
    private static final Pattern VARIABLE = Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    /**
     * How many finished executions keep their bodies around for the "load payload" button. Old
     * enough results fall out: this is a convenience for the page that just ran them, not a history.
     */
    private static final int RETAINED_EXECUTIONS = 200;

    /**
     * How many executions the Global Runs page can look back over. These are payload-free — a few
     * hundred bytes each — so the window can be far longer than the one that keeps bodies, which is
     * what makes "every run across every app" a useful view rather than a glimpse of the last batch.
     */
    private static final int RETAINED_HISTORY = 5000;

    /**
     * How many calls a single run makes at once, by default. A run-count of a hundred against three
     * environments is three hundred calls, and firing all of them together would be a load test of
     * the endpoint rather than a regression pass over it. Configurable via {@code appcatalog.thread-count}
     * because that ceiling depends on what the target endpoints can actually take; a caller can also
     * override it per request (see {@link #executeAll(List, ExecutionTarget, String, Integer, Integer)}).
     */
    @Value("${appcatalog.thread-count:10}")
    private int defaultThreadCount;

    private final AppCatalogService catalog;
    private final ObjectMapper objectMapper;
    private final AgentHttpDispatchService agentDispatch;

    /** Access-ordered so a result that is still being expanded in the grid stays resident. */
    private final Map<String, AppUseCaseInstanceOutput> recentExecutions = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, AppUseCaseInstanceOutput> eldest) {
                    return size() > RETAINED_EXECUTIONS;
                }
            });

    /**
     * Newest first, payload-free, capped at {@link #RETAINED_HISTORY}. Deliberately separate from
     * {@link #recentExecutions}: that map is keyed for one-row payload lookups and evicts on access
     * order, neither of which gives a stable "what has run lately, in order" list.
     */
    private final Deque<AppUseCaseInstanceOutput> history = new ArrayDeque<>();

    public AppExecutionService(AppCatalogService catalog, ObjectMapper objectMapper,
                               AgentHttpDispatchService agentDispatch) {
        this.catalog = catalog;
        this.objectMapper = objectMapper;
        this.agentDispatch = agentDispatch;
    }

    /** The full result of a past execution, bodies and all, or null once it has aged out. */
    public AppUseCaseInstanceOutput getExecution(String executionId) {
        return recentExecutions.get(executionId);
    }

    /**
     * Every execution this server has made recently, newest first and payload-free — what the
     * Global Runs page lists. It spans every app, so it is the one view where a run made from the
     * Orders instances page and one made from Payments sit side by side.
     */
    public List<AppUseCaseInstanceOutput> history() {
        synchronized (history) {
            return new ArrayList<>(history);
        }
    }

    /** Empties the global run history. The retained payloads go with it — they are its detail rows. */
    public void clearHistory() {
        synchronized (history) {
            history.clear();
        }
        recentExecutions.clear();
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    /** Runs several instances concurrently on this server, preserving the caller's ordering. */
    public List<AppUseCaseInstanceOutput> executeAll(List<String> instanceIds) {
        return executeAll(instanceIds, ExecutionTarget.LOCAL, null, null, null);
    }

    /**
     * Runs several instances concurrently, preserving the caller's ordering in the result list.
     * Each instance expands first — one call per environment it names, repeated as many times as
     * {@code runCount} asks — so a group of three instances at a hundred repeats can very well come
     * back as three hundred results, ordered instance by instance, then environment, then repeat.
     *
     * <p>On {@link ExecutionTarget#AGENT} with no {@code agentId} the calls go out round-robin, so a
     * group run spreads itself across every connected agent.
     */
    public List<AppUseCaseInstanceOutput> executeAll(List<String> instanceIds, ExecutionTarget target, String agentId) {
        return executeAll(instanceIds, target, agentId, null, null);
    }

    /**
     * Same as {@link #executeAll(List, ExecutionTarget, String)}, but lets the caller cap how many
     * calls run at once instead of taking {@link #defaultThreadCount}, and how many times each
     * instance repeats against each of its environments. Both null fall back to their defaults — a
     * cap of {@link #defaultThreadCount} and a single pass — so a caller can ask for a regression
     * run (repeat 1) or a load test (repeat 100) without either being baked into the instances
     * themselves.
     */
    public List<AppUseCaseInstanceOutput> executeAll(List<String> instanceIds, ExecutionTarget target, String agentId,
                                                     Integer threadCount, Integer runCount) {
        if (instanceIds == null || instanceIds.isEmpty()) return List.of();

        List<RunPlan> plans = new ArrayList<>();
        for (String id : instanceIds) plans.addAll(plan(id, runCount));
        return executePlans(plans, target, agentId, threadCount);
    }

    /**
     * Runs one instance, expanded over its environments and {@code runCount} — so this returns a
     * list even for a single instance, and a hundred rows when the caller asks for a hundred
     * repeats. Null or non-positive {@code runCount} means once each.
     */
    public List<AppUseCaseInstanceOutput> executeInstance(String instanceId, ExecutionTarget target, String agentId,
                                                           Integer runCount) {
        return executePlans(plan(instanceId, runCount), target, agentId, null);
    }

    /** Runs one instance on this server, once against each of its environments. */
    public List<AppUseCaseInstanceOutput> executeInstance(String instanceId) {
        return executeInstance(instanceId, ExecutionTarget.LOCAL, null, null);
    }

    /**
     * Runs one instance exactly once against one named environment, ignoring its run count. This is
     * the re-run door: a row on the Global Runs page repeats the environment it originally hit
     * rather than fanning back out over every environment the instance now names.
     */
    public AppUseCaseInstanceOutput executeOnce(String instanceId, String environment,
                                                ExecutionTarget target, String agentId) {
        AppUseCaseInstance instance = catalog.getInstance(instanceId);
        String chosen = environment != null && !environment.isBlank() ? environment
                : instance != null ? firstEnvironment(instance) : null;
        return execute(new RunPlan(instanceId, chosen, 1, 1), target, agentId);
    }

    // -------------------------------------------------------------------------
    // Expansion — one instance becomes environments x run count calls
    // -------------------------------------------------------------------------

    /**
     * One unit of work: an instance pinned to a single environment and a single repeat. The whole
     * cartesian product is built up front so the pool sees a flat list and the result ordering is
     * decided in one place rather than emerging from the order the futures happened to finish in.
     *
     * @param environment the environment to call, or null when the instance names none at all —
     *                    that plan exists only to carry the failure into the grid
     */
    private record RunPlan(String instanceId, String environment, int runIndex, int runCount) {}

    /**
     * The full expansion of one instance: every environment it names, each repeated {@code runCount}
     * times. Null or non-positive {@code runCount} means once each.
     */
    private List<RunPlan> plan(String instanceId, Integer runCount) {
        AppUseCaseInstance instance = catalog.getInstance(instanceId);
        if (instance == null) return List.of(new RunPlan(instanceId, null, 1, 1));

        List<String> environments = instance.getEffectiveEnvironments();
        // No environment at all is still one plan — execute() turns it into a named failure rather
        // than silently dropping an instance the caller asked to run.
        if (environments.isEmpty()) environments = Collections.singletonList(null);

        int effectiveRunCount = runCount != null && runCount > 0 ? runCount : 1;
        List<RunPlan> plans = new ArrayList<>(environments.size() * effectiveRunCount);
        for (String environment : environments) {
            for (int run = 1; run <= effectiveRunCount; run++) {
                plans.add(new RunPlan(instanceId, environment, run, effectiveRunCount));
            }
        }
        return plans;
    }

    private static String firstEnvironment(AppUseCaseInstance instance) {
        List<String> environments = instance.getEffectiveEnvironments();
        return environments.isEmpty() ? null : environments.get(0);
    }

    /**
     * Runs a flat list of plans concurrently and returns the results in the plans' own order — a
     * regression sweep finishes out of order by nature, and a grid that reshuffles itself from one
     * run to the next cannot be compared with the previous one.
     *
     * @param threadCount caller-supplied cap on how many plans run at once, or null to use
     *                    {@link #defaultThreadCount}. Anything less than 1 also falls back to the
     *                    default rather than deadlocking on a zero-size pool.
     */
    private List<AppUseCaseInstanceOutput> executePlans(List<RunPlan> plans, ExecutionTarget target, String agentId,
                                                         Integer threadCount) {
        if (plans.isEmpty()) return List.of();

        int effectiveThreadCount = threadCount != null && threadCount > 0 ? threadCount : defaultThreadCount;
        try (ExecutorService pool = Executors.newFixedThreadPool(Math.min(plans.size(), effectiveThreadCount))) {
            List<Future<AppUseCaseInstanceOutput>> futures = new ArrayList<>(plans.size());
            for (RunPlan runPlan : plans) {
                futures.add(pool.submit(() -> execute(runPlan, target, agentId)));
            }
            List<AppUseCaseInstanceOutput> out = new ArrayList<>(futures.size());
            for (int i = 0; i < futures.size(); i++) {
                try {
                    out.add(futures.get(i).get());
                } catch (Exception e) {
                    out.add(errorOutput(plans.get(i), null, rootMessage(e)));
                }
            }
            return out;
        }
    }

    /**
     * Runs one plan from {@code target}. Never throws — a failure comes back as an ERROR-status
     * output, including "no agents connected" and anything the agent itself reports.
     */
    private AppUseCaseInstanceOutput execute(RunPlan runPlan, ExecutionTarget target, String agentId) {
        String instanceId = runPlan.instanceId();
        AppUseCaseInstance instance = catalog.getInstance(instanceId);
        if (instance == null) return errorOutput(runPlan, null, "Unknown instance id: " + instanceId);
        if (runPlan.environment() == null)
            return errorOutput(runPlan, instance, "Instance names no environment to run against");

        AppDefinition  app     = catalog.getApp(instance.getAppName());
        AppUseCase     useCase = catalog.getUseCase(instance.getAppName(), instance.getAppUseCaseName());
        AppEnvironment env     = catalog.getEnvironment(instance.getAppName(), runPlan.environment());

        if (app == null)     return errorOutput(runPlan, instance, "Unknown app: " + instance.getAppName(), env, useCase);
        if (useCase == null) return errorOutput(runPlan, instance, "Unknown use case: " + instance.getAppUseCaseName(), env, useCase);
        if (env == null)     return errorOutput(runPlan, instance, "Unknown environment: " + runPlan.environment(), null, useCase);

        if (!"ACTIVE".equalsIgnoreCase(app.getAppStatus()))
            return errorOutput(runPlan, instance, "App '" + app.getAppName() + "' is " + app.getAppStatus(), env, useCase);
        if (!"ACTIVE".equalsIgnoreCase(env.getEnvStatus()))
            return errorOutput(runPlan, instance, "Environment '" + env.getEnvironment() + "' is " + env.getEnvStatus(), env, useCase);
        if (!"HTTP".equalsIgnoreCase(app.getAppMode()))
            return errorOutput(runPlan, instance, "App mode '" + app.getAppMode() + "' cannot be executed — only HTTP is supported", env, useCase);

        long started = System.currentTimeMillis();
        String url = null;
        String requestBody = null;
        String jwtToken = null;
        Map<String, String> requestHeaders = new LinkedHashMap<>();

        try {
            Map<String, Object> variables = mergeVariables(app, useCase, instance);

            // Auth runs before substitution so a JWT token is available to the request as $jwtToken.
            String authorization = applyAuth(app, env, variables);
            // The same token the templates saw, reported on the output so it can be replayed by hand.
            jwtToken = variables.get("jwtToken") instanceof String token ? token : null;

            url = substitute(nullToEmpty(env.getUrlPrefix()) + nullToEmpty(useCase.getUrlSuffix()), variables);
            requestBody = useCase.getHttpBody() != null ? substitute(useCase.getHttpBody(), variables) : null;
            requestHeaders = parseHeaders(useCase.getHttpHeaders(), variables);
            if (authorization != null && !containsHeaderIgnoreCase(requestHeaders, "Authorization")) {
                requestHeaders.put("Authorization", authorization);
            }

            String method = useCase.getHttpMethod();
            HttpOutcome outcome = target == ExecutionTarget.AGENT
                    ? sendViaAgent(method, url, requestHeaders, requestBody, useCase.getTimeout(), agentId)
                    : sendLocally(method, url, requestHeaders, requestBody, useCase.getTimeout());
            long elapsed = System.currentTimeMillis() - started;

            Object transformed = null;
            String transformError = null;
            try {
                transformed = transform(outcome.body(), useCase);
            } catch (Exception e) {
                transformError = "Transform failed: " + rootMessage(e);
            }

            return retain(new AppUseCaseInstanceOutput(
                    newExecutionId(),
                    started,
                    instanceId,
                    labelFor(instance, env.getEnvironment()),
                    instance.getAppName(),
                    env.getEnvironment(),
                    env.getEnvClass(),
                    instance.getAppUseCaseName(),
                    instance.getAppUseCaseInstanceInputs(),
                    outcome.statusCode() < 400 ? "SUCCESS" : "FAILED",
                    outcome.statusCode(),
                    runPlan.runIndex(),
                    runPlan.runCount(),
                    elapsed,
                    url,
                    unresolvedVariables(url),
                    method,
                    outcome.executedVia(),
                    jwtToken,
                    requestBody,
                    outcome.body(),
                    requestHeaders,
                    outcome.headers(),
                    transformed,
                    transformError,
                    null, null));

        } catch (Exception e) {
            // Substitution may not have run at all — an auth failure happens before the URL is
            // built. Then the URL reported is the configured template, and saying which of its
            // placeholders are "unresolved" would be a lie: nothing was ever resolved.
            return retain(new AppUseCaseInstanceOutput(
                    newExecutionId(),
                    started,
                    instanceId,
                    labelFor(instance, env.getEnvironment()),
                    instance.getAppName(),
                    env.getEnvironment(),
                    env.getEnvClass(),
                    instance.getAppUseCaseName(),
                    instance.getAppUseCaseInstanceInputs(),
                    "ERROR",
                    null,
                    runPlan.runIndex(),
                    runPlan.runCount(),
                    System.currentTimeMillis() - started,
                    url != null ? url : rawUrl(env, useCase),
                    url != null ? unresolvedVariables(url) : null,
                    useCase.getHttpMethod(),
                    describeTarget(target, agentId),
                    jwtToken,
                    requestBody,
                    null,
                    requestHeaders,
                    Map.of(),
                    null,
                    rootMessage(e),
                    null, null));
        }
    }

    // -------------------------------------------------------------------------
    // Sending
    // -------------------------------------------------------------------------

    /** What came back from the endpoint, however the call was made. */
    private record HttpOutcome(int statusCode, String body, Map<String, List<String>> headers, String executedVia) {}

    private HttpOutcome sendLocally(String method, String url, Map<String, String> headers,
                                    String body, int timeoutMs) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs));
        headers.forEach(builder::header);

        // method() rather than GET()/POST()/PUT(): it accepts a body on any verb, so whatever
        // the use case defines is actually sent and the reported requestBody stays truthful.
        builder = builder.method(method, body != null && !body.isBlank()
                ? HttpRequest.BodyPublishers.ofString(body)
                : HttpRequest.BodyPublishers.noBody());

        HttpResponse<String> response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return new HttpOutcome(response.statusCode(), response.body(),
                new LinkedHashMap<>(response.headers().map()), ExecutionTarget.LOCAL.name());
    }

    /**
     * Hands the fully-resolved request to a connected agent and waits for its reply. The agent is a
     * dumb pipe here — auth, substitution and the transform all still happen on this side, so the
     * only difference from a local run is the host the packet leaves from.
     */
    private HttpOutcome sendViaAgent(String method, String url, Map<String, String> headers,
                                     String body, int timeoutMs, String agentId) {
        HttpBatchRequest.Item item = new HttpBatchRequest.Item();
        item.setUrl(url);
        item.setMethod(HttpMethod.from(method));
        item.setHeaders(headers);
        item.setBody(body);
        item.setTimeoutMs(timeoutMs);

        Map<String, Object> reply = agentDispatch.dispatchSingle(item, timeoutMs, agentId);
        String ranOn = describeTarget(ExecutionTarget.AGENT, String.valueOf(reply.get("agentId")));

        Object failure = reply.get("error") != null ? reply.get("error")
                : "error".equals(reply.get("type")) ? reply.get("message") : null;
        if (failure != null) throw new IllegalStateException(ranOn + " reported: " + failure);
        if (!(reply.get("statusCode") instanceof Number statusCode))
            throw new IllegalStateException(ranOn + " returned no status code");

        Map<String, List<String>> responseHeaders = new LinkedHashMap<>();
        if (reply.get("headers") instanceof Map<?, ?> raw) {
            raw.forEach((name, value) -> responseHeaders.put(String.valueOf(name),
                    value instanceof List<?> list ? list.stream().map(String::valueOf).toList()
                            : List.of(String.valueOf(value))));
        }
        return new HttpOutcome(statusCode.intValue(),
                reply.get("body") == null ? "" : String.valueOf(reply.get("body")),
                responseHeaders, ranOn);
    }

    /** How a run names where it went — {@code LOCAL}, {@code AGENT} or {@code AGENT:orders-host1}. */
    private static String describeTarget(ExecutionTarget target, String agentId) {
        if (target != ExecutionTarget.AGENT) return ExecutionTarget.LOCAL.name();
        return agentId == null || agentId.isBlank() || "null".equals(agentId) ? "AGENT" : "AGENT:" + agentId;
    }

    private static String newExecutionId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Keeps the full result addressable so the browser can pull its bodies back on demand, and adds
     * a payload-free copy to the head of the global history the Global Runs page reads.
     */
    private AppUseCaseInstanceOutput retain(AppUseCaseInstanceOutput output) {
        recentExecutions.put(output.executionId(), output);
        synchronized (history) {
            history.addFirst(output.withoutPayload());
            while (history.size() > RETAINED_HISTORY) history.removeLast();
        }
        return output;
    }

    // -------------------------------------------------------------------------
    // Variables
    // -------------------------------------------------------------------------

    /**
     * Builds the effective variable map for one execution, lowest precedence first:
     * app variables, then use case variables, then the use case's declared input defaults,
     * then the instance's own input values.
     */
    public Map<String, Object> mergeVariables(AppDefinition app, AppUseCase useCase, AppUseCaseInstance instance) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (app != null)     merged.putAll(app.getAppVariables());
        if (useCase != null) {
            merged.putAll(useCase.getAppUseCaseVariables());
            for (AppUseCaseInput input : useCase.getAppUseCaseInputs()) {
                if (input.name() == null || input.name().isBlank()) continue;
                if (input.defaultValue() != null && !input.defaultValue().isBlank()) {
                    merged.put(input.name(), coerce(input.defaultValue(), input.type()));
                }
            }
        }
        if (instance != null) {
            instance.getAppUseCaseInstanceInputs().forEach((k, v) -> {
                if (v != null && !(v instanceof String s && s.isBlank())) merged.put(k, v);
            });
        }
        return merged;
    }

    /** Coerces a declared input's string default into the type the use case says it is. */
    private Object coerce(String value, String type) {
        try {
            return switch (type == null ? "string" : type) {
                case "number"  -> value.contains(".") ? Double.valueOf(value) : Long.valueOf(value);
                case "boolean" -> Boolean.valueOf(value);
                case "json"    -> objectMapper.readValue(value, Object.class);
                default        -> value;
            };
        } catch (Exception e) {
            return value;
        }
    }

    /**
     * Replaces every {@code ${name}} / {@code $name} placeholder with its variable value.
     * Unknown placeholders are left untouched so a missing variable shows up plainly in the
     * resulting URL or body rather than silently becoming an empty string.
     */
    public String substitute(String template, Map<String, Object> variables) {
        if (template == null || template.isEmpty()) return template;

        Matcher matcher = VARIABLE.matcher(template);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String name = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            Object value = variables.get(name);
            String replacement = value == null ? matcher.group() : stringify(value);
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    /**
     * The placeholders {@link #substitute} left behind, in the order they appear. Every one of them
     * went to the endpoint verbatim, because no variable answered to that name — which is a very
     * different thing from a URL that was never substituted in the first place.
     */
    private static List<String> unresolvedVariables(String resolved) {
        if (resolved == null || resolved.isEmpty()) return List.of();

        List<String> names = new ArrayList<>();
        Matcher matcher = VARIABLE.matcher(resolved);
        while (matcher.find()) {
            String name = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (!names.contains(name)) names.add(name);
        }
        return names;
    }

    private String stringify(Object value) {
        if (value instanceof String s) return s;
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    // -------------------------------------------------------------------------
    // Headers
    // -------------------------------------------------------------------------

    /** Accepts either a JSON object or newline-separated {@code Name: value} lines. */
    public Map<String, String> parseHeaders(String raw, Map<String, Object> variables) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return headers;

        String resolved = substitute(raw, variables);
        String trimmed = resolved.trim();

        if (trimmed.startsWith("{")) {
            try {
                Map<String, Object> parsed = objectMapper.readValue(trimmed, Map.class);
                parsed.forEach((k, v) -> headers.put(k, v == null ? "" : stringify(v)));
                return headers;
            } catch (Exception e) {
                throw new IllegalArgumentException("httpHeaders is not valid JSON: " + rootMessage(e));
            }
        }

        for (String line : trimmed.split("\\r?\\n")) {
            if (line.isBlank()) continue;
            int colon = line.indexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("Malformed header line (expected 'Name: value'): " + line);
            headers.put(line.substring(0, colon).trim(), line.substring(colon + 1).trim());
        }
        return headers;
    }

    private boolean containsHeaderIgnoreCase(Map<String, String> headers, String name) {
        return headers.keySet().stream().anyMatch(k -> k.equalsIgnoreCase(name));
    }

    // -------------------------------------------------------------------------
    // Auth
    // -------------------------------------------------------------------------

    /**
     * Resolves the app's auth method against the environment's credentials, returning the
     * {@code Authorization} header value (or null for NONE).
     *
     * <p>For JWT the raw token is also published as the {@code jwtToken} variable so a use case
     * that needs it somewhere other than the Authorization header — a query parameter, a custom
     * header, a field in the body — can reference {@code $jwtToken} directly.
     */
    private String applyAuth(AppDefinition app, AppEnvironment env, Map<String, Object> variables) throws Exception {
        String method = app.getAuthMethod() == null ? "NONE" : app.getAuthMethod().trim().toUpperCase();

        switch (method) {
            case "JWT" -> {
                if (env.getJwtUrl() == null || env.getJwtUrl().isBlank())
                    throw new IllegalArgumentException("JWT auth requires a jwtUrl on environment " + env.getEnvironment());
                HttpAuthProvider provider = new JwtAuthProvider(
                        app.getAppName(), env.getUsername(), env.getPassword(),
                        env.getJwtUrl(), env.getJwtMethod(), objectMapper);
                String header = provider.getAuthorizationHeader();
                variables.put("jwtToken", header.startsWith("Bearer ") ? header.substring(7) : header);
                return header;
            }
            case "USERNAMEPASSWORD", "BASIC" -> {
                if (env.getUsername() == null || env.getUsername().isBlank())
                    throw new IllegalArgumentException("Username/password auth requires a username on environment " + env.getEnvironment());
                return new BasicAuthProvider(env.getUsername(), nullToEmpty(env.getPassword())).getAuthorizationHeader();
            }
            case "DIGEST" -> {
                return new DigestAuthProvider(env.getUsername(), env.getPassword(), env.getJwtUrl(), objectMapper)
                        .getAuthorizationHeader();
            }
            case "KERBEROS" -> {
                // Reuses the shared provider: username is the principal, password the keytab path,
                // and the target service principal is derived from the environment's host.
                String servicePrincipal = "HTTP@" + URI.create(nullToEmpty(env.getUrlPrefix())).getHost();
                return new KerberosAuthProvider(env.getUsername(), env.getPassword(), servicePrincipal)
                        .getAuthorizationHeader();
            }
            default -> {
                return null;
            }
        }
    }

    /**
     * Calls an environment's token endpoint on its own and hands the result back, so the
     * Environments tab can prove the JWT setup works before any use case depends on it. Takes the
     * environment as posted rather than as stored, so credentials can be tried before they are
     * saved.
     *
     * <p>Never throws: a failure is the answer the caller is looking for, so it comes back as
     * {@code ok:false} with the message rather than as an HTTP error the page has to unwrap.
     */
    public Map<String, Object> testJwt(AppEnvironment env) {
        long started = System.currentTimeMillis();
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            if (env == null || env.getJwtUrl() == null || env.getJwtUrl().isBlank())
                throw new IllegalArgumentException("A JWT URL is required to fetch a token.");

            String header = new JwtAuthProvider(env.getAppName(), env.getUsername(), env.getPassword(),
                    env.getJwtUrl(), env.getJwtMethod(), objectMapper).getAuthorizationHeader();

            result.put("ok", true);
            result.put("token", header.startsWith("Bearer ") ? header.substring(7) : header);
            result.put("authorizationHeader", header);
            result.put("jwtUrl", env.getJwtUrl());
            result.put("jwtMethod", env.getJwtMethod());
            result.put("sentCredentials", env.getUsername() != null && !env.getUsername().isBlank());
        } catch (Exception e) {
            result.put("ok", false);
            result.put("error", rootMessage(e));
            result.put("jwtUrl", env == null ? null : env.getJwtUrl());
            result.put("jwtMethod", env == null ? null : env.getJwtMethod());
        }
        result.put("timeTaken", System.currentTimeMillis() - started);
        return result;
    }

    // -------------------------------------------------------------------------
    // Response handling
    // -------------------------------------------------------------------------

    /**
     * Evaluates the use case's JSONata expression against the response body, parsed according to
     * its output format. Returns null when no transform is configured — the raw body is already
     * on the output, so echoing a parsed copy of it would only double the payload.
     */
    private Object transform(String body, AppUseCase useCase) throws Exception {
        JsonataTransform transform = useCase.getJsonataTransform();
        if (transform == null || transform.value() == null || transform.value().isBlank()) return null;
        if (body == null || body.isBlank()) return null;

        Object parsed = switch (useCase.getOutputFormat()) {
            case "json" -> objectMapper.readValue(body, Object.class);
            case "xml"  -> parseXml(body);
            default     -> body;
        };
        return Jsonata.jsonata(transform.value()).evaluate(parsed);
    }

    /**
     * Converts an XML response into nested maps so a JSONata expression can walk it the same way
     * it walks JSON. Attributes are folded in under their own names, repeated sibling elements
     * become lists, and an element holding only text collapses to that text.
     */
    private Object parseXml(String body) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Untrusted responses: no external entity resolution, no DTDs.
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);

        Document document = factory.newDocumentBuilder()
                .parse(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        document.getDocumentElement().normalize();

        Element root = document.getDocumentElement();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put(root.getNodeName(), elementToValue(root));
        return out;
    }

    private Object elementToValue(Element element) {
        Map<String, Object> map = new LinkedHashMap<>();

        NamedNodeMap attributes = element.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            map.put(attributes.item(i).getNodeName(), attributes.item(i).getNodeValue());
        }

        StringBuilder text = new StringBuilder();
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child instanceof Element childElement) {
                Object value = elementToValue(childElement);
                map.merge(childElement.getNodeName(), value, (existing, added) -> {
                    List<Object> list = existing instanceof List<?> l ? new ArrayList<>(l) : new ArrayList<>(List.of(existing));
                    list.add(added);
                    return list;
                });
            } else if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
                text.append(child.getNodeValue());
            }
        }

        String trimmed = text.toString().trim();
        if (map.isEmpty()) return trimmed;
        if (!trimmed.isEmpty()) map.put("#text", trimmed);
        return map;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private AppUseCaseInstanceOutput errorOutput(RunPlan runPlan, AppUseCaseInstance instance, String message) {
        return errorOutput(runPlan, instance, message, null, null);
    }

    /**
     * A failure that happened before the request could be built. The environment and use case are
     * passed in whenever the catalog lookup had already resolved them, so the result still names the
     * endpoint the run was aimed at: a blank URL tells an operator nothing about what went wrong.
     *
     * <p>The environment reported is the one the <em>plan</em> aimed at rather than the instance's
     * primary one — on a multi-environment instance those differ, and naming the wrong environment
     * on a failure row sends the reader to the wrong host.
     */
    private AppUseCaseInstanceOutput errorOutput(RunPlan runPlan, AppUseCaseInstance instance, String message,
                                                 AppEnvironment env, AppUseCase useCase) {
        String instanceId = runPlan.instanceId();
        return retain(new AppUseCaseInstanceOutput(
                newExecutionId(),
                System.currentTimeMillis(),
                instanceId,
                instance != null ? labelFor(instance, runPlan.environment()) : instanceId,
                instance != null ? instance.getAppName() : null,
                runPlan.environment(),
                env != null ? env.getEnvClass() : null,
                instance != null ? instance.getAppUseCaseName() : null,
                instance != null ? instance.getAppUseCaseInstanceInputs() : Map.of(),
                "ERROR", null,
                runPlan.runIndex(), runPlan.runCount(), 0L,
                rawUrl(env, useCase),
                null,   // the request was never built, so nothing was resolved or left unresolved
                useCase != null ? useCase.getHttpMethod() : null,
                null,
                null,
                null, null, Map.of(), Map.of(), null, message,
                null, null));
    }

    /**
     * The endpoint as configured, before variable substitution. Reported when a run fails too early
     * to resolve the placeholders -- an auth failure happens before the URL is built, and a failed
     * catalog lookup before that again. Null rather than "" when neither part is known, so the grid
     * keeps showing an empty cell instead of a misleading blank string.
     */
    private static String rawUrl(AppEnvironment env, AppUseCase useCase) {
        String raw = nullToEmpty(env == null ? null : env.getUrlPrefix())
                   + nullToEmpty(useCase == null ? null : useCase.getUrlSuffix());
        return raw.isBlank() ? null : raw;
    }

    /**
     * The name this run shows under. The fallback names the environment the run actually went to
     * rather than the instance's primary one: on a multi-environment instance every row would
     * otherwise claim to be the first environment.
     */
    private String labelFor(AppUseCaseInstance instance, String environment) {
        if (instance.getInstanceLabel() != null && !instance.getInstanceLabel().isBlank())
            return instance.getInstanceLabel();
        return instance.getAppUseCaseName() + " @ "
                + (environment != null && !environment.isBlank() ? environment : instance.getAppEnvironment());
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    /**
     * Names the exception and the most specific message available. The thrown type is kept because
     * the JDK HTTP client buries a refused connection under a message-less ClosedChannelException,
     * and "ClosedChannelException" on its own tells an operator nothing about what went wrong.
     */
    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();

        String message = t.getMessage() != null ? t.getMessage()
                : root.getMessage() != null ? root.getMessage()
                : originHint(root);
        return t.getClass().getSimpleName() + ": " + message;
    }

    /**
     * Where a message-less exception came from. Repeating the class name ("NullPointerException:
     * NullPointerException") tells an operator nothing; the throw site at least says where to look.
     */
    private static String originHint(Throwable root) {
        StackTraceElement[] frames = root.getStackTrace();
        return frames.length == 0
                ? "no message"
                : "no message — thrown at " + frames[0].getClassName().substring(frames[0].getClassName().lastIndexOf('.') + 1)
                  + "." + frames[0].getMethodName() + ":" + frames[0].getLineNumber();
    }
}
