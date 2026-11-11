package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.McpSseConfig.McpSseServer;
import com.mycompany.batch.service.ConnectionLogService;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Publishes every use-case action on every page as an MCP tool, so Claude can run a page's buttons
 * without the page.
 *
 * <p>One tool per action — page-level and inline on a control alike — named {@code page__label}.
 * Its arguments are the page fields the action's input templates read ({@code ${orderId}}), plus
 * {@code environment}. All are optional, exactly as on the page: a field nobody supplies leaves the
 * input out and the instance keeps the value it was saved with.
 *
 * <p>Templates are resolved the way {@code apppage.js#resolveInputs} resolves them, against the
 * built-ins, then the page's variables, then the tool arguments. Claude gets what each control the
 * action fills would show — the page's transforms, path, enriched columns and bind filters applied
 * by {@link AppPageOutputReader} — keyed by target when there are several. An optional {@value #TARGET_ARG}
 * argument (or the operator's pick of one output) narrows that to one, and only its chain runs.
 *
 * <p>Operators enable, disable and re-describe tools on the MCP Tools page ({@link McpToolSetting}).
 * The tool list is re-derived from the catalog on a timer as well as on every setting saved, so a page
 * edited by any route shows up without each route having to remember to say so. Every tool goes to
 * both servers — streamable HTTP at {@code /mcp} and HTTP+SSE at {@code /sse} — along with
 * {@value #SEARCH_TOOL}, which finds tools by words in their name or description.
 */
@Component
public class AppPageMcpTools {

    private static final Logger log = LoggerFactory.getLogger(AppPageMcpTools.class);

    // Same two forms apppage.js accepts: ${field.with-dots} and $bareName.
    private static final Pattern PLACEHOLDER =
            Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)");
    private static final Pattern WHOLE =
            Pattern.compile("^\\$\\{([A-Za-z0-9_.\\-]+)\\}$|^\\$([A-Za-z_][A-Za-z0-9_]*)$");
    /** Responses are cut to this many characters: a multi-megabyte body would flood Claude's context. */
    static final int MAX_RESPONSE_CHARS = 50_000;
    static final String SEARCH_TOOL = "search_tools";
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final ObjectProvider<McpSyncServer> streamable;
    private final ObjectProvider<McpSseServer> sse;
    private final AppCatalogService catalog;
    private final AppExecutionService execution;
    private final McpToolSettingService settings;
    private final AppPageOutputReader reader;
    private final ObjectMapper mapper;
    private final ConnectionLogService connectionLog;
    /** What is registered with the servers now, by tool name — the diff baseline for {@link #sync}. */
    private final Map<String, McpSchema.Tool> registered = new HashMap<>();

    public AppPageMcpTools(ObjectProvider<McpSyncServer> streamable, ObjectProvider<McpSseServer> sse,
                           AppCatalogService catalog, AppExecutionService execution,
                           McpToolSettingService settings, AppPageOutputReader reader, ObjectMapper mapper,
                           ConnectionLogService connectionLog) {
        this.connectionLog = connectionLog;
        this.streamable = streamable;
        this.sse = sse;
        this.catalog = catalog;
        this.execution = execution;
        this.settings = settings;
        this.reader = reader;
        this.mapper = mapper;
    }

    // ── The registry ─────────────────────────────────────────────────────────

    /** One output an action binds its response into: the action's own target, or one of its extra bindings. */
    public record Output(String id, String label, String source, String path, List<String> transformNames,
                         @JsonIgnore Binding binding) {}

    /**
     * How the page reads a target once its path has picked: as grid rows (enriched, then filtered)
     * or as one value (a text box or link).
     */
    public record Binding(boolean rows, boolean keyValueGrid, boolean scalarsOnly,
                          List<AppPageEnrichColumn> enrichColumns, List<AppPageRowFilter> bindFilters, boolean pivoted) {}

    /** Control types the page binds one value into (apppage.js#bindValue, #bindLink); every other target takes rows. */
    private static final Set<String> VALUE_TYPES = Set.of("text", "textarea", "link");
    static final String TARGET_ARG = "target";

    /** One page action as a tool, enabled or not — what the MCP Tools page lists. */
    public record ToolEntry(String name, String pageName, String actionKey, String label, String appName,
                            String useCase, String httpMethod, String urlSuffix, List<String> environments,
                            List<String> arguments, String generatedDescription, String description,
                            boolean enabled, List<Output> outputs, String output, String problem,
                            Map<String, List<String>> aliases, boolean readOnly) {}

    /** An action plus what to call it when it has no actionId of its own. */
    record ActionRef(AppPageAction action, String key) {}

    private record Built(ToolEntry entry, McpSchema.Tool tool) {}

    /** Every use-case action on every page, settings applied, whether or not it is published. */
    public List<ToolEntry> registry() {
        return build().stream().map(Built::entry).toList();
    }

    /** {@link #registry()} narrowed to entries containing every word of {@code query}. */
    public List<ToolEntry> search(String query) {
        return registry().stream().filter(e -> matches(e, query)).toList();
    }

    static boolean matches(ToolEntry e, String query) {
        if (isBlank(query)) return true;
        String haystack = String.join(" ", e.name(), e.pageName(), nullToEmpty(e.label()), nullToEmpty(e.useCase()),
                nullToEmpty(e.description()), nullToEmpty(e.urlSuffix())).toLowerCase(Locale.ROOT);
        return Arrays.stream(query.toLowerCase(Locale.ROOT).trim().split("\\s+")).allMatch(haystack::contains);
    }

    private List<Built> build() {
        List<Built> built = new ArrayList<>();
        Set<String> taken = new LinkedHashSet<>(List.of(SEARCH_TOOL));
        for (AppPage page : catalog.listPages(null)) {
            for (ActionRef ref : actionsOf(page)) {
                String label = !isBlank(ref.action.getActionLabel()) ? ref.action.getActionLabel() : ref.key;
                String name = uniqueName(taken, toolName(page.getPageName(), label));
                taken.add(name);
                try {
                    built.add(entryFor(page, ref, label, name));
                } catch (Exception e) {
                    log.warn("MCP: skipping action {} on page {}: {}", ref.key, page.getPageName(), e.getMessage());
                }
            }
        }
        return built;
    }

    private Built entryFor(AppPage page, ActionRef ref, String label, String name) {
        AppPageAction action = ref.action;
        McpToolSetting setting = settings.get(page.getPageName(), ref.key);
        boolean enabled = setting == null || setting.enabled();
        String custom = setting == null ? null : setting.description();
        List<Output> outputs = outputsOf(page, action);
        String output = setting == null || isBlank(setting.output())
                || outputs.stream().noneMatch(o -> o.id().equals(setting.output())) ? McpToolSetting.ALL : setting.output();
        List<String> arguments = new ArrayList<>(fieldsOf(page, action));
        Map<String, List<String>> aliases = aliasesOf(setting, arguments);

        AppUseCaseInstance instance = catalog.getInstance(action.getAppUseCaseInstanceId());
        AppUseCase useCase = instance == null ? null : catalog.getUseCase(instance.getAppName(), instance.getAppUseCaseName());
        if (useCase == null) {
            String problem = instance == null ? "Instance " + action.getAppUseCaseInstanceId() + " not found"
                    : "Use case " + instance.getAppUseCaseName() + " not found";
            return new Built(new ToolEntry(name, page.getPageName(), ref.key, label, null, null, null, null,
                    List.of(), arguments, "", nullToEmpty(custom), false, outputs, output, problem, aliases,
                    setting != null && Boolean.TRUE.equals(setting.readOnly())), null);
        }
        String method = useCase.getHttpMethod() == null ? "GET" : useCase.getHttpMethod().toUpperCase(Locale.ROOT);
        boolean readOnly = setting == null || setting.readOnly() == null ? "GET".equals(method) : setting.readOnly();

        StringBuilder generated = new StringBuilder("Runs the \"").append(label).append("\" action of page \"")
                .append(page.getPageName()).append("\": ").append(method).append(' ')
                .append(nullToEmpty(useCase.getUrlSuffix())).append(" (app ").append(useCase.getAppName())
                .append(", use case ").append(useCase.getUseCaseName()).append(").");
        if (!isBlank(page.getDescription())) {
            String about = page.getDescription().trim();
            generated.append(" Page: ").append(about).append(about.endsWith(".") ? "" : ".");
        }
        Map<String, Output> targets = keyed(outputs);
        boolean offersTarget = offersTarget(arguments, outputs, output);
        if (!McpToolSetting.ALL.equals(output)) {
            outputs.stream().filter(o -> o.id().equals(output)).findFirst()
                    .ifPresent(o -> generated.append(" Returns the rows the \"").append(o.label())
                            .append("\" control would show, after the page's transforms, enriched columns and filters."));
        } else if (targets.size() == 1) {
            generated.append(" Returns the rows the \"").append(outputs.get(0).label())
                    .append("\" control would show, after the page's transforms, enriched columns and filters.");
        } else if (targets.size() > 1) {
            generated.append(" Returns what each control it fills would show, after the page's transforms, enriched "
                    + "columns and filters, keyed by target: ").append(String.join(", ", targets.keySet()))
                    .append(offersTarget ? ". Pass \"" + TARGET_ARG + "\" to get only one of them." : ".");
        }
        List<String> required = arguments.stream().filter(f -> feedsRequired(action, useCase, f)).toList();
        generated.append(required.isEmpty() ? " Every argument is optional; omitted ones keep the saved value."
                : " Required: " + String.join(", ", required) + ". Other arguments are optional; omitted ones keep the saved value.");
        String description = isBlank(custom) ? generated.toString() : custom.trim();

        Map<String, Object> properties = new LinkedHashMap<>();
        for (String field : arguments) {
            Map<String, Object> prop = new LinkedHashMap<>();
            String type = jsonType(action, useCase, field);
            if (type != null) prop.put("type", type);
            prop.put("description", fieldDescription(page, field) + (required.contains(field) ? " (required)" : ""));
            properties.put(field, prop);
            // The schema forbids unknown properties, so each alternate name is listed for Claude to use.
            for (String alias : aliases.getOrDefault(field, List.of())) {
                Map<String, Object> aliasProp = new LinkedHashMap<>(prop);
                aliasProp.put("description", "Same as \"" + field + "\".");
                properties.putIfAbsent(alias, aliasProp);
            }
        }
        List<String> appEnvironments = catalog.listEnvironments(useCase.getAppName()).stream()
                .map(AppEnvironment::getEnvironment).toList();
        if (!appEnvironments.isEmpty()) {
            properties.put("environment", Map.of("type", "string", "enum", appEnvironments,
                    "description", "Which environment to call. Omit for the page's default"
                            + (isBlank(page.getDefaultEnvironment()) ? "." : " (" + page.getDefaultEnvironment().trim() + ").")));
        }
        if (offersTarget) {
            properties.put(TARGET_ARG, Map.of("type", "string", "enum", List.copyOf(targets.keySet()),
                    "description", "Return only this target's data; the others are not worked out. Omit for all of them."));
        }
        String title = (isBlank(page.getTitle()) ? page.getPageName() : page.getTitle()) + " — " + label;
        McpSchema.Tool tool = McpSchema.Tool.builder()
                .name(name)
                .title(title)
                .description(description)
                .inputSchema(new McpSchema.JsonSchema("object", properties, required, false, null, null))
                .annotations(new McpSchema.ToolAnnotations(title, readOnly,
                        !readOnly && "DELETE".equals(method), null, true, null))
                .build();
        return new Built(new ToolEntry(name, page.getPageName(), ref.key, label, useCase.getAppName(),
                useCase.getUseCaseName(), method, useCase.getUrlSuffix(), appEnvironments, arguments,
                generated.toString(), description, enabled, outputs, output, null, aliases, readOnly), tool);
    }

    /** The setting's alternate names, kept only for arguments the action still has and names it does not. */
    static Map<String, List<String>> aliasesOf(McpToolSetting setting, Collection<String> arguments) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        if (setting == null || setting.aliases() == null) return out;
        Set<String> used = new LinkedHashSet<>(arguments);
        used.addAll(List.of("environment", TARGET_ARG));
        for (String field : arguments) {
            List<String> names = new ArrayList<>();
            for (String a : setting.aliases().getOrDefault(field, List.of())) {
                if (!isBlank(a) && used.add(a.trim())) names.add(a.trim());
            }
            if (!names.isEmpty()) out.put(field, names);
        }
        return out;
    }

    /** Renames alias keys to their argument; a value under the argument's own name wins. */
    static void applyAliases(Map<String, Object> args, Map<String, List<String>> aliases) {
        aliases.forEach((field, names) -> names.forEach(a -> {
            Object v = args.remove(a);
            if (v != null) args.putIfAbsent(field, v);
        }));
    }

    /** The action's own target first, then its extra bindings; only those that fill a control. */
    static List<Output> outputsOf(AppPage page, AppPageAction action) {
        List<Output> outputs = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        addOutput(outputs, seen, page, action.getTargetControlId(), action.getSource(), action.getArrayPath(),
                action.getValuePath(), action.getTransformNames(), new Binding(true, action.isKeyValueGrid(),
                        action.isScalarsOnly(), action.getEnrichColumns(), action.getBindFilters(), pivoted(action.getPivot())));
        for (AppPageBinding b : action.getExtraBindings()) {
            addOutput(outputs, seen, page, b.getTargetControlId(), b.getSource(), b.getArrayPath(), b.getValuePath(),
                    b.getTransformNames(), new Binding(true, b.isKeyValueGrid(), b.isScalarsOnly(),
                            b.getEnrichColumns(), b.getBindFilters(), pivoted(b.getPivot())));
        }
        return outputs;
    }

    private static void addOutput(List<Output> outputs, Set<String> seen, AppPage page, String target, String source,
                                  String arrayPath, String valuePath, List<String> transforms, Binding binding) {
        // shortcut: two bindings into the same target (e.g. two new grids) share an id, so only the first is selectable.
        if (isBlank(target) || !seen.add(target)) return;
        String label = AppPageAction.NEW_GRID.equals(target) ? "New grid" : target;
        boolean value = false;
        for (AppPageControl c : page.getControls()) {
            if (!target.equals(c.getControlId())) continue;
            if (!isBlank(c.getLabel())) label = c.getLabel();
            value = VALUE_TYPES.contains(c.getType());
        }
        Binding read = value ? new Binding(false, false, false, List.of(), List.of(), false) : binding;
        outputs.add(new Output(target, label, AppPageAction.METADATA.equals(source) ? AppPageAction.METADATA
                : AppPageAction.PAYLOAD, nullToEmpty(value ? valuePath : arrayPath),
                transforms == null ? List.of() : transforms, read));
    }

    private static boolean pivoted(AppPagePivot pivot) { return pivot != null && pivot.groupsAnything(); }

    /** Outputs under the names a caller picks them by and the result is keyed by: their labels, made unique. */
    static Map<String, Output> keyed(List<Output> outputs) {
        Map<String, Output> keyed = new LinkedHashMap<>();
        for (Output o : outputs) keyed.put(keyed.containsKey(o.label()) ? o.label() + " (" + o.id() + ")" : o.label(), o);
        return keyed;
    }

    // ── Publishing ───────────────────────────────────────────────────────────

    private List<McpSyncServer> servers() {
        List<McpSyncServer> servers = new ArrayList<>();
        streamable.ifAvailable(servers::add);
        sse.ifAvailable(s -> servers.add(s.server()));
        return servers;
    }

    @Scheduled(initialDelay = 0, fixedDelay = 30_000)
    public synchronized void sync() {
        List<McpSyncServer> servers = servers();
        if (servers.isEmpty()) return;

        Map<String, SyncToolSpecification> wanted = new LinkedHashMap<>();
        wanted.put(SEARCH_TOOL, searchTool());
        for (Built b : build()) {
            if (b.tool == null || !b.entry.enabled()) continue;
            String pageName = b.entry.pageName(), key = b.entry.actionKey();
            wanted.put(b.tool.name(), SyncToolSpecification.builder().tool(b.tool)
                    .callHandler((exchange, request) -> call(pageName, key, request.arguments()))
                    .build());
        }
        for (String name : new ArrayList<>(registered.keySet())) {
            SyncToolSpecification next = wanted.get(name);
            if (next == null || !next.tool().equals(registered.get(name))) {
                servers.forEach(s -> s.removeTool(name));
                registered.remove(name);
            }
        }
        wanted.forEach((name, spec) -> {
            if (registered.containsKey(name)) return;
            SyncToolSpecification logged = logged(spec);
            servers.forEach(s -> s.addTool(logged));
            registered.put(name, spec.tool());
        });
    }

    /** The same tool, with each call recorded for the SSE / MCP Connections page. */
    private SyncToolSpecification logged(SyncToolSpecification spec) {
        return SyncToolSpecification.builder().tool(spec.tool()).callHandler((exchange, request) -> {
            long start = System.currentTimeMillis();
            McpSchema.CallToolResult result = null;
            try {
                return result = spec.callHandler().apply(exchange, request);
            } finally {
                McpSchema.Implementation client = exchange.getClientInfo();
                String text = result == null || result.content() == null ? null : result.content().stream()
                        .filter(McpSchema.TextContent.class::isInstance)
                        .map(c -> ((McpSchema.TextContent) c).text()).findFirst().orElse(null);
                connectionLog.toolCall(exchange.sessionId(), client == null ? null : client.name() + " " + client.version(),
                        spec.tool().name(), request.arguments(), start,
                        result == null || Boolean.TRUE.equals(result.isError()), text);
            }
        }).build();
    }

    private SyncToolSpecification searchTool() {
        McpSchema.Tool tool = McpSchema.Tool.builder()
                .name(SEARCH_TOOL)
                .title("Search page action tools")
                .description("Finds the page action tools on this server whose name, page, use case or description "
                        + "contains every word of the query. Use it to pick a tool when the list is long.")
                .inputSchema(new McpSchema.JsonSchema("object", Map.of("query", Map.of("type", "string",
                        "description", "Words to look for, e.g. \"order status\". Blank lists every tool.")),
                        List.of(), false, null, null))
                .annotations(new McpSchema.ToolAnnotations("Search page action tools", true, false, true, false, null))
                .build();
        return SyncToolSpecification.builder().tool(tool).callHandler((exchange, request) -> {
            Object query = request.arguments() == null ? null : request.arguments().get("query");
            try {
                List<Map<String, Object>> hits = search(query == null ? null : query.toString()).stream()
                        .filter(e -> e.enabled() && e.problem() == null)
                        .map(e -> Map.<String, Object>of("name", e.name(), "description", e.description(),
                                "arguments", e.arguments()))
                        .toList();
                return McpSchema.CallToolResult.builder().addTextContent(mapper.writeValueAsString(hits)).build();
            } catch (Exception e) {
                return error(e.getMessage());
            }
        }).build();
    }

    // ── Running ──────────────────────────────────────────────────────────────

    /**
     * Runs the action as it stands now — looked up again by page and key, so an edit since the last
     * sync is honoured rather than the tool replaying a stale copy.
     */
    McpSchema.CallToolResult call(String pageName, String key, Map<String, Object> arguments) {
        try {
            McpToolSetting setting = settings.get(pageName, key);
            if (setting != null && !setting.enabled()) return error("This tool has been disabled.");
            Run run = run(pageName, key, arguments);
            String body = run.data() == null ? "" : run.data() instanceof String s ? s : mapper.writeValueAsString(run.data());
            if (body.length() > MAX_RESPONSE_CHARS) {
                run.meta().put("truncated", "response cut to " + MAX_RESPONSE_CHARS + " of " + body.length() + " characters");
                body = body.substring(0, MAX_RESPONSE_CHARS);
            }
            return McpSchema.CallToolResult.builder()
                    .addTextContent(mapper.writeValueAsString(run.meta()))
                    .addTextContent(body)
                    .isError(run.error())
                    .build();
        } catch (Exception e) {
            return error(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    /** One run of an action: the call's facts, what its chosen targets would show, and whether it failed. */
    record Run(Map<String, Object> meta, Object data, boolean error) {}

    /**
     * Runs the action {@code key} of page {@code pageName} as the page would — {@code arguments} over
     * the page's own values — and reads the answer as its targets would show it. Shared by the MCP
     * tools and the {@link DbPublishScheduler}.
     *
     * @throws IllegalArgumentException when the action is gone or the target asked for is not one of its
     */
    Run run(String pageName, String key, Map<String, Object> arguments) throws Exception {
        AppPage page = catalog.getPage(pageName);
        AppPageAction action = page == null ? null : actionsOf(page).stream()
                .filter(r -> r.key.equals(key)).map(ActionRef::action).findFirst().orElse(null);
        if (action == null) throw new IllegalArgumentException("Action " + key + " no longer exists on page " + pageName);
        McpToolSetting setting = settings.get(pageName, key);

        Map<String, Object> args = arguments == null ? new HashMap<>() : new HashMap<>(arguments);
        applyAliases(args, aliasesOf(setting, fieldsOf(page, action)));
        List<Output> outputs = outputsOf(page, action);
        String pinned = setting == null ? McpToolSetting.ALL : nullToEmpty(setting.output());
        Map<String, Output> targets = keyed(outputs);
        Object wanted = offersTarget(fieldsOf(page, action), outputs, pinned) ? args.remove(TARGET_ARG) : null;
        if (outputs.stream().anyMatch(o -> o.id().equals(pinned))) {
            targets.values().removeIf(o -> !o.id().equals(pinned));
        } else if (wanted != null && !wanted.toString().isBlank()) {
            String w = wanted.toString().trim();
            // By the key the schema lists, or by the control id, ignoring case.
            targets.entrySet().removeIf(e -> !e.getKey().equalsIgnoreCase(w) && !e.getValue().id().equalsIgnoreCase(w));
            if (targets.isEmpty())
                throw new IllegalArgumentException("No target \"" + w + "\". Targets: " + String.join(", ", keyed(outputs).keySet()));
        }
        Map<String, Object> scope = scopeFor(page, args);
        Object env = args.get("environment");
        // The page's DEFAULT wins over the action/instance environment, which only the page itself uses.
        String environment = env != null && !env.toString().isBlank() ? env.toString()
                : !isBlank(page.getDefaultEnvironment()) ? page.getDefaultEnvironment().trim()
                : resolveTemplate(action.getEnvironmentOverride(), scope);
        String baseUrl = resolveTemplate(action.getBaseUrlOverride(), scope);
        String timeout = resolveTemplate(action.getTimeoutOverride(), scope).trim();
        Integer timeoutMs = timeout.matches("\\d+") && Integer.parseInt(timeout) > 0 ? Integer.parseInt(timeout) : null;

        // shortcut: repeatCount (stress runs) is ignored here; add it if Claude should load-test.
        Map<String, Object> sent = resolveInputs(action.getInputs(), scope);
        AppUseCaseInstanceOutput out = execution.executeWithInputs(action.getAppUseCaseInstanceId(),
                environment, sent, ExecutionTarget.from(null), null, false, timeoutMs, baseUrl);
        return toRun(page, out, targets, sent, scope);
    }

    /** The data each chosen target would show on the page; an action that fills nothing returns the payload. */
    private Run toRun(AppPage page, AppUseCaseInstanceOutput out, Map<String, Output> targets,
                      Map<String, Object> sent, Map<String, Object> scope) throws Exception {
        Map<String, Object> meta = metadataOf(out);
        Object data;
        boolean allFailed = false;
        if (targets.isEmpty()) {
            data = AppPageOutputReader.payload(out);
        } else {
            Map<String, String> problems = new LinkedHashMap<>();
            Set<String> notes = new LinkedHashSet<>();
            Map<String, Object> read = reader.read(page, targets, out, sent, scope, problems, notes);
            meta.put("targets", List.copyOf(targets.keySet()));
            if (!problems.isEmpty()) meta.put("targetErrors", problems);
            if (!notes.isEmpty()) meta.put("notes", notes);
            allFailed = read.isEmpty();
            data = targets.size() == 1 ? read.get(targets.keySet().iterator().next()) : read;
        }
        return new Run(meta, data, !"SUCCESS".equals(out.status()) || allFailed);
    }

    /** The call's own facts. Deliberately leaves out jwtToken and the request headers: they carry credentials. */
    private static Map<String, Object> metadataOf(AppUseCaseInstanceOutput out) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("status", out.status());
        meta.put("statusCode", out.statusCode());
        meta.put("httpMethod", out.httpMethod());
        meta.put("url", out.url());
        meta.put("environment", out.environment());
        meta.put("timeTakenMs", out.timeTaken());
        if (out.error() != null) meta.put("error", out.error());
        if (out.unresolvedVariables() != null && !out.unresolvedVariables().isEmpty())
            meta.put("unresolvedVariables", out.unresolvedVariables());
        return meta;
    }

    /** A {@code target} argument is offered when the caller has a choice and no page field is already called that. */
    static boolean offersTarget(java.util.Collection<String> fields, List<Output> outputs, String pinned) {
        return outputs.size() > 1 && !fields.contains(TARGET_ARG)
                && outputs.stream().noneMatch(o -> o.id().equals(pinned));
    }

    private static McpSchema.CallToolResult error(String message) {
        return McpSchema.CallToolResult.builder().addTextContent(nullToEmpty(message)).isError(true).build();
    }

    // ── Actions, names and arguments ─────────────────────────────────────────

    static List<ActionRef> actionsOf(AppPage page) {
        List<ActionRef> refs = new ArrayList<>();
        for (AppPageAction a : page.getActions()) refs.add(new ActionRef(a, keyOf(a, "action", refs.size())));
        for (AppPageControl c : page.getControls()) {
            List<AppPageAction> inline = c.getActions();
            for (int i = 0; i < inline.size(); i++)
                refs.add(new ActionRef(inline.get(i), keyOf(inline.get(i), c.getControlId(), i + 1)));
        }
        refs.removeIf(r -> !AppPageAction.USECASE.equals(r.action.getActionKind())
                || isBlank(r.action.getAppUseCaseInstanceId()));
        return refs;
    }

    private static String keyOf(AppPageAction a, String fallback, int index) {
        return !isBlank(a.getActionId()) ? a.getActionId() : fallback + "_" + index;
    }

    /** Tool names allow [a-zA-Z0-9_-]. Claude Code calls a tool mcp__<server>__<name> and the whole must fit in 64, so 48 leaves room for the server name. */
    static final int MAX_NAME = 48;

    static String toolName(String page, String action) {
        String name = page.replaceAll("[^A-Za-z0-9_-]+", "_") + "__" + action.replaceAll("[^A-Za-z0-9_-]+", "_");
        return name.length() > MAX_NAME ? name.substring(0, MAX_NAME) : name;
    }

    private static String uniqueName(Set<String> taken, String base) {
        String name = base;
        for (int n = 2; taken.contains(name); n++) {
            String suffix = "_" + n;
            name = base.substring(0, Math.min(base.length(), MAX_NAME - suffix.length())) + suffix;
        }
        return name;
    }

    /** The page fields an action reads that the caller can sensibly supply: not built-ins, not page variables. */
    static Set<String> fieldsOf(AppPage page, AppPageAction action) {
        Set<String> fields = new LinkedHashSet<>();
        List<String> templates = new ArrayList<>(action.getInputs().values());
        templates.add(action.getEnvironmentOverride());
        templates.add(action.getBaseUrlOverride());
        templates.add(action.getTimeoutOverride());
        for (String template : templates) {
            if (template == null) continue;
            Matcher m = PLACEHOLDER.matcher(template);
            while (m.find()) fields.add(m.group(1) != null ? m.group(1) : m.group(2));
        }
        fields.removeAll(AppPageVariable.BUILT_IN);
        page.getVariables().forEach(v -> fields.remove(v.name()));
        return fields;
    }

    /** A JSON type when the field feeds one use case input whole, so a number stays a number. */
    private static String jsonType(AppPageAction action, AppUseCase useCase, String field) {
        for (Map.Entry<String, String> input : action.getInputs().entrySet()) {
            if (!field.equals(wholeField(input.getValue()))) continue;
            for (AppUseCaseInput declared : useCase.getAppUseCaseInputs()) {
                if (!declared.name().equals(input.getKey())) continue;
                return switch (declared.type()) {
                    case "number" -> "number";
                    case "boolean" -> "boolean";
                    case "json" -> null; // object, array or scalar — any JSON
                    default -> "string";
                };
            }
        }
        return "string";
    }

    /** True when the field appears in the template of an input the use case marks required. */
    private static boolean feedsRequired(AppPageAction action, AppUseCase useCase, String field) {
        for (AppUseCaseInput declared : useCase.getAppUseCaseInputs()) {
            if (!declared.required()) continue;
            String template = action.getInputs().get(declared.name());
            if (template == null) continue;
            Matcher m = PLACEHOLDER.matcher(template);
            while (m.find()) if (field.equals(m.group(1) != null ? m.group(1) : m.group(2))) return true;
        }
        return false;
    }

    private static String fieldDescription(AppPage page, String field) {
        for (AppPageControl c : page.getControls()) {
            if (field.equals(c.getFieldName()) && !isBlank(c.getLabel())) return c.getLabel() + " (page field " + field + ")";
        }
        return "Page field " + field;
    }

    // ── Template resolution ──────────────────────────────────────────────────

    /** Built-ins first, then the page's variables in order (each may use the ones above), then the caller's arguments. */
    static Map<String, Object> scopeFor(AppPage page, Map<String, Object> args) {
        LocalDateTime now = LocalDateTime.now();
        LocalDate today = now.toLocalDate();
        Map<String, Object> scope = new HashMap<>();
        scope.put("MACHINE", MachineName.local());
        scope.put("PID", String.valueOf(ProcessHandle.current().pid()));
        scope.put("DATESTAMP", today.format(STAMP));
        scope.put("PREVDATESTAMP", businessDay(today, -1).format(STAMP));
        scope.put("NEXTDATESTAMP", businessDay(today, 1).format(STAMP));
        scope.put("DATETIME", now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
        scope.put("DATETIMEHR", now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        scope.put("UUID", UUID.randomUUID().toString());
        scope.put("DEBUG", false);
        for (AppPageVariable v : page.getVariables()) {
            if (v != null && v.name() != null) scope.put(v.name(), resolveTemplate(v.value(), scope));
        }
        scope.putAll(args);
        return scope;
    }

    static LocalDate businessDay(LocalDate from, int step) {
        LocalDate d = from.plusDays(step);
        while (d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY) d = d.plusDays(step);
        return d;
    }

    /** Java twin of apppage.js#resolveInputs. */
    static Map<String, Object> resolveInputs(Map<String, String> inputs, Map<String, Object> scope) {
        Map<String, Object> resolved = new LinkedHashMap<>();
        inputs.forEach((name, template) -> {
            if (template == null) return;
            String whole = wholeField(template);
            if (whole != null) {
                // A lone placeholder keeps its value's type; one nothing answers to is left out entirely.
                if (scope.containsKey(whole)) resolved.put(name, scope.get(whole));
                return;
            }
            resolved.put(name, substitute(template, scope, true));
        });
        return resolved;
    }

    /** Java twin of apppage.js#resolveTemplate: missing values become empty. */
    static String resolveTemplate(String template, Map<String, Object> scope) {
        return template == null ? "" : substitute(template, scope, false);
    }

    private static String substitute(String template, Map<String, Object> scope, boolean keepMissing) {
        Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            Object value = scope.get(m.group(1) != null ? m.group(1) : m.group(2));
            String text = value == null ? (keepMissing ? m.group() : "") : asText(value);
            m.appendReplacement(sb, Matcher.quoteReplacement(text));
        }
        return m.appendTail(sb).toString();
    }

    private static String asText(Object value) {
        if (value instanceof Map || value instanceof List) {
            try { return new ObjectMapper().writeValueAsString(value); } catch (Exception e) { return String.valueOf(value); }
        }
        return String.valueOf(value);
    }

    private static String wholeField(String template) {
        Matcher m = WHOLE.matcher(template);
        return m.matches() ? (m.group(1) != null ? m.group(1) : m.group(2)) : null;
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
