package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.JsonataTransform;
import com.mycompany.batch.staticdataset.StaticDatasetService;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Loads and persists the whole App Catalog — apps, their environments, their use cases, the
 * instances that pin inputs to an environment, and the groups those instances are bundled into.
 *
 * <p>Each collection is a JSON array under {@code ${DATADIR}/appcatalog/}, following the same
 * read-at-startup / write-on-change pattern as
 * {@link com.mycompany.batch.staticdataset.StaticDatasetService} so definitions survive restarts
 * and are shared by everyone hitting the server.
 *
 * <p>Deletes cascade downward (app to environments/use cases to instances to group membership) so
 * the catalog can never end up holding an instance pointing at a use case that no longer exists.
 */
@Service
public class AppCatalogService {

    private static final String DIR = "appcatalog";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    /**
     * Only ever asked whether a dataset a page names is in the library. Held rather than looked up
     * per save so a page pointing at a dataset that has since been deleted is refused where the
     * message can name it, instead of running as a grid that is empty for no stated reason.
     */
    private final StaticDatasetService staticDatasets;
    /**
     * Only ever asked whether a name a page's transform points at is in the shared library — the
     * same reason and the same treatment as {@link #staticDatasets}. A page naming a JSONata that
     * has since been deleted is refused where the message can name it, rather than running a chain
     * with a step missing out of the middle of it.
     */
    private final JsonataLibraryService jsonataLibrary;

    private final List<AppDefinition>           apps         = new CopyOnWriteArrayList<>();
    private final List<AppEnvironment>          environments = new CopyOnWriteArrayList<>();
    private final List<AppUseCase>              useCases     = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstance>      instances    = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstanceGroup> groups       = new CopyOnWriteArrayList<>();
    private final List<AppPage>                 pages        = new CopyOnWriteArrayList<>();

    public AppCatalogService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader,
                             StaticDatasetService staticDatasets, JsonataLibraryService jsonataLibrary) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
        this.staticDatasets = staticDatasets;
        this.jsonataLibrary = jsonataLibrary;
    }

    @PostConstruct
    public void loadAll() {
        apps.addAll(read("appdefinitions.json", new TypeReference<List<AppDefinition>>() {}));
        environments.addAll(read("appenvironments.json", new TypeReference<List<AppEnvironment>>() {}));
        useCases.addAll(read("appusecases.json", new TypeReference<List<AppUseCase>>() {}));
        instances.addAll(read("appusecaseinstances.json", new TypeReference<List<AppUseCaseInstance>>() {}));
        groups.addAll(read("appusecaseinstancegroups.json", new TypeReference<List<AppUseCaseInstanceGroup>>() {}));
        pages.addAll(read("apppages.json", new TypeReference<List<AppPage>>() {}));
    }

    // -------------------------------------------------------------------------
    // Apps
    // -------------------------------------------------------------------------

    public List<AppDefinition> listApps() {
        return new ArrayList<>(apps);
    }

    public AppDefinition getApp(String appName) {
        return apps.stream().filter(a -> a.getAppName().equals(appName)).findFirst().orElse(null);
    }

    public synchronized AppDefinition saveApp(AppDefinition app) throws Exception {
        requireName(app.getAppName(), "appName");
        apps.removeIf(a -> a.getAppName().equals(app.getAppName()));
        apps.add(app);
        write("appdefinitions.json", apps);
        return app;
    }

    /** Removes the app along with every environment, use case and instance that referenced it. */
    public synchronized void deleteApp(String appName) throws Exception {
        apps.removeIf(a -> a.getAppName().equals(appName));
        environments.removeIf(e -> appName.equals(e.getAppName()));
        useCases.removeIf(u -> appName.equals(u.getAppName()));
        List<String> orphaned = instances.stream()
                .filter(i -> appName.equals(i.getAppName()))
                .map(AppUseCaseInstance::getAppUseCaseInstanceId)
                .collect(Collectors.toList());
        instances.removeIf(i -> appName.equals(i.getAppName()));
        groups.forEach(g -> g.getAppUseCaseInstanceIds().removeAll(orphaned));
        // Pages survive: they span apps, so one app going away leaves the rest of the page working.
        // An action left pointing at a deleted instance reports that when it runs.

        write("appdefinitions.json", apps);
        write("appenvironments.json", environments);
        write("appusecases.json", useCases);
        write("appusecaseinstances.json", instances);
        write("appusecaseinstancegroups.json", groups);
    }

    // -------------------------------------------------------------------------
    // Environments
    // -------------------------------------------------------------------------

    public List<AppEnvironment> listEnvironments(String appName) {
        return environments.stream()
                .filter(e -> appName == null || appName.equals(e.getAppName()))
                .collect(Collectors.toList());
    }

    public AppEnvironment getEnvironment(String appName, String environment) {
        return environments.stream()
                .filter(e -> appName.equals(e.getAppName()) && environment.equals(e.getEnvironment()))
                .findFirst().orElse(null);
    }

    public synchronized AppEnvironment saveEnvironment(AppEnvironment env) throws Exception {
        requireName(env.getAppName(), "appName");
        requireName(env.getEnvironment(), "environment");
        if (getApp(env.getAppName()) == null)
            throw new IllegalArgumentException("Unknown app: " + env.getAppName());

        environments.removeIf(e -> e.getAppName().equals(env.getAppName())
                && e.getEnvironment().equals(env.getEnvironment()));
        environments.add(env);
        write("appenvironments.json", environments);
        return env;
    }

    /**
     * Removes the environment, drops it from every instance that named it, and deletes only those
     * instances left with no environment at all. An instance running against three environments
     * survives losing one of them — deleting it outright would take the other two with it.
     */
    public synchronized void deleteEnvironment(String appName, String environment) throws Exception {
        environments.removeIf(e -> appName.equals(e.getAppName()) && environment.equals(e.getEnvironment()));

        List<String> orphaned = new ArrayList<>();
        for (AppUseCaseInstance instance : instances) {
            if (!appName.equals(instance.getAppName())) continue;

            List<String> remaining = instance.getEffectiveEnvironments();
            if (!remaining.remove(environment)) continue;

            if (remaining.isEmpty()) {
                orphaned.add(instance.getAppUseCaseInstanceId());
            } else {
                instance.setAppEnvironments(remaining);
                instance.setAppEnvironment(remaining.get(0));
            }
        }
        instances.removeIf(i -> orphaned.contains(i.getAppUseCaseInstanceId()));
        groups.forEach(g -> g.getAppUseCaseInstanceIds().removeAll(orphaned));

        write("appenvironments.json", environments);
        write("appusecaseinstances.json", instances);
        write("appusecaseinstancegroups.json", groups);
    }

    // -------------------------------------------------------------------------
    // Use cases
    // -------------------------------------------------------------------------

    public List<AppUseCase> listUseCases(String appName) {
        return useCases.stream()
                .filter(u -> appName == null || appName.equals(u.getAppName()))
                .collect(Collectors.toList());
    }

    public AppUseCase getUseCase(String appName, String useCaseName) {
        return useCases.stream()
                .filter(u -> appName.equals(u.getAppName()) && useCaseName.equals(u.getUseCaseName()))
                .findFirst().orElse(null);
    }

    public synchronized AppUseCase saveUseCase(AppUseCase useCase) throws Exception {
        requireName(useCase.getAppName(), "appName");
        requireName(useCase.getUseCaseName(), "useCaseName");
        if (getApp(useCase.getAppName()) == null)
            throw new IllegalArgumentException("Unknown app: " + useCase.getAppName());

        useCases.removeIf(u -> u.getAppName().equals(useCase.getAppName())
                && u.getUseCaseName().equals(useCase.getUseCaseName()));
        useCases.add(useCase);
        write("appusecases.json", useCases);
        return useCase;
    }

    /** Removes the use case and every instance of it. */
    public synchronized void deleteUseCase(String appName, String useCaseName) throws Exception {
        useCases.removeIf(u -> appName.equals(u.getAppName()) && useCaseName.equals(u.getUseCaseName()));
        List<String> orphaned = instances.stream()
                .filter(i -> appName.equals(i.getAppName()) && useCaseName.equals(i.getAppUseCaseName()))
                .map(AppUseCaseInstance::getAppUseCaseInstanceId)
                .collect(Collectors.toList());
        instances.removeIf(i -> orphaned.contains(i.getAppUseCaseInstanceId()));
        groups.forEach(g -> g.getAppUseCaseInstanceIds().removeAll(orphaned));

        write("appusecases.json", useCases);
        write("appusecaseinstances.json", instances);
        write("appusecaseinstancegroups.json", groups);
    }

    // -------------------------------------------------------------------------
    // Instances
    // -------------------------------------------------------------------------

    public List<AppUseCaseInstance> listInstances(String appName) {
        return instances.stream()
                .filter(i -> appName == null || appName.equals(i.getAppName()))
                .collect(Collectors.toList());
    }

    public AppUseCaseInstance getInstance(String instanceId) {
        return instances.stream()
                .filter(i -> i.getAppUseCaseInstanceId().equals(instanceId))
                .findFirst().orElse(null);
    }

    /**
     * Saves an instance, generating the id when the caller did not supply one (i.e. on create).
     *
     * <p>An instance may name several environments. They are all validated, and the first becomes
     * {@code appEnvironment} — the single-environment field every older reader still uses, and the
     * one the generated id is built from.
     */
    public synchronized AppUseCaseInstance saveInstance(AppUseCaseInstance instance) throws Exception {
        requireName(instance.getAppName(), "appName");
        requireName(instance.getAppUseCaseName(), "appUseCaseName");
        if (getUseCase(instance.getAppName(), instance.getAppUseCaseName()) == null)
            throw new IllegalArgumentException("Unknown use case: "
                    + instance.getAppName() + "/" + instance.getAppUseCaseName());

        List<String> environments = instance.getEffectiveEnvironments();
        if (environments.isEmpty()) throw new IllegalArgumentException("appEnvironment is required");
        for (String environment : environments) {
            if (getEnvironment(instance.getAppName(), environment) == null)
                throw new IllegalArgumentException("Unknown environment: "
                        + instance.getAppName() + "/" + environment);
        }
        instance.setAppEnvironments(environments);
        instance.setAppEnvironment(environments.get(0));

        if (instance.getAppUseCaseInstanceId() == null || instance.getAppUseCaseInstanceId().isBlank()) {
            instance.setAppUseCaseInstanceId(newInstanceId(instance));
        }
        instances.removeIf(i -> i.getAppUseCaseInstanceId().equals(instance.getAppUseCaseInstanceId()));
        instances.add(instance);
        write("appusecaseinstances.json", instances);
        return instance;
    }

    public synchronized void deleteInstance(String instanceId) throws Exception {
        instances.removeIf(i -> i.getAppUseCaseInstanceId().equals(instanceId));
        groups.forEach(g -> g.getAppUseCaseInstanceIds().remove(instanceId));
        write("appusecaseinstances.json", instances);
        write("appusecaseinstancegroups.json", groups);
    }

    /**
     * Readable-but-unique id: app-usecase-env plus a short random suffix, so the ids showing up in
     * a group are recognisable at a glance instead of being opaque UUIDs.
     */
    private String newInstanceId(AppUseCaseInstance instance) {
        String base = (instance.getAppName() + "-" + instance.getAppUseCaseName() + "-"
                + instance.getAppEnvironment()).replaceAll("[^A-Za-z0-9\\-_]", "_");
        return base + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    // -------------------------------------------------------------------------
    // Instance groups
    // -------------------------------------------------------------------------

    public List<AppUseCaseInstanceGroup> listGroups() {
        return new ArrayList<>(groups);
    }

    public AppUseCaseInstanceGroup getGroup(String groupName) {
        return groups.stream().filter(g -> g.getGroupName().equals(groupName)).findFirst().orElse(null);
    }

    public synchronized AppUseCaseInstanceGroup saveGroup(AppUseCaseInstanceGroup group) throws Exception {
        requireName(group.getGroupName(), "groupName");
        for (String id : group.getAppUseCaseInstanceIds()) {
            if (getInstance(id) == null) throw new IllegalArgumentException("Unknown instance id: " + id);
        }
        groups.removeIf(g -> g.getGroupName().equals(group.getGroupName()));
        groups.add(group);
        write("appusecaseinstancegroups.json", groups);
        return group;
    }

    public synchronized void deleteGroup(String groupName) throws Exception {
        groups.removeIf(g -> g.getGroupName().equals(groupName));
        write("appusecaseinstancegroups.json", groups);
    }

    // -------------------------------------------------------------------------
    // Pages
    // -------------------------------------------------------------------------

    public List<AppPage> listPages(String appName) {
        return pages.stream()
                .filter(p -> appName == null || appName.equals(p.getAppName()))
                .collect(Collectors.toList());
    }

    public AppPage getPage(String pageName) {
        return pages.stream().filter(p -> pageName.equals(p.getPageName())).findFirst().orElse(null);
    }

    /**
     * Saves a page after checking it hangs together: every control is addressable, every value
     * control has a distinct field name, and every instance and target a select, button or link
     * points at really exists. A page that half-resolves is worse than one that refuses to save —
     * the parts that do resolve make it look like it works.
     */
    public synchronized AppPage savePage(AppPage page) throws Exception {
        requireName(page.getPageName(), "pageName");
        validateControls(page);

        pages.removeIf(p -> p.getPageName().equals(page.getPageName()));
        pages.add(page);
        write("apppages.json", pages);
        return page;
    }

    public synchronized void deletePage(String pageName) throws Exception {
        pages.removeIf(p -> pageName.equals(p.getPageName()));
        write("apppages.json", pages);
    }

    /**
     * Everything in the catalog that names a shared JSONata — each page transform that references it
     * and each use case whose response transform does. Asked before it is deleted, and shown beside
     * it in the library, so "is this still used" has an answer that names the pages rather than an
     * answer somebody has to go and look for.
     *
     * <p>Each entry says {@code where} it is (the page or use case) and {@code what} inside it
     * (the transform's name), which is enough to go and find it.
     */
    public List<Map<String, String>> jsonataUsage(String name) {
        List<Map<String, String>> usage = new ArrayList<>();
        if (name == null || name.isBlank()) return usage;

        for (AppPage page : pages) {
            for (AppPageTransform transform : page.getTransforms()) {
                if (name.equals(transform.getJsonataRef()))
                    usage.add(Map.of("kind", "page", "where", String.valueOf(page.getPageName()),
                                     "what", String.valueOf(transform.getName())));
            }
        }
        for (AppUseCase useCase : useCases) {
            JsonataTransform transform = useCase.getJsonataTransform();
            if (transform == null) continue;
            if (name.equals(JsonataLibraryService.refName(transform.value()))
                    || name.equals(JsonataLibraryService.refName(transform.key())))
                usage.add(Map.of("kind", "usecase",
                                 "where", useCase.getAppName() + " / " + useCase.getUseCaseName(),
                                 "what", "response transform"));
        }
        return usage;
    }

    /**
     * Control types that hold a value the operator supplies, and so need a field name. A multi-select
     * is one of them: it holds the operator's picks as one comma-separated value, so everything that
     * reads a control by field name — an action's {@code ${field}}, a mandatory check, an assignment
     * — reads it without knowing it came from a list rather than a box.
     */
    private static final List<String> VALUE_TYPES =
            List.of("text", "textarea", "number", "date", "hidden", "select", "checkbox", "multiselect");

    /** Control types that run use case instances when clicked. */
    private static final List<String> ACTION_TYPES = List.of("button", "link");

    /**
     * Control types an action can put a response into. A link is here as well as in
     * {@link #ACTION_TYPES}, and the two mean different halves of it: it runs actions when clicked,
     * and what an action binds into it is the address it points at.
     *
     * <p>A pie chart is here without being in {@link #ACTION_TYPES}, and is in
     * {@link #TRIGGERLESS_TYPES} as well, which is not a contradiction: nothing sets a chart off,
     * and an action can still fill it. The rows it binds become the wedges, named and sized by two
     * fields of each row. Mirrors TARGET_TYPES in apppage.js.
     */
    private static final List<String> TARGET_TYPES =
            List.of("grid", "select", "multiselect", "text", "textarea", "link", "pie", "piegrid", "bar",
                    "timeseries", "line");

    /**
     * Control types another control can write a value into. Wider than {@link #TARGET_TYPES}: a
     * response needs somewhere that can hold rows or be read back, while an assignment is only a
     * value being put somewhere — every value control takes one, and a label takes one to show.
     */
    private static final List<String> ASSIGN_TYPES =
            List.of("text", "textarea", "number", "date", "hidden", "select", "multiselect", "checkbox", "label");

    /**
     * Control types nothing sets off, and which therefore have nothing to set or run. A hidden field
     * belongs here with the grids, the labels and the tab sets: it carries a value the rest of the
     * page reads, but it is never drawn, so it is never clicked or changed — and a value arriving in
     * it deliberately does not fire its own trigger either. An assignment written on one would sit in
     * the saved page looking wired up and never once run.
     *
     * <p>A panel belongs here with the tab sets, and for the same reason: it is a box the controls
     * inside it are drawn in, and nobody clicks the box.
     *
     * <p>A multi-button belongs here for a different reason, and it is worth being explicit about.
     * It is clicked — that is all it does — but the click opens a menu rather than running anything,
     * and every entry on that menu is its own trigger with its own actions. So there is nothing for
     * the <em>control</em> to set or run: wiring written at this level would be wiring the operator
     * could never reach, whichever entry they went on to pick. See {@link #validateMenuOptions}.
     */
    private static final List<String> TRIGGERLESS_TYPES = List.of("grid", "label", "tabs", "panel", "hidden",
            "multibutton", "pie", "piegrid", "bar", "timeseries", "line", "page");

    private void validateControls(AppPage page) {
        List<String> controlIds = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<String> transformNames = validateTransforms(page);
        List<String> actionIds = validatePageActions(page, transformNames);
        List<String> variableNames = validateVariables(page);

        for (AppPageControl control : page.getControls()) {
            if (control.getControlId() == null || control.getControlId().isBlank()) {
                control.setControlId("c-" + UUID.randomUUID().toString().substring(0, 8));
            }
            if (controlIds.contains(control.getControlId()))
                throw new IllegalArgumentException("Duplicate control id: " + control.getControlId());
            controlIds.add(control.getControlId());

            String where = "Control '" + describe(control) + "'";
            if (VALUE_TYPES.contains(control.getType())) {
                requireName(control.getFieldName(), where + " field name");
                if (fieldNames.contains(control.getFieldName()))
                    throw new IllegalArgumentException("Duplicate field name: " + control.getFieldName());
                fieldNames.add(control.getFieldName());
                checkFieldNameFree(control, variableNames, where);
            }
            validateSlices(control, where);
            validateSliceColors(control, where);
            validateLinkUrl(control, where);
            validateLinkPage(control, where);
            validateChildPage(page, control, where, this::getPage);
            validateDatasetName(control, where);
            validateRowErrorExpression(control, where);
            validateSliceErrorExpression(control, where);
            validateDisplayFilterExpression(control, where);
            validateGridStatus(control, variableNames, where);
            validateHideOnRun(page, control, where);
            validateDateFormat(control, where);
            validateLeadColumns(control, where);
            validateHiddenColumns(control, where);
            validateWrapText(control, where);
            if (isSelect(control.getType()) && control.getOptionSource() != null) {
                AppPageOptionSource source = control.getOptionSource();
                if ("USECASE".equals(source.getMode())) {
                    requireInstance(source.getAppUseCaseInstanceId(), where + " option source");
                } else if ("ENVIRONMENTS".equals(source.getMode())) {
                    if (source.getAppName() == null || source.getAppName().isBlank())
                        throw new IllegalArgumentException(where + " option source names no app");
                    if (getApp(source.getAppName()) == null)
                        throw new IllegalArgumentException(where + " option source names an unknown app: " + source.getAppName());
                } else if ("DATASET".equals(source.getMode())) {
                    requireDataset(source.getDatasetName(), where + " option source");
                }
            }
        }
        validateGridStatusNames(page);

        // Ids for the inline actions too, unique across the whole page and not only within the
        // library: an action is waited for by id, and two answering to one would make "which of them
        // does this wait for" unanswerable. Collected as they are minted, so an inline id can never
        // land on a library one.
        List<String> seenActionIds = new ArrayList<>(actionIds);
        Map<String, AppPageAction> library = new LinkedHashMap<>();
        for (AppPageAction action : page.getActions()) library.put(action.getActionId(), action);

        for (AppPageControl control : page.getControls()) {
            for (String id : control.getActionIds()) {
                if (!actionIds.contains(id))
                    throw new IllegalArgumentException("Control '" + describe(control)
                            + "' triggers an action that is not on this page: " + id);
            }
            validateAssignments(page, control);
            validateColumnLinks(page, control, actionIds);
            validateRowClick(page, control, actionIds);
            validateFocusControl(page, control);
            if (!ACTION_TYPES.contains(control.getType())) continue;
            // What an action written here may wait for: the library, plus this control's own list.
            // Not narrowed to the actions the control currently triggers — detaching a page action
            // leaves the wait unmet rather than invalid, and the designer says so where the two are
            // wired together, which is the place it can be put right.
            Map<String, AppPageAction> reachable = new LinkedHashMap<>(library);
            for (AppPageAction action : control.getActions()) {
                if (action.getActionId() == null || action.getActionId().isBlank()) {
                    action.setActionId("a-" + UUID.randomUUID().toString().substring(0, 8));
                }
                if (seenActionIds.contains(action.getActionId()))
                    throw new IllegalArgumentException("Duplicate action id: " + action.getActionId());
                seenActionIds.add(action.getActionId());
                reachable.put(action.getActionId(), action);
                validateAction(page, action, transformNames, "Action '" + actionName(action, describe(control)) + "'");
            }
            validateDependencies(control.getActions(), reachable, "Action",
                    " on control '" + describe(control) + "'");
        }

        for (String id : page.getOnLoadActionIds()) {
            if (!actionIds.contains(id))
                throw new IllegalArgumentException("The page's on-load list names an action that is not on this page: " + id);
        }

        for (AppPageControl control : page.getControls()) validateMenuOptions(control, actionIds);

        validateTabs(page);
        validateChartControls(page);
    }

    /**
     * A pie's slices: a name and a size each, and the size has to be a number.
     *
     * <p>The size is typed into a text box like everything else on a control, so "12 orders" or an
     * empty box are both things the designer can leave behind — and both are angles that cannot be
     * worked out, which would leave the saved page with a slice the chart silently drops. It is
     * refused here instead, where the message can say which slice and what it says.
     *
     * <p>Negative sizes go the same way: a pie shows each slice's share of the whole, and a share
     * below zero has no wedge to be drawn as. Zero is allowed — a slice that is genuinely nothing
     * this time still belongs in the legend beside the ones that are not.
     */
    private static final Pattern SLICE_NUMBER = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private void validateSlices(AppPageControl control, String where) {
        if (control.getSlices().isEmpty()) return;
        if (!"pie".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a pie chart has slices");
        List<String> names = new ArrayList<>();
        for (AppPageOption slice : control.getSlices()) {
            requireName(slice.key(), where + " slice name");
            if (names.contains(slice.key()))
                throw new IllegalArgumentException(where + " has two slices named " + slice.key());
            names.add(slice.key());
            String text = slice.value() == null ? "" : slice.value().trim();
            // Matched before it is parsed, and against plainly-a-number rather than against whatever
            // Double.parseDouble will take: it accepts "1d" and "0x1p3", which the browser drawing
            // the chart does not, and a page that saves with a slice the chart then leaves out is the
            // one thing this check exists to prevent. Mirrors #sliceNumber in apppage.js.
            if (!SLICE_NUMBER.matcher(text).matches())
                throw new IllegalArgumentException(where + " slice '" + slice.key() + "' has a value that is not a number: "
                        + (text.isBlank() ? "(blank)" : text));
            double size = Double.parseDouble(text);
            if (!Double.isFinite(size) || size < 0)
                throw new IllegalArgumentException(where + " slice '" + slice.key()
                        + "' has a value a pie cannot draw: " + slice.value());
        }
    }

    /**
     * What a slice colour may be written as: a name, a #hex, or an rgb()/hsl() function — the same
     * characters {@code cssColor} in apppage.js keeps, and no others.
     *
     * <p>The value goes into a {@code style} attribute on the running page, so anything that could
     * close that attribute and start something else has no business being stored here. Checked at the
     * save rather than scrubbed at the draw for the reason every other check on this screen is: the
     * designer finds out where they can fix it, instead of an operator finding a wedge that came out
     * the wrong colour for no stated reason.
     */
    private static final Pattern CSS_COLOR = Pattern.compile("[#a-zA-Z0-9\\s.,%()-]+");

    /**
     * A pie's named colours: only a pie has them, each names a slice once, and each says what colour
     * that slice is drawn in.
     *
     * <p>A name that matches no slice is deliberately fine and is not checked — it cannot be. The
     * whole point of naming a colour is that the slices arrive from a call: a page is saved knowing
     * it will have an UP wedge and a DOWN wedge long before any run proves it, and refusing the
     * colour until the wedge exists would make the setting unusable on exactly the charts it is for.
     */
    static void validateSliceColors(AppPageControl control, String where) {
        if (control.getSliceColors().isEmpty()) return;
        if (!"pie".equals(control.getType()) && !"piegrid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a pie chart colours its slices by name");
        List<String> named = new ArrayList<>();
        for (AppPageOption colour : control.getSliceColors()) {
            requireName(colour.key(), where + " slice colour name");
            String key = colour.key().trim().toLowerCase();
            if (named.contains(key))
                throw new IllegalArgumentException(where + " gives the slice '" + colour.key() + "' two colours");
            named.add(key);
            String value = colour.value() == null ? "" : colour.value().trim();
            if (value.isEmpty())
                throw new IllegalArgumentException(where + " slice '" + colour.key() + "' is named but given no colour");
            if (!CSS_COLOR.matcher(value).matches())
                throw new IllegalArgumentException(where + " slice '" + colour.key()
                        + "' has a colour that is not a CSS colour: " + value);
        }
    }

    /**
     * A grid's static dataset, when it was given one. Only a grid has one — a select reaches a
     * dataset through its option source instead, where it can also say which columns are the key and
     * the label — so a dataset name left behind on a control that has since become something else is
     * refused rather than saved as a setting nothing would ever read.
     */
    private void validateDatasetName(AppPageControl control, String where) {
        String name = control.getDatasetName();
        if (name == null || name.isBlank()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid is filled straight from a static dataset");
        requireDataset(name, where);
    }

    /**
     * A grid that holds its rows without being drawn — see {@link AppPageControl#isHideOnRun()}.
     *
     * <p>Only a grid, for the same reason only a grid has a static dataset: hiding a control that
     * takes a value would leave a field the operator is asked for and cannot fill, and a hidden field
     * is what that page already wants. A setting left behind on a control that has since become
     * something else is refused rather than saved as one nothing would ever read.
     *
     * <p>And not a grid that lives in a tab set. That grid is drawn by the set rather than by the
     * layout, so hiding it would leave a tab with nothing behind it — which is not a hidden grid but
     * a broken tab set. Take it out of the set and it hides as any other grid does.
     */
    static void validateHideOnRun(AppPage page, AppPageControl control, String where) {
        if (!control.isHideOnRun()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid can hold its rows without being drawn"
                    + (VALUE_TYPES.contains(control.getType())
                        ? "; a control that carries a value hides by being a hidden field instead" : ""));
        boolean inTabs = page.getControls().stream()
                .anyMatch(c -> "tabs".equals(c.getType()) && c.getTabControlIds().contains(control.getControlId()));
        if (inTabs)
            throw new IllegalArgumentException(where + " is a tab of a tab set, which draws it — so hiding it would "
                    + "leave a tab with nothing behind it. Take it out of the tab set, or untick hidden.");
    }

    /**
     * The control a trigger scrolls to once it has finished — see
     * {@link AppPageControl#getFocusControlId()}.
     *
     * <p>Held to naming something the operator can actually end up looking at. A control that is no
     * longer on the page, a hidden field, or a grid that holds its rows without being drawn would
     * all save as a page whose button quietly scrolls nowhere, and "nothing happened" is the one
     * outcome there is no way to tell from a setting that was never made.
     *
     * <p>A control inside a tab set or a panel is a perfectly good thing to name: the running page
     * opens the tab it is behind on the way to it, which is most of the point of the setting.
     */
    static void validateFocusControl(AppPage page, AppPageControl control) {
        String focus = control.getFocusControlId();
        if (focus == null || focus.isBlank()) return;
        String where = "Control '" + describe(control) + "'";
        if (focus.equals(control.getControlId()))
            throw new IllegalArgumentException(where + " is set to scroll to itself after it runs, "
                    + "which is where the operator already is. Name the grid the run fills instead.");
        AppPageControl target = controlById(page, focus);
        if (target == null)
            throw new IllegalArgumentException(where + " scrolls to a control that is not on this page: " + focus);
        if ("hidden".equals(target.getType()))
            throw new IllegalArgumentException(where + " scrolls to the hidden field '" + describe(target)
                    + "', which is never drawn. Name something the operator can see.");
        if (target.isHideOnRun())
            throw new IllegalArgumentException(where + " scrolls to '" + describe(target)
                    + "', which is set to hold its rows without being drawn. Name a grid that is on screen, "
                    + "or untick hidden on that one.");
    }

    /**
     * A date control's format: the form its value is written in everywhere except the box — see
     * {@link AppPageControl#getDateFormat()}.
     *
     * <p>Only a date control has one, so a format left behind on a control that has since become
     * something else is refused rather than saved as a setting nothing would ever read — the same
     * rule the static dataset above follows, for the same reason.
     *
     * <p>And it has to be a format the browser can actually write, since the browser is what applies
     * it. One it does not know would leave a control quietly publishing ISO while its panel said
     * otherwise, which is the one outcome worth a save failing over: the value goes into a call.
     */
    static void validateDateFormat(AppPageControl control, String where) {
        String format = control.getDateFormat();
        if (format == null || format.isBlank()) return;
        if (!"date".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a date control has a date format");
        if (!AppPageControl.DATE_FORMATS.contains(format))
            throw new IllegalArgumentException(where + " has a date format that is not one of "
                    + String.join(", ", AppPageControl.DATE_FORMATS) + ": " + format);
    }

    /**
     * A grid's leading columns — see {@link AppPageControl#getLeadColumns()}.
     *
     * <p>Only a grid has columns to order, so a list left behind on a control that has since become
     * something else is refused rather than kept as a setting nothing would read.
     *
     * <p>Two other ways of writing a list that cannot mean what it says. A column named twice has no
     * second place to go, so the repeat can only be a typo for another column. And a grid that fixes
     * its {@link AppPageControl#getColumns() columns} shows those and nothing else, so naming one
     * outside that set asks for a column first that will never be there at all — which is exactly the
     * mistake that looks like the setting being ignored. Names are matched without regard to case,
     * the way the browser matches them against the fields of a row.
     */
    static void validateLeadColumns(AppPageControl control, String where) {
        List<String> lead = control.getLeadColumns();
        if (lead.isEmpty()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid has columns to put in order");
        List<String> seen = new ArrayList<>();
        for (String column : lead) {
            if (column == null || column.isBlank())
                throw new IllegalArgumentException(where + " names a blank column among the ones to show first");
            String name = column.trim().toLowerCase();
            if (seen.contains(name))
                throw new IllegalArgumentException(where + " names '" + column.trim()
                        + "' twice among the columns to show first, and a column has only one place to go");
            seen.add(name);
            boolean fixed = !control.getColumns().isEmpty();
            if (fixed && control.getColumns().stream().noneMatch(c -> c != null && c.trim().equalsIgnoreCase(name)))
                throw new IllegalArgumentException(where + " asks to show '" + column.trim()
                        + "' first, but its columns are fixed to " + String.join(", ", control.getColumns())
                        + " — so that column would never be on the grid at all");
        }
    }

    /**
     * A grid's row check, when it was given one: {@code STATUS != SUCCESS || RECORDCOUNT = 0}.
     *
     * <p>Only a grid has rows to judge, so an expression left behind on a control that has since
     * become something else is refused rather than saved as a setting nothing would ever read — the
     * same rule the static dataset above follows, for the same reason.
     *
     * <p>And it has to be an expression that can be read. The browser is what evaluates it, over the
     * rows as they arrive, and an expression it cannot parse there leaves a grid that judges nothing
     * while looking as though it does — the one outcome this check exists to prevent. Refused here,
     * the message can say which grid and what about the expression could not be read.
     */
    static void validateRowErrorExpression(AppPageControl control, String where) {
        String expression = control.getRowErrorExpression();
        if (expression == null || expression.isBlank()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid checks its rows");
        try {
            AppPageRowCheck.check(expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + " has a row check that cannot be read: "
                    + e.getMessage() + " — in '" + expression + "'");
        }
    }

    /**
     * A pie's slice check, when it was given one: {@code NAME != SUCCESS && VALUE > 0}.
     *
     * <p>The same expression a grid judges its rows with, turned on the one thing a pie has instead
     * of rows — its wedges. Only a pie draws any, so one left behind on a control that has since
     * become something else is refused rather than saved as a setting nothing would ever read, and it
     * has to be readable here for the same reason a grid's does: the browser is what evaluates it, and
     * one it cannot parse would leave a chart judging nothing while looking as though it did.
     */
    static void validateSliceErrorExpression(AppPageControl control, String where) {
        String expression = control.getSliceErrorExpression();
        if (expression == null || expression.isBlank()) return;
        if (!"pie".equals(control.getType()) && !"piegrid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a pie checks its slices");
        checkExpression(expression, where + " has a slice check");
    }

    /**
     * A grid's display filter: the same rules as its row check above — only a grid has rows to keep,
     * and an expression the browser cannot read would show every row while looking as though it
     * filtered them.
     */
    static void validateDisplayFilterExpression(AppPageControl control, String where) {
        String expression = control.getDisplayFilterExpression();
        if (expression == null || expression.isBlank()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid filters its rows");
        checkExpression(expression, where + " has a display filter");
    }

    /**
     * The error check and display filter a tab-per-row fan-out puts every one of its tabs' grids
     * through. Only read under {@link AppPageAction#TABS}, so ones left on an action that has since
     * been switched to collecting into a grid are harmless and not refused; but whatever is there has
     * to be readable, for the same reason a grid's own are — see {@link #validateRowErrorExpression}.
     */
    static void validateActionRowErrorExpression(AppPageAction action, String where) {
        checkExpression(action.getRowErrorExpression(), where + " has an error check");
        checkExpression(action.getDisplayFilterExpression(), where + " has a display filter");
    }

    private static void checkExpression(String expression, String what) {
        if (expression == null || expression.isBlank()) return;
        try {
            AppPageRowCheck.check(expression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(what + " that cannot be read: "
                    + e.getMessage() + " — in '" + expression + "'");
        }
    }

    /**
     * A grid's status: the row-count condition it is judged by, and the variable that verdict is
     * published under. Only a grid has rows to count; the condition has to be one the browser knows;
     * and the variable has to be spellable in a template and not already mean something else there —
     * a page variable or a built-in. Clashes with field names and other grids need every control
     * seen first — see {@link #validateGridStatusNames}.
     */
    static void validateGridStatus(AppPageControl control, List<String> variableNames, String where) {
        String condition = control.getStatusCondition();
        String variable  = control.getStatusVariable();
        if (condition == null && variable == null) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType() + " — only a grid has a status");
        if (condition != null && !AppPageControl.STATUS_CONDITIONS.contains(condition))
            throw new IllegalArgumentException(where + " has a status condition that is not one of "
                    + String.join(", ", AppPageControl.STATUS_CONDITIONS) + ": " + condition);
        if (variable == null) return;
        if (condition == null)
            throw new IllegalArgumentException(where + " names a status variable but has no status condition to set it by");
        if (!AppPageVariable.isLegalName(variable))
            throw new IllegalArgumentException(where + " status variable '" + variable + "' is not a name a template can spell —"
                    + " letters, digits and underscores only, starting with a letter or an underscore");
        if (variableNames.contains(variable) || AppPageVariable.BUILT_IN.contains(variable.toUpperCase()))
            throw new IllegalArgumentException(where + " status variable '" + variable + "' is already a page variable");
    }

    /** Every grid's status variable against every field name and every other grid's, once all are known. */
    static void validateGridStatusNames(AppPage page) {
        List<String> fieldNames = new ArrayList<>();
        for (AppPageControl control : page.getControls()) {
            if (VALUE_TYPES.contains(control.getType()) && control.getFieldName() != null) fieldNames.add(control.getFieldName());
        }
        List<String> seen = new ArrayList<>();
        for (AppPageControl control : page.getControls()) {
            String variable = control.getStatusVariable();
            if (variable == null) continue;
            if (fieldNames.contains(variable))
                throw new IllegalArgumentException("Control '" + describe(control) + "' status variable '" + variable
                        + "' is already a control's field name");
            if (seen.contains(variable))
                throw new IllegalArgumentException("Two grids publish their status as the same variable: " + variable);
            seen.add(variable);
        }
    }

    /** A dataset a page names has to be one the library actually holds. */
    private void requireDataset(String datasetName, String where) {
        if (datasetName == null || datasetName.isBlank())
            throw new IllegalArgumentException(where + " names no static dataset");
        if (staticDatasets.get(datasetName) == null)
            throw new IllegalArgumentException(where + " names an unknown static dataset: " + datasetName);
    }

    /**
     * A link's own address, when the designer gave it one. Only {@code http}, {@code https} and a
     * path rooted on this server are allowed through, and it is the same rule the running page
     * applies to an address an action binds — the value ends up in an href either way, and a
     * {@code javascript:} one there would be whatever was typed running as the page.
     *
     * <p>Refused at the save rather than left to the browser, which drops such an address silently:
     * the page would store a link that looked wired up and went nowhere, with nothing anywhere to
     * say why. Here the message can name the link and the address it was given.
     */
    /** Whether this control picks from a list of options — one of them, or several. */
    static boolean isSelect(String type) {
        return "select".equals(type) || "multiselect".equals(type);
    }

    static void validateLinkUrl(AppPageControl control, String where) {
        if (!"link".equals(control.getType())) return;
        String url = control.getDefaultValue() == null ? "" : control.getDefaultValue().trim();
        if (url.isEmpty()) return;
        // An address that opens with a placeholder has no scheme to check yet: the page supplies one
        // when it runs, and the browser holds the finished address to exactly this rule before it
        // ever reaches an href. See AppPageControl#getDefaultValue and apppage.js#linkAddress.
        if (startsWithPlaceholder(url)) return;
        // A leading "//" is another host, not a path on this one, so it is held to the same rule as
        // any other absolute address rather than let through as if it were rooted here.
        boolean rooted   = url.startsWith("/") && !url.startsWith("//");
        boolean absolute = url.regionMatches(true, 0, "http://", 0, 7)
                        || url.regionMatches(true, 0, "https://", 0, 8);
        if (!rooted && !absolute)
            throw new IllegalArgumentException(where + " has a URL a link cannot point at: " + url
                    + " — http, https, or a path on this server.");
    }

    /**
     * Whether an address opens with a {@code ${name}} or {@code $name} placeholder, which is the one
     * case where the scheme it will end up with cannot be told from what was typed.
     *
     * <p>Only the opening of the address is the question. {@code https://reports/orders/${orderId}}
     * says what it is and is checked like any other address; {@code ${reportHost}/orders/${orderId}}
     * does not, and refusing it would stop a link pointing at a host the operator picks — which is
     * most of why placeholders were let into a link's address at all.
     */
    private static boolean startsWithPlaceholder(String url) {
        if (url.startsWith("${")) return url.indexOf('}') > 2;
        return url.length() > 1 && url.charAt(0) == '$'
                && (Character.isLetter(url.charAt(1)) || url.charAt(1) == '_');
    }

    /**
     * The columns a grid leaves undrawn — see {@link AppPageControl#getHiddenColumns()}.
     *
     * <p>Only a grid has columns, so a list left behind on a control that has since become something
     * else is refused rather than saved as a setting nothing would ever read — the rule every other
     * grid-only setting here follows.
     *
     * <p>And a grid cannot both fix its columns and hide one of them. Naming a column under both
     * {@link AppPageControl#getColumns()} and here is two instructions that contradict each other,
     * and either reading of it would be a guess at which the designer meant; said out loud, it takes
     * a moment to fix, and the alternative is a column missing from a screen with nothing anywhere
     * to say why.
     */
    static void validateHiddenColumns(AppPageControl control, String where) {
        List<String> hidden = control.getHiddenColumns();
        if (hidden.isEmpty()) return;
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid has columns to hide");
        List<String> seen = new ArrayList<>();
        for (String column : hidden) {
            if (column == null || column.isBlank())
                throw new IllegalArgumentException(where + " names a blank column among the ones to hide");
            String name = column.trim().toLowerCase();
            if (seen.contains(name))
                throw new IllegalArgumentException(where + " names '" + column.trim()
                        + "' twice among the columns to hide");
            seen.add(name);
            if (control.getColumns().stream().anyMatch(c -> c != null && c.trim().equalsIgnoreCase(name)))
                throw new IllegalArgumentException(where + " both fixes its columns to include '"
                        + column.trim() + "' and asks to hide it — drop it from one list or the other");
            if (control.getLeadColumns().stream().anyMatch(c -> c != null && c.trim().equalsIgnoreCase(name)))
                throw new IllegalArgumentException(where + " asks to show '" + column.trim()
                        + "' first and to hide it — drop it from one list or the other");
        }
    }

    /**
     * Wrapped text is a way of drawing rows, so only something with rows may ask for it — see
     * {@link AppPageControl#isWrapText()}.
     */
    static void validateWrapText(AppPageControl control, String where) {
        if (control.isWrapText() && !"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid has rows to wrap");
    }

    /**
     * The other page a link opens, when it was pointed at one. Only a link may name a page — a name
     * left behind on a control that has since become something else is a setting nothing would ever
     * read — and the page it names has to be one this catalog holds.
     *
     * <p>Checked here rather than left to the click, which is the whole reason the page is named
     * rather than written out as a URL: a link to a page that was renamed or deleted looks exactly
     * like a working one until somebody follows it and lands on "no such page". Refusing the save
     * puts the problem in front of whoever can still fix it.
     */
    private void validateLinkPage(AppPageControl control, String where) {
        String name = control.getLinkPageName();
        if (name == null || name.isBlank()) return;
        if (!"link".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a link opens another page");
        if (getPage(name) == null)
            throw new IllegalArgumentException(where + " opens a page that is not in this catalog: " + name);
    }

    /**
     * The page a page control runs inside this one. Only a page control may name one, and a page
     * control has to: without a page it is an empty frame. The page has to be in this catalog, and
     * may not be the page being saved or lead back to it through its own child pages — each running
     * page opens its children, so a loop would open frames inside frames until the browser gave up.
     *
     * <p>The page being saved is taken as given rather than looked up, since what is on disk is the
     * version this save is replacing; every other page in the walk is read through {@code pages}.
     */
    static void validateChildPage(AppPage page, AppPageControl control, String where,
                                  Function<String, AppPage> pages) {
        String name = control.getChildPageName();
        if (!"page".equals(control.getType())) {
            if (name != null)
                throw new IllegalArgumentException(where + " is a " + control.getType()
                        + " — only a page control runs another page inside it");
            return;
        }
        if (name == null)
            throw new IllegalArgumentException(where + " is a page control but names no page to run");
        if (name.equals(page.getPageName()))
            throw new IllegalArgumentException(where + " runs this same page inside itself");
        if (pages.apply(name) == null)
            throw new IllegalArgumentException(where + " runs a page that is not in this catalog: " + name);

        // Walk everything reachable from the child; reaching this page again is a loop.
        Deque<List<String>> todo = new ArrayDeque<>();
        todo.push(List.of(page.getPageName(), name));
        Set<String> seen = new HashSet<>();
        while (!todo.isEmpty()) {
            List<String> path = todo.pop();
            String current = path.get(path.size() - 1);
            if (!seen.add(current)) continue;
            AppPage child = pages.apply(current);
            if (child == null) continue;
            for (AppPageControl c : child.getControls()) {
                String next = "page".equals(c.getType()) ? c.getChildPageName() : null;
                if (next == null) continue;
                List<String> longer = new ArrayList<>(path);
                longer.add(next);
                if (next.equals(page.getPageName()))
                    throw new IllegalArgumentException(where + " runs page '" + name
                            + "', which leads back to this page: " + String.join(" → ", longer));
                todo.push(longer);
            }
        }
    }

    /**
     * What a control writes into other controls has to be somewhere a value can actually go: a
     * control on this page, one that holds or shows a value, and not the control doing the writing.
     * A page that saves an assignment aimed at nothing looks wired up and quietly does nothing when
     * the operator triggers it, which is the failure this refuses to store.
     */
    private void validateAssignments(AppPage page, AppPageControl control) {
        String where = "Control '" + describe(control) + "'";
        if (!control.getAssignments().isEmpty() && TRIGGERLESS_TYPES.contains(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — nothing triggers it, so it cannot set a value");
        checkAssignments(page, control.getAssignments(), control.getControlId(), where);
    }

    /**
     * The checks an assignment answers to wherever it was written: on a control, or on one of a
     * grid's clickable columns. Held apart from {@link #validateAssignments} because only the first
     * of those has a type that could be triggerless — a column link is triggered by definition, and
     * lives on a grid, which is exactly the type that check refuses.
     *
     * @param owner the control the assignment belongs to, so writing into itself can be refused;
     *              null where there is nothing to write into itself
     */
    static void checkAssignments(AppPage page, List<AppPageAssignment> assignments, String owner, String where) {
        for (AppPageAssignment assignment : assignments) {
            String target = assignment.getTargetControlId();
            if (target == null || target.isBlank())
                throw new IllegalArgumentException(where + " sets a value into no control");
            if (target.equals(owner))
                throw new IllegalArgumentException(where + " sets a value into itself");
            AppPageControl targetControl = page.getControls().stream()
                    .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
            if (targetControl == null)
                throw new IllegalArgumentException(where + " sets a value into a control that is not on this page: " + target);
            if (!ASSIGN_TYPES.contains(targetControl.getType()))
                throw new IllegalArgumentException(where + " sets a value into a " + targetControl.getType()
                        + " — a value goes into an input, a hidden field or a label");
        }
    }

    /**
     * A grid's clickable columns: only a grid has them, each names a column once, and each does
     * something when it is clicked.
     *
     * <p>That last check is the one worth having. A column marked clickable that sets nothing and
     * runs nothing draws itself as a link on the running page and answers a click with nothing at
     * all — the operator is told the cell is live by the only means the page has of telling them,
     * and it is not. The column name itself cannot be checked against anything: a grid whose columns
     * follow the response does not know what they are until a call answers.
     */
    /**
     * What carries clickable columns and rows: a grid, and a pie with grids — whose settings apply to
     * every grid it opens. Mirrors GRID_OWNER_TYPES in apppage.js.
     */
    private static final List<String> GRID_OWNER_TYPES = List.of("grid", "piegrid");

    static void validateColumnLinks(AppPage page, AppPageControl control, List<String> actionIds) {
        if (control.getColumnLinks().isEmpty()) return;
        String where = "Control '" + describe(control) + "'";
        if (!GRID_OWNER_TYPES.contains(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid or a pie with grids has clickable columns");
        List<String> named = new ArrayList<>();
        for (AppPageColumnLink link : control.getColumnLinks()) {
            requireName(link.getColumn(), where + " clickable column name");
            if (named.contains(link.getColumn()))
                throw new IllegalArgumentException(where + " makes the column '" + link.getColumn() + "' clickable twice");
            named.add(link.getColumn());

            String on = where + " column '" + link.getColumn() + "'";
            if (link.getAssignments().isEmpty() && link.getActionIds().isEmpty())
                throw new IllegalArgumentException(on + " is clickable but neither sets a value nor runs an action, "
                        + "so a click on it would do nothing");
            checkAssignments(page, link.getAssignments(), control.getControlId(), on);
            for (String id : link.getActionIds()) {
                if (!actionIds.contains(id))
                    throw new IllegalArgumentException(on + " runs an action that is not on this page: " + id);
            }
        }
    }

    /**
     * A grid's clickable rows: only a grid has them, and a click has to do something.
     *
     * <p>The same check the columns answer to, and it is worth having for the same reason: rows drawn
     * as clickable tell the operator, by the only means the page has of telling them, that pointing
     * at one will do something — and a row click that sets nothing and runs nothing answers that
     * with silence.
     */
    static void validateRowClick(AppPage page, AppPageControl control, List<String> actionIds) {
        AppPageRowClick click = control.getRowClick();
        if (click == null) return;
        String where = "Control '" + describe(control) + "'";
        if (!GRID_OWNER_TYPES.contains(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid or a pie with grids has clickable rows");
        if (click.getAssignments().isEmpty() && click.getActionIds().isEmpty())
            throw new IllegalArgumentException(where + " has clickable rows that neither set a value nor run an "
                    + "action, so a click on one would do nothing");
        checkAssignments(page, click.getAssignments(), control.getControlId(), where + " row click");
        for (String id : click.getActionIds()) {
            if (!actionIds.contains(id))
                throw new IllegalArgumentException(where + " row click runs an action that is not on this page: " + id);
        }
    }

    /**
     * What a tab set may <em>not</em> hold. Everything else may, which is the rule: a tab is a place
     * on the page with a name over it, and anything that can sit on the page can sit there. Grids and
     * charts are what tab sets were built for and remain the obvious ones, but the form half of a page
     * belongs in tabs just as often — a set whose first tab is the fields to fill in and whose others
     * are the readings of what came back, or a wizard whose steps are its tabs, none of which was
     * expressible while only rows and marks were allowed in.
     *
     * <p>One type stays out. A hidden field cannot be a tab — it is not on screen to be looked at, so
     * a tab holding one would be a name over nothing, and it has to stay where the page lays it out
     * to go on carrying its value.
     *
     * <p>A tab set may hold another tab set, which is how a page groups a set of tabs under one:
     * eight environments' worth of grids read far better as two tabs of four than as one strip of
     * eight, and the outer tab then answers for all of them at once — green only when every tab
     * inside it is (see {@link #validateTabCycles}). What is refused is not the nesting but the one
     * thing that cannot be drawn: a set that, followed down, arrives back at itself.
     *
     * <p>A panel answers to the same rule, since it is the same question one box in: anything that
     * can sit on the page can sit in a panel, and a hidden field stays out for the same reason —
     * it is not on screen, so putting it in a box would be drawing a gap in the box's layout while
     * moving the field away from where the page reads it.
     *
     * <p>Mirrors NOT_TAB_CHILD_TYPES in apppage.js.
     */
    private static final List<String> NOT_TAB_CHILD_TYPES = List.of("hidden");

    /**
     * A tab set holds controls that are on the same page and holds each of them once. Both checks are
     * about the same thing: a tab is only a place to put a control, so a name in the list that
     * answers to nothing on the page — or to a control another tab set has already claimed — leaves
     * the page with a tab that shows nothing, or with a control whose home has two answers. Neither
     * survives a save.
     */
    /**
     * A multi-button's menu: entries that each have something to read and something to run.
     *
     * <p>Both halves are refused when missing, because either one alone is a menu entry that cannot
     * do its job. An entry with no name is a blank line the operator is invited to click; an entry
     * with no actions is a line that looks alive and does nothing — which is precisely the kind of
     * wiring a multi-button exists to make visible, since one button hides six of them behind a
     * click.
     *
     * <p>The actions are named out of the page's library rather than written on the entry, so a name
     * the library does not hold is refused here in the same way a control triggering a missing
     * action is. And a menu on anything that is not a multi-button is refused rather than ignored:
     * nothing would ever draw it.
     */
    static void validateMenuOptions(AppPageControl control, List<String> actionIds) {
        String where = "Control '" + describe(control) + "'";
        if (!"multibutton".equals(control.getType())) {
            if (!control.getMenuOptions().isEmpty())
                throw new IllegalArgumentException(where + " is a " + control.getType()
                        + " and carries menu entries — only a multibutton has a menu");
            return;
        }
        if (control.getMenuOptions().isEmpty())
            throw new IllegalArgumentException(where + " is a multibutton with no entries — "
                    + "a menu with nothing on it is a button that does nothing when clicked");
        List<String> labels = new ArrayList<>();
        for (AppPageMenuOption option : control.getMenuOptions()) {
            if (option.getLabel() == null)
                throw new IllegalArgumentException(where + " has a menu entry with no name");
            // Not an error the page could not run with, but one the operator could not read: two
            // entries reading the same has them guessing which of the two calls they just made.
            if (labels.contains(option.getLabel()))
                throw new IllegalArgumentException(where + " has two menu entries called '"
                        + option.getLabel() + "' — an operator picking one could not tell them apart");
            labels.add(option.getLabel());
            if (option.getActionIds().isEmpty())
                throw new IllegalArgumentException(where + " menu entry '" + option.getLabel()
                        + "' runs nothing — attach a page action to it or take the entry off");
            for (String id : option.getActionIds()) {
                if (!actionIds.contains(id))
                    throw new IllegalArgumentException(where + " menu entry '" + option.getLabel()
                            + "' runs an action that is not on this page: " + id);
            }
        }
    }

    /**
     * The two controls that hold other controls instead of content of their own, and which are
     * therefore checked as one thing: a tab set, which shows one of its children at a time, and a
     * panel, which shows all of them at once inside a box with a name on it.
     *
     * <p>Everything below is about being a container rather than about being either of them, which
     * is why neither has its own copy: a child has to be on the page, a child belongs to exactly one
     * container whichever kind it is, and no container may arrive back at itself. Mirrors
     * CONTAINER_TYPES in apppage.js.
     */
    private static final List<String> CONTAINER_TYPES = List.of("tabs", "panel");

    /** What one container holds, whichever kind it is, or nothing at all for anything else. */
    static List<String> childIdsOf(AppPageControl control) {
        if ("tabs".equals(control.getType()))  return control.getTabControlIds();
        if ("panel".equals(control.getType())) return control.getPanelControlIds();
        return List.of();
    }

    /**
     * A container holds controls that are on the same page, holds each of them once, and is the only
     * container holding them. All three checks are about the same thing: a tab or a place in a panel
     * is only somewhere to put a control, so a name in the list that answers to nothing on the page
     * — or to a control another container has already claimed — leaves the page with a tab that shows
     * nothing, or with a control whose home has two answers. Neither survives a save.
     *
     * <p>The claim is shared across both kinds rather than counted per kind, which is what makes
     * "where is this drawn" answerable: a grid that is a tab of one set and also inside a panel would
     * be laid out twice, and an action filling it would fill one of the two copies.
     */
    static void validateTabs(AppPage page) {
        List<String> claimed = new ArrayList<>();
        for (AppPageControl control : page.getControls()) {
            String type = control.getType();
            if (!CONTAINER_TYPES.contains(type)) {
                // A list left behind on a control that is no longer a container would be drawn by
                // nothing while still claiming its children off the layout, so it is refused rather
                // than ignored.
                if (!control.getTabControlIds().isEmpty() || !control.getPanelControlIds().isEmpty())
                    throw new IllegalArgumentException("Control '" + describe(control) + "' is a " + type
                            + " and holds other controls — only a tab set or a panel can");
                continue;
            }
            boolean tabs = "tabs".equals(type);
            String where = (tabs ? "Tabs control '" : "Panel '") + describe(control) + "'";
            for (String id : childIdsOf(control)) {
                AppPageControl child = page.getControls().stream()
                        .filter(c -> id.equals(c.getControlId())).findFirst().orElse(null);
                if (child == null)
                    throw new IllegalArgumentException(where + " holds a control that is not on this page: " + id);
                if (NOT_TAB_CHILD_TYPES.contains(child.getType()))
                    throw new IllegalArgumentException(where + " holds a " + child.getType()
                            + " — a " + (tabs ? "tab set" : "panel")
                            + " holds any control except a hidden field");
                if (claimed.contains(id))
                    throw new IllegalArgumentException("Control '" + describe(child)
                            + "' is in more than one tab set or panel");
                claimed.add(id);
            }
            if (!tabs) continue;
            String preferred = control.getDefaultTabControlId();
            if (preferred != null && !control.getTabControlIds().contains(preferred))
                throw new IllegalArgumentException(where + " opens on a tab it does not hold: " + preferred);
        }
        validateTabCycles(page);
    }

    /**
     * No container arrives back at itself, however many containers it is followed through.
     *
     * <p>The one thing nesting cannot do. A set — or a panel — holding itself, directly or by way of
     * something it holds, has no depth at which it stops being drawn, so the browser would recurse
     * until it gave up; and the arrangement means nothing anyway, since there is no outermost tab or
     * box for the operator to be looking at. Every other nesting is fine and is the point of allowing
     * it: a panel of fields inside a tab, a tab set inside a panel, a panel inside a panel.
     *
     * <p>Checked by walking down from each container rather than up from each control, so the message
     * can name the one the reader is most likely to recognise: the one they just put something into.
     */
    static void validateTabCycles(AppPage page) {
        for (AppPageControl control : page.getControls()) {
            if (!CONTAINER_TYPES.contains(control.getType())) continue;
            Set<String> seen = new LinkedHashSet<>();
            if (reachesItself(page, control.getControlId(), control.getControlId(), seen))
                throw new IllegalArgumentException(("tabs".equals(control.getType()) ? "Tabs control '" : "Panel '")
                        + describe(control) + "' ends up holding itself: " + String.join(" › ", seen)
                        + " — a container inside itself has no depth at which it stops being drawn");
        }
    }

    /** Whether {@code target} is reachable by following the containers held under {@code from}. */
    private static boolean reachesItself(AppPage page, String from, String target, Set<String> path) {
        if (!path.add(from)) return false;   // already walked through here, so not a way back to target
        AppPageControl set = page.getControls().stream()
                .filter(c -> CONTAINER_TYPES.contains(c.getType()) && from.equals(c.getControlId()))
                .findFirst().orElse(null);
        if (set == null) return false;
        for (String id : childIdsOf(set)) {
            if (target.equals(id)) return true;
            if (reachesItself(page, id, target, path)) return true;
        }
        return false;
    }

    /** The controls drawn out of rows. Mirrors CHART_TYPES in apppage.js. */
    private static final List<String> CHART_TYPES = List.of("pie", "piegrid", "bar", "timeseries", "line");

    /**
     * The two charts that read rows along an axis and draw a line per series: a time series, whose
     * axis is a column of instants, and a line chart, whose axis counts. They share everything about
     * how a row becomes a point — the line column, the value columns, the filters, how values landing
     * together are combined — and differ only in what the x of a row is. Mirrors SERIES_TYPES in
     * apppage.js.
     */
    private static final List<String> SERIES_TYPES = List.of("timeseries", "line");

    /**
     * What the two charts that read rows along an axis carry, and what nothing else may.
     *
     * <p>A time series has to name the column its time is read from: that column is its x axis, and a
     * chart with no axis has nothing to draw against. A line chart names nothing of the sort, and
     * that is the difference between them — its axis counts the rows, so a blank
     * {@link AppPageControl#getXField() x column} is not an omission but the default, the line number
     * of each row. It may still name one, and then it has to be the line chart that does: a time
     * column on a line chart, or an x column on a time series, is a setting on a control whose chart
     * would never read it.
     *
     * <p>Filters belong to both and to nothing else. Each has to name a column and a test that
     * exists; the column itself is not checked against anything, for the same reason a fan-out's
     * filters are not — a chart drawn from a response has whatever columns that response returned,
     * which is not known until the page runs.
     */
    static void validateSeriesChart(AppPageControl control, String where) {
        String type = control.getType();
        boolean series = SERIES_TYPES.contains(type);
        boolean timed = "timeseries".equals(type);
        if (!series) {
            if (control.getTimeField() != null)
                throw new IllegalArgumentException(where + " is a " + type + " — only a time series chart has a time column");
            if (control.getTimeOffsetField() != null)
                throw new IllegalArgumentException(where + " is a " + type
                        + " — only a time series chart adds an offset column to its time column");
            if (control.getXField() != null)
                throw new IllegalArgumentException(where + " is a " + type + " — only a line chart has an x column");
            if (!control.getChartFilters().isEmpty())
                throw new IllegalArgumentException(where + " is a " + type
                        + " — only a time series or line chart filters its rows");
            return;
        }
        if (timed && control.getTimeField() == null)
            throw new IllegalArgumentException(where + " is a time series chart but names no time column");
        if (!timed && control.getTimeField() != null)
            throw new IllegalArgumentException(where + " is a line chart, which counts along its x axis — "
                    + "a time column belongs on a time series chart");
        if (timed && control.getXField() != null)
            throw new IllegalArgumentException(where + " is a time series chart, whose x axis is its time column — "
                    + "an x column belongs on a line chart");
        // An offset is something added to the time column, so there has to be one to add it to, and
        // it cannot be the same column — a date plus itself in seconds is not an instant.
        if (!timed && control.getTimeOffsetField() != null)
            throw new IllegalArgumentException(where + " is a line chart, which counts along its x axis — "
                    + "an offset column belongs on a time series chart");
        if (timed && control.getTimeOffsetField() != null
                && control.getTimeOffsetField().equalsIgnoreCase(control.getTimeField()))
            throw new IllegalArgumentException(where + " reads its offset from the same column as its time, '"
                    + control.getTimeField() + "' — the offset is a second column added on top of the first");
        checkRowFilters(control.getChartFilters(), where);
    }

    /**
     * A chart or a grid drawn from a grid: only those draw themselves from one, and the grid has to be
     * a grid on this page — anything else would be waiting on rows that never arrive. A grid may not
     * take its rows from itself, nor from a chain of grids that leads back to it: each would refill
     * the next forever.
     */
    static void validateChartGridSource(AppPage page, AppPageControl control, String where) {
        String gridId = control.getSourceGridControlId();
        if (gridId == null) return;
        boolean isGrid = "grid".equals(control.getType());
        if (!CHART_TYPES.contains(control.getType()) && !isGrid)
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a chart or a grid draws itself from a grid");
        AppPageControl grid = controlById(page, gridId);
        if (grid == null)
            throw new IllegalArgumentException(where + " draws itself from a grid that is not on this page: " + gridId);
        if (!"grid".equals(grid.getType()))
            throw new IllegalArgumentException(where + " draws itself from a " + grid.getType() + " — it needs a grid");
        if (!isGrid) return;
        List<String> seen = new ArrayList<>();
        for (AppPageControl at = grid; at != null; at = controlById(page, at.getSourceGridControlId())) {
            if (control.getControlId().equals(at.getControlId()))
                throw new IllegalArgumentException(where + " takes its rows from a grid that takes its rows from it");
            if (seen.contains(at.getControlId()) || at.getSourceGridControlId() == null) break;
            seen.add(at.getControlId());
        }
    }

    private static AppPageControl controlById(AppPage page, String controlId) {
        if (controlId == null) return null;
        return page.getControls().stream()
                .filter(c -> controlId.equals(c.getControlId())).findFirst().orElse(null);
    }

    /**
     * The settings only one kind of chart reads: the tab set a pie-with-grids puts its grids into —
     * which it has to name, and which has to be a tab set on this page — and a bar chart's orientation.
     * A tab set named on anything else is refused rather than saved as wiring nothing reads.
     */
    static void validateChartControls(AppPage page) {
        for (AppPageControl control : page.getControls()) {
            String where = "Control '" + describe(control) + "'";
            validateChartGridSource(page, control, where);
            validateSeriesChart(control, where);
            if (!"tabs".equals(control.getType()) && control.getDefaultTabControlId() != null)
                throw new IllegalArgumentException(where + " is a " + control.getType() + " — only a tab set has a default tab");
            String tabsId = control.getTabsControlId();
            if (!"piegrid".equals(control.getType())) {
                if (tabsId != null)
                    throw new IllegalArgumentException(where + " is a " + control.getType()
                            + " — only a pie chart with grids puts grids into a tab set");
                continue;
            }
            if (tabsId == null)
                throw new IllegalArgumentException(where + " is a pie chart with grids but names no tab set to put its grids in");
            AppPageControl tabs = page.getControls().stream()
                    .filter(c -> tabsId.equals(c.getControlId())).findFirst().orElse(null);
            if (tabs == null)
                throw new IllegalArgumentException(where + " puts its grids into a tab set that is not on this page: " + tabsId);
            if (!"tabs".equals(tabs.getType()))
                throw new IllegalArgumentException(where + " puts its grids into a " + tabs.getType() + " — it needs a tab set");
            // A chart may be a tab now, which is what makes this reachable: the set it fills would be
            // the set it is drawn in, so every run would add tabs beside the chart that produced them
            // and the operator would lose sight of that chart to look at them.
            if (tabs.getTabControlIds().contains(control.getControlId()))
                throw new IllegalArgumentException(where + " puts its grids into the tab set it is itself a tab of — "
                        + "pick another tab set, or take the chart out of this one");
        }
    }

    /**
     * Checks the page's own action library and hands back its ids for the controls to be checked
     * against. Ids are minted here when missing, so a page built in the designer never has to invent
     * them, and duplicates are refused: two actions answering to one id would make "which action does
     * this button run" unanswerable.
     */
    private List<String> validatePageActions(AppPage page, List<String> transformNames) {
        List<String> ids = new ArrayList<>();
        for (AppPageAction action : page.getActions()) {
            if (action.getActionId() == null || action.getActionId().isBlank()) {
                action.setActionId("a-" + UUID.randomUUID().toString().substring(0, 8));
            }
            if (ids.contains(action.getActionId()))
                throw new IllegalArgumentException("Duplicate action id: " + action.getActionId());
            ids.add(action.getActionId());
            validateAction(page, action, transformNames, "Page action '" + actionName(action, action.getActionId()) + "'");
        }
        // A library action may only wait for another library action: it runs wherever it happens to
        // be attached, and one particular control's own action is not there to be waited for from
        // the next control along.
        Map<String, AppPageAction> library = new LinkedHashMap<>();
        for (AppPageAction action : page.getActions()) library.put(action.getActionId(), action);
        validateDependencies(page.getActions(), library, "Page action", "");
        return ids;
    }

    /**
     * What an action is allowed to wait for: something that exists, is not itself, and is reachable
     * from where the action lives — see {@link AppPageAction#getDependsOnActionId()}.
     *
     * <p>And nothing that waits, however indirectly, on itself. A circle of actions waiting on each
     * other has no member that could go first, so no member of it would ever go at all; the running
     * page refuses to send them and says which, and storing a page whose trigger is known in advance
     * to be partly dead is not worth doing. The walk follows each action's chain of waits rather than
     * only its first step, so a circle of three is caught as surely as one of two.
     *
     * @param kind   what to call one of these actions in a message
     * @param on     where they live, for the same message; empty for the page's own library
     */
    private static void validateDependencies(List<AppPageAction> actions,
                                             Map<String, AppPageAction> reachable, String kind, String on) {
        for (AppPageAction action : actions) {
            String waited = action.getDependsOnActionId();
            if (waited == null || waited.isBlank()) continue;
            String where = kind + " '" + actionName(action, action.getActionId()) + "'" + on;
            if (waited.equals(action.getActionId()))
                throw new IllegalArgumentException(where + " waits for itself");
            if (!reachable.containsKey(waited))
                throw new IllegalArgumentException(where + " waits for an action it cannot see: " + waited);
        }
        for (AppPageAction action : actions) {
            Set<String> walked = new LinkedHashSet<>();
            AppPageAction step = action;
            while (step != null) {
                if (!walked.add(step.getActionId())) {
                    throw new IllegalArgumentException(kind + " '" + actionName(action, action.getActionId()) + "'" + on
                            + " is in a circle of actions waiting on each other, so none of them could go first: "
                            + String.join(" then ", walked));
                }
                String next = step.getDependsOnActionId();
                step = (next == null || next.isBlank()) ? null : reachable.get(next);
            }
        }
    }

    /**
     * The page's ad-hoc variables, handed back by name so a control cannot be given a field name one
     * of them already answers to.
     *
     * <p>Three things are refused, and each of them is a page that would half-work. A name a
     * template cannot spell is a variable nothing could ever read. Two variables of one name make
     * "which value is this" unanswerable. And a name {@link AppPageVariable#BUILT_IN} already holds
     * is a value the run computes for itself, so the stored one would be quietly ignored every time
     * the page ran.
     *
     * <p>A blank value is left alone: a variable declared for an operator to see and a page to fill
     * in later is a reasonable thing to save, and unlike a blank transform expression there is
     * nothing it could fail at.
     */
    /**
     * A value control may not answer to a name a variable already holds, its own or a built-in.
     *
     * <p>Both would otherwise make <code>${name}</code> mean two things at once, and which of them a
     * template got would depend on a lookup order nobody writing the page can see. Refused at the
     * control rather than at the variable because the variable may be the older of the two and is
     * read from more places: a page that has carried <code>${runTag}</code> into forty actions should
     * not quietly start meaning a text box somebody has just dropped on the canvas.
     */
    static void checkFieldNameFree(AppPageControl control, List<String> variableNames, String where) {
        String fieldName = control.getFieldName();
        if (fieldName == null || fieldName.isBlank()) return;
        if (variableNames.contains(fieldName))
            throw new IllegalArgumentException(where + " has the field name of a page variable: " + fieldName);
        if (AppPageVariable.BUILT_IN.contains(fieldName.toUpperCase()))
            throw new IllegalArgumentException(where + " has the field name of a variable every page already has: "
                    + fieldName);
    }

    static List<String> validateVariables(AppPage page) {
        List<String> names = new ArrayList<>();
        for (AppPageVariable variable : page.getVariables()) {
            String name = variable.name() == null ? "" : variable.name().trim();
            if (name.isBlank())
                throw new IllegalArgumentException("A page variable has no name");
            if (!AppPageVariable.isLegalName(name))
                throw new IllegalArgumentException("Page variable '" + name + "' is not a name a template can spell —"
                        + " letters, digits and underscores only, starting with a letter or an underscore");
            if (AppPageVariable.BUILT_IN.contains(name.toUpperCase()))
                throw new IllegalArgumentException("Page variable '" + name + "' redefines one this page already has:"
                        + " " + String.join(", ", AppPageVariable.BUILT_IN) + " are worked out when a trigger runs");
            if (names.contains(name))
                throw new IllegalArgumentException("Duplicate page variable: " + name);
            names.add(name);
        }
        return names;
    }

    /**
     * Checks the page's transform library and hands back its names for the actions to be checked
     * against. A blank name would be unnameable and a duplicate would make "which step does this
     * action run" unanswerable, so both are refused rather than silently picking one. Only a JSONata
     * step needs an expression: an XML-to-JSON step is fully described by its type.
     *
     * <p>A step may hold its expression or name one in the shared library, and a name is checked
     * here for the same reason a dataset name is — the page runs the library's text, so a name the
     * library has not got is a step that does nothing, and a step that does nothing in the middle of
     * a chain is a grid of the wrong thing rather than an error anyone would see.
     */
    private List<String> validateTransforms(AppPage page) {
        List<String> names = new ArrayList<>();
        for (AppPageTransform transform : page.getTransforms()) {
            requireName(transform.getName(), "Transform name");
            if (names.contains(transform.getName()))
                throw new IllegalArgumentException("Duplicate transform name: " + transform.getName());
            names.add(transform.getName());
            validateTransformExpression(transform, jsonataLibrary::has);
        }
        return names;
    }

    /**
     * Where a JSONata step's expression comes from: the page, or the library by name. Split out and
     * static so the rule can be read — and tested — on its own, with the library passed in as the
     * one question this asks of it.
     */
    static void validateTransformExpression(AppPageTransform transform, Predicate<String> libraryHolds) {
        if (transform.isXml2Json()) return;
        String where = "Transform '" + transform.getName() + "'";
        if (transform.getJsonataRef() != null) {
            if (!libraryHolds.test(transform.getJsonataRef()))
                throw new IllegalArgumentException(where + " names a shared JSONata that is not in the library: "
                        + transform.getJsonataRef());
            return;
        }
        if (transform.getExpression() == null || transform.getExpression().isBlank())
            throw new IllegalArgumentException(where + " has no JSONata expression");
    }

    /** The instance an action runs, the transforms it chains and the control it fills all have to be real. */
    private void validateAction(AppPage page, AppPageAction action, List<String> transformNames, String where) {
        // Asked of every action, before the kinds part company: both describe fanning out, and a
        // performance action is refused one — so filters or columns written on either kind are
        // wiring that could never run.
        validateRowFilters(action, where);
        validateBindFilters(page, action, where);
        validateRowColumns(action, where);
        validateExtraBindings(page, action, transformNames, where);
        validateEnrichColumns(page, action, name -> staticDatasets.get(name) != null, where);
        validatePivots(page, action, where);
        validateActionRowErrorExpression(action, where);
        if (action.isPerformance()) {
            validatePerformanceAction(page, action, where);
            return;
        }
        // Like a performance summary it names no instance and runs no transform, so it parts company
        // from an ordinary action before either is required of it.
        if (action.isDataset()) {
            validateDatasetAction(page, action, name -> staticDatasets.get(name) != null, where);
            return;
        }
        requireInstance(action.getAppUseCaseInstanceId(), where);
        for (String name : action.getTransformNames()) {
            if (!transformNames.contains(name))
                throw new IllegalArgumentException(where + " applies a transform that is not on this page: " + name);
        }
        // A comparison names an instance and reshapes each side with the page's transforms exactly as
        // an ordinary action does — both checks above are its as much as anyone's — and parts company
        // only over what it calls and where the answer goes.
        if (action.isCompare()) {
            validateCompareAction(page, action, where);
            return;
        }
        validateRowSource(page, action, where);
        validateActionTarget(page, action, where);
    }

    /**
     * What a comparison has to name for the page to be storable: two environments to hold against
     * each other, and a grid to put the differences in.
     *
     * <p>Both environments are required, and required to be different. One of them blank would run
     * the instance twice against its own configured environment and report, with total confidence,
     * that nothing had changed — which is the single most misleading thing this action could do, and
     * the one failure a page owner would never think to check for. The same goes for two that are
     * spelled the same. Templates are exempt from the second check and not from the first: what
     * {@code ${envA}} and {@code ${envB}} resolve to is a question about the operator's picks at run
     * time, and the running page says so there rather than the catalog guessing here.
     *
     * <p>The target has to be a grid, for the reason a performance summary's does: the answer is a
     * table of paths and values, and a text box or a chart has nowhere to put one. A blank target is
     * refused too — an ordinary action with no target is still worth running for the call it makes,
     * and a comparison that binds nothing has read two responses to no purpose whatever.
     *
     * <p>Fanning out is refused. A fan-out has one answer per row and a comparison has one table;
     * running it once per row would overwrite that table with each row's version of it.
     */
    static void validateCompareAction(AppPage page, AppPageAction action, String where) {
        if (action.isRowFanOut())
            throw new IllegalArgumentException(where + " compares two environments, which produces one table of "
                    + "differences rather than an answer per row — so running it once per row of a grid would leave "
                    + "only the last row's table. Clear its \"for each row of\".");

        String a = action.getCompareEnvironmentA();
        String b = action.getCompareEnvironmentB();
        if (a == null || a.isBlank() || b == null || b.isBlank())
            throw new IllegalArgumentException(where + " compares two environments but only names "
                    + ((a == null || a.isBlank()) && (b == null || b.isBlank()) ? "neither" : "one")
                    + " — name both, or it would run the same environment twice and report no differences.");
        if (a.trim().equals(b.trim()) && !a.contains("${"))
            throw new IllegalArgumentException(where + " compares '" + a.trim() + "' against itself, which can only "
                    + "ever report no differences — name two different environments.");

        if (action.getCompareTolerancePercent() > 100)
            throw new IllegalArgumentException(where + " sets a threshold of " + action.getCompareTolerancePercent()
                    + "%, which accepts almost any difference as a match — a threshold is a percentage, so 0.1 means "
                    + "a tenth of a percent.");

        String target = action.getTargetControlId();
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " compares two environments but has nowhere to put the "
                    + "differences — aim it at a grid, or at a new grid.");
        if (AppPageAction.NEW_GRID.equals(target)) return;

        AppPageControl targetControl = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (targetControl == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        if (!"grid".equals(targetControl.getType()))
            throw new IllegalArgumentException(where + " compares two environments, which is a table of the paths "
                    + "that differ and what each side said — so it must target a grid, not a "
                    + targetControl.getType());
    }

    /**
     * An action's further bindings, each held to what the action's own target is held to: transforms
     * the page has, and a target that is on the page and can take what is bound into it.
     *
     * <p>A binding with no target is refused rather than skipped. The action's own target may be
     * blank — the call is still worth making for its effect — but a binding is nothing except a
     * target, so a blank one is a row in the designer that would do nothing.
     */
    static void validateExtraBindings(AppPage page, AppPageAction action, List<String> transformNames, String where) {
        if (action.getExtraBindings().isEmpty()) return;
        if (action.isPerformance())
            throw new IllegalArgumentException(where + " summarises performance, which fills one grid — "
                    + "remove its other targets");
        if (action.isCompare())
            throw new IllegalArgumentException(where + " compares two environments, which fills one grid with "
                    + "the differences — remove its other targets");
        if (action.isDataset())
            throw new IllegalArgumentException(where + " reads a static dataset, which fills one control with "
                    + "its rows — remove its other targets, or add a second dataset action");
        if (action.isRowFanOut())
            throw new IllegalArgumentException(where + " runs once per row, which has an answer per row rather "
                    + "than one response to bind several ways — remove its other targets, or clear its \"for each row of\"");
        int n = 1;
        for (AppPageBinding binding : action.getExtraBindings()) {
            n++;
            String on = where + " target " + n;
            if (binding == null || binding.getTargetControlId() == null || binding.getTargetControlId().isBlank())
                throw new IllegalArgumentException(on + " has no control chosen, so it would bind nothing");
            for (String name : binding.getTransformNames()) {
                if (!transformNames.contains(name))
                    throw new IllegalArgumentException(on + " applies a transform that is not on this page: " + name);
            }
            AppPageAction shadow = new AppPageAction();
            shadow.setTargetControlId(binding.getTargetControlId());
            validateActionTarget(page, shadow, on);
        }
    }

    /**
     * Where an action is allowed to put what it bound. Which control types those are is not one list
     * but three, and which applies is decided by how many answers the action is going to have: run
     * once it may fill any of the long-standing targets; fanned out over a grid's rows it fills one
     * grid, a row per call, or a tab set, a grid per call.
     *
     * <p>A tab set is deliberately in neither of the first two: there is no single answer a tab set
     * as such holds, so an ordinary action aimed at one is refused and told to aim at one of the
     * grids inside it instead.
     */
    /**
     * A performance action: it calls nothing outward, so it answers to none of the checks an ordinary
     * action does — no instance to be real, no transforms to be on the page, no response for a path
     * to be read out of. What is left is where its rows go.
     *
     * <p>Which has to be a grid, and there is nothing to soften about that: the summary is a table
     * with six columns and a row per app, environment and use case, and a text box, a link or a chart
     * has nowhere to put one. A blank target is refused for the same reason — an ordinary action with
     * no target is still worth running for the call it makes, and this one makes none, so a
     * targetless performance action is an action that would do nothing whatever.
     *
     * <p>Fanning out is refused as well: a fan-out runs its action once per row of a grid, and this
     * action reads the run history rather than the row, so every one of those calls would summarise
     * the identical thing.
     */
    static void validatePerformanceAction(AppPage page, AppPageAction action, String where) {
        if (action.isRowFanOut())
            throw new IllegalArgumentException(where + " summarises performance, which reads the run history "
                    + "rather than a row — so running it once per row of a grid would produce the same summary "
                    + "every time. Clear its \"for each row of\".");

        String target = action.getTargetControlId();
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " summarises performance but has nowhere to put it — "
                    + "aim it at a grid, or at a new grid.");
        if (AppPageAction.NEW_GRID.equals(target)) return;

        AppPageControl targetControl = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (targetControl == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        if (!"grid".equals(targetControl.getType()))
            throw new IllegalArgumentException(where + " summarises performance, which is a table of app, "
                    + "environment, use case, request count and timings — so it must target a grid, not a "
                    + targetControl.getType());
    }

    /**
     * A dataset action: like a performance summary it calls nothing outward, so it answers to none of
     * the checks about a call — no instance, no environment, no transform chain, no path into a
     * response. What it does have to name is the dataset it reads, which has to be one the library
     * actually holds, and a control its rows can go in.
     *
     * <p>Its rows are ordinary rows, so — unlike the other two special kinds — it is not confined to
     * a grid: a select takes them as options and a chart takes them as marks. What it may not target
     * is a text box, a text area or a link, each of which holds one value read out of a response by
     * path, and there is no response here to read one out of. A blank target is refused for the
     * reason a performance summary's is: an ordinary action with no target is still worth running for
     * the call it makes, and this one makes none.
     *
     * <p>Fanning out is refused as well. The filters are resolved against the page, not against a
     * row, so every one of those queries would ask the identical question — and a fan-out collects
     * one answer per row, which would then be the same list over and over.
     *
     * @param datasetKnown whether the static dataset library holds a dataset of that name
     */
    static void validateDatasetAction(AppPage page, AppPageAction action, Predicate<String> datasetKnown,
                                      String where) {
        if (action.isRowFanOut())
            throw new IllegalArgumentException(where + " reads a static dataset, which asks the same question "
                    + "however many rows a grid has — so running it once per row would fetch the identical list "
                    + "every time. Clear its \"for each row of\".");

        String dataset = action.getDatasetName();
        if (dataset == null || dataset.isBlank())
            throw new IllegalArgumentException(where + " names no static dataset to read");
        if (!datasetKnown.test(dataset))
            throw new IllegalArgumentException(where + " names an unknown static dataset: " + dataset);
        validateDatasetFilters(action, where);

        String target = action.getTargetControlId();
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " reads a static dataset but has nowhere to put it — "
                    + "aim it at a grid, a new grid, a dropdown or a chart.");
        if (AppPageAction.NEW_GRID.equals(target)) return;

        AppPageControl targetControl = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (targetControl == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        String type = targetControl.getType();
        if (!"grid".equals(type) && !isSelect(type) && !CHART_TYPES.contains(type))
            throw new IllegalArgumentException(where + " reads a static dataset, which is a list of rows — so it "
                    + "must target a grid, a select, a multi-select or a chart, not a " + type);
    }

    /**
     * A dataset action's own conditions: each has to name a column and a test the dataset library
     * knows. The value is not checked and cannot be — it is routinely a ${field} template whose
     * answer is the operator's pick at run time — but a condition with no column is a row in the
     * designer that would narrow nothing, and an operator the library does not know would be refused
     * at the far end, after the page had been saved and clicked.
     */
    static void validateDatasetFilters(AppPageAction action, String where) {
        int n = 0;
        for (AppPageDatasetFilter filter : action.getDatasetFilters()) {
            n++;
            String on = where + " filter " + n;
            if (filter == null || filter.attribute() == null || filter.attribute().isBlank())
                throw new IllegalArgumentException(on + " names no column to test");
            if (!AppPageDatasetFilter.OPERATORS.contains(filter.opOrDefault()))
                throw new IllegalArgumentException(on + " tests '" + filter.attribute() + "' with '"
                        + filter.opOrDefault() + "', which is not one of: "
                        + String.join(", ", AppPageDatasetFilter.OPERATORS));
        }
    }

    static void validateActionTarget(AppPage page, AppPageAction action, String where) {
        String target = action.getTargetControlId();
        if (target == null || target.isBlank() || AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl targetControl = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (targetControl == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");

        if (action.isTabsPerRow()) {
            if (!"tabs".equals(targetControl.getType()))
                throw new IllegalArgumentException(where + " gives each row its own tab, so it must target a tab set, not a "
                        + targetControl.getType());
            return;
        }
        if ("tabs".equals(targetControl.getType()))
            throw new IllegalArgumentException(where + " targets a tab set, which only an action giving each row "
                    + "of a grid its own tab may do — aim it at one of the grids inside instead");
        if (!TARGET_TYPES.contains(targetControl.getType()))
            throw new IllegalArgumentException(where
                    + " must target a grid, select, text, text area, link, pie chart, bar chart, time series"
                    + " or line chart, not a "
                    + targetControl.getType());
        // Every row's answer becoming one of a collected list only means anything where a list can
        // go: a grid, a row per call, or a dropdown, an option per call — the list of things the
        // next click has to be made against is as often a picker as it is a table.
        if (action.isRowFanOut() && !"grid".equals(targetControl.getType()) && !isSelect(targetControl.getType()))
            throw new IllegalArgumentException(where + " runs once per row and collects the answers into one grid "
                    + "or dropdown, so it must target a grid, a select or a multi-select, not a "
                    + targetControl.getType());
    }

    /**
     * A fan-out's row source: a grid on this page, and not one this very action fills, which would
     * be an action feeding itself its own next set of rows.
     */
    static void validateRowSource(AppPage page, AppPageAction action, String where) {
        if (!action.isRowFanOut()) return;
        String sourceId = action.getRowSourceControlId();
        AppPageControl source = page.getControls().stream()
                .filter(c -> sourceId.equals(c.getControlId())).findFirst().orElse(null);
        if (source == null)
            throw new IllegalArgumentException(where + " runs once per row of a grid that is not on this page: " + sourceId);
        if (!"grid".equals(source.getType()) && !isSelect(source.getType()))
            throw new IllegalArgumentException(where + " runs once per row of a " + source.getType()
                    + " — only a grid, a select or a multi-select has rows to run over");
        if (sourceId.equals(action.getTargetControlId()))
            throw new IllegalArgumentException(where + " reads its rows from the same grid it fills, "
                    + "so each run would be over whatever the last one left behind");
    }

    /**
     * A fan-out's own row filters — see {@link AppPageRowFilter}. Every one of them has to name a
     * column and a test that exists; a filter written on an action that does not fan out is refused
     * outright rather than saved as wiring that could never run, which is the same rule an
     * assignment on a grid answers to.
     *
     * <p>The column is not checked against the source grid's columns, and deliberately: a grid whose
     * rows come from an endpoint has whatever columns that endpoint returned, which is not known
     * until the page runs. A filter naming a column that never turns up says so on the page, where
     * the rows are, rather than here.
     */
    static void validateRowFilters(AppPageAction action, String where) {
        if (action.getRowFilters().isEmpty()) return;
        if (!action.isRowFanOut())
            throw new IllegalArgumentException(where + " filters the rows it runs over but does not run over rows —"
                    + " point it at a grid under \"for each row of\", or take the filters off");
        checkRowFilters(action.getRowFilters(), where);
    }

    /**
     * The filters an action — or one of its further targets — narrows what it binds with, before the
     * rows reach the control. Same tests as a fan-out's, checked the same way: a column and an
     * operator that exists.
     *
     * <p>What differs is where they are allowed. These narrow a list of rows on its way into
     * something that holds rows, so the target has to be one of those: a grid, a new grid, or a
     * select. A text box, a link or a chart is refused them — a box takes one value and has no rows
     * to keep, and a chart already filters its own rows under its own settings, which the operator
     * can change while the page runs. Refusing rather than ignoring is the same rule the row filters
     * and the enriched columns answer to: wiring the designer can see and the page would never read
     * is worse than a message at the save.
     */
    static void validateBindFilters(AppPage page, AppPageAction action, String where) {
        if (!action.getBindFilters().isEmpty()) {
            requireRowTarget(page, action.getTargetControlId(), action.isTabsPerRow(), where);
            checkRowFilters(action.getBindFilters(), where);
        }
        int n = 1;
        for (AppPageBinding binding : action.getExtraBindings()) {
            n++;
            if (binding == null || binding.getBindFilters().isEmpty()) continue;
            String on = where + " target " + n;
            requireRowTarget(page, binding.getTargetControlId(), false, on);
            checkRowFilters(binding.getBindFilters(), on);
        }
    }

    /**
     * Where filtered rows may land: a grid, a new grid, a select — or, for a fan-out giving each row
     * its own tab, the tab set whose grids they fill.
     */
    private static void requireRowTarget(AppPage page, String target, boolean tabsPerRow, String where) {
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " filters what it binds but has nowhere to bind it — "
                    + "aim it at a grid or a dropdown, or take the filters off");
        if (AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl control = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (control == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        String type = control.getType();
        boolean holdsRows = tabsPerRow ? "tabs".equals(type) : "grid".equals(type) || isSelect(type);
        if (!holdsRows)
            throw new IllegalArgumentException(where + " filters the rows it binds, which only a grid or a "
                    + "dropdown takes a list of — its target is a " + type);
    }

    /** One list of row filters: each naming a column, and a test this page knows how to make. */
    private static void checkRowFilters(List<AppPageRowFilter> filters, String where) {
        for (AppPageRowFilter filter : filters) {
            if (filter == null || filter.column() == null || filter.column().isBlank())
                throw new IllegalArgumentException(where + " has a filter that names no column");
            if (!AppPageRowFilter.OPERATORS.contains(filter.operatorOrDefault()))
                throw new IllegalArgumentException(where + " has a filter with an unknown test: " + filter.operator());
        }
    }

    /**
     * The columns of the grid a collected fan-out fills — see {@link AppPageResultColumn}.
     *
     * <p>Only a fan-out collecting into one grid has a grid these describe. Under a tab per row every
     * call fills a grid of its own with its whole answer, and an action that runs once has one
     * answer and the target grid's own columns to show it under, so in both cases these columns would
     * be saved and never consulted.
     */
    static void validateRowColumns(AppPageAction action, String where) {
        if (action.getRowColumns().isEmpty()) return;
        if (!action.isRowFanOut())
            throw new IllegalArgumentException(where + " defines the columns its calls are collected under but makes"
                    + " one call — point it at a grid under \"for each row of\", or take the columns off");
        if (action.isTabsPerRow())
            throw new IllegalArgumentException(where + " gives each row its own tab, so each call fills a grid with its"
                    + " whole answer and there is no collected grid for these columns to lay out");
        List<String> names = new ArrayList<>();
        for (AppPageResultColumn column : action.getRowColumns()) {
            if (column.name() == null || column.name().isBlank())
                throw new IllegalArgumentException(where + " has a result column with no name");
            if (names.contains(column.name()))
                throw new IllegalArgumentException(where + " has two result columns called " + column.name());
            names.add(column.name());
            if (!AppPageResultColumn.KINDS.contains(column.kindOrDefault()))
                throw new IllegalArgumentException(where + " column '" + column.name()
                        + "' reads something this page has no idea how to read: " + column.kind());
            if (AppPageResultColumn.NEEDS_EXPRESSION.contains(column.kindOrDefault())
                    && (column.expression() == null || column.expression().isBlank()))
                throw new IllegalArgumentException(where + " column '" + column.name() + "' says where to read from"
                        + " but not what to read");
        }
    }

    /**
     * The enriched columns on an action and on each of its further targets — see
     * {@link AppPageEnrichColumn}. Columns are added to rows, so whatever carries them has to be
     * filling a grid: a grid, a new grid, or the tab set a tab-per-row fan-out fills with grids.
     *
     * <p>A performance summary is refused them: it makes no call, so there is no record or header to
     * read, and its table's columns are fixed.
     *
     * @param datasetKnown whether the static dataset library holds a dataset of that name
     */
    static void validateEnrichColumns(AppPage page, AppPageAction action, Predicate<String> datasetKnown, String where) {
        if (!action.getEnrichColumns().isEmpty()) {
            if (action.isPerformance())
                throw new IllegalArgumentException(where + " summarises performance, which makes no call to enrich "
                        + "its rows from — take its enriched columns off");
            // Two calls, so "the status code" has two answers and a column holding one of them would
            // be labelled as if it held the call's. The two environments are already columns of the
            // report itself, which is what an enriched column would most often have been added for.
            if (action.isCompare())
                throw new IllegalArgumentException(where + " compares two environments, so there are two calls "
                        + "behind every row and no one record to enrich it from — take its enriched columns off");
            // A lookup is about the rows, so a dataset action takes one as readily as any other action
            // does — looking each row's desk up in a second dataset is most of what these are for. The
            // two kinds that read the call behind the rows are the ones with nothing to read: this
            // action makes none, and a column quietly empty down every row is worse than a refusal
            // here, which can say why.
            if (action.isDataset()) {
                for (AppPageEnrichColumn column : action.getEnrichColumns()) {
                    String kind = column == null ? null : column.kindOrDefault();
                    if (AppPageEnrichColumn.META.equals(kind) || AppPageEnrichColumn.HEADER.equals(kind))
                        throw new IllegalArgumentException(where + " reads a static dataset, which makes no call — so "
                                + "its column '" + column.name() + "' has no "
                                + (AppPageEnrichColumn.META.equals(kind) ? "call record" : "response header")
                                + " to read. Take it off, or look the value up in a dataset or another grid.");
                }
            }
            // A fan-out collecting into a dropdown enriches the same rows a collected grid's would
            // be — and then reads the key and label off them, which is often exactly the column the
            // enrichment brought in — so a dropdown takes them there as readily as a grid does.
            requireGridTarget(page, action.getTargetControlId(), action.isTabsPerRow(),
                    action.isRowFanOut() && !action.isTabsPerRow(), where);
            checkEnrichColumns(page, action.getEnrichColumns(), datasetKnown, where);
        }
        int n = 1;
        for (AppPageBinding binding : action.getExtraBindings()) {
            n++;
            if (binding == null || binding.getEnrichColumns().isEmpty()) continue;
            String on = where + " target " + n;
            requireGridTarget(page, binding.getTargetControlId(), false, false, on);
            checkEnrichColumns(page, binding.getEnrichColumns(), datasetKnown, on);
        }
    }

    /**
     * @param collectedList whether a dropdown counts as somewhere the enriched rows may land, which
     *                      a fan-out collecting its answers into one makes true
     */
    private static void requireGridTarget(AppPage page, String target, boolean tabsPerRow,
                                          boolean collectedList, String where) {
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " has enriched columns but no grid to add them to — "
                    + "aim it at a grid, or take the columns off");
        if (AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl control = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (control == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        String wanted = tabsPerRow ? "tabs" : "grid";
        if (!wanted.equals(control.getType()) && !(collectedList && isSelect(control.getType())))
            throw new IllegalArgumentException(where + " has enriched columns, which only a grid has rows for — "
                    + "its target is a " + control.getType());
    }

    private static void checkEnrichColumns(AppPage page, List<AppPageEnrichColumn> columns, Predicate<String> datasetKnown, String where) {
        List<String> names = new ArrayList<>();
        for (AppPageEnrichColumn column : columns) {
            if (column == null)
                throw new IllegalArgumentException(where + " has an enriched column with no name");
            String kind = column.kindOrDefault();
            // A whole-row lookup is named after the row it finds, so it is the one kind with no name
            // of its own to require or to hold against another column's.
            boolean wholeRow = column.bringsWholeRow();
            if (!wholeRow) {
                if (column.name() == null || column.name().isBlank())
                    throw new IllegalArgumentException(where + " has an enriched column with no name");
                if (names.contains(column.name()))
                    throw new IllegalArgumentException(where + " has two enriched columns called " + column.name());
                names.add(column.name());
            }
            String label = where + " enriched column '" + (wholeRow ? describeWholeRow(column) : column.name()) + "'";
            if (!AppPageEnrichColumn.KINDS.contains(kind))
                throw new IllegalArgumentException(label + " reads something this page has no idea how to read: "
                        + column.kind());
            if (AppPageEnrichColumn.REGEX.equals(kind)) {
                checkRegexColumn(column, label);
                continue;
            }
            boolean intoGrid = AppPageEnrichColumn.GRID_KINDS.contains(kind);
            if (!AppPageEnrichColumn.VLOOKUP.equals(kind) && !intoGrid) {
                if (isBlank(column.expression()))
                    throw new IllegalArgumentException(label + " does not say which "
                            + (AppPageEnrichColumn.HEADER.equals(kind) ? "header" : "call record field") + " to read");
                continue;
            }
            String source = intoGrid ? "grid" : "dataset";
            if (intoGrid) {
                if (isBlank(column.gridControlId()))
                    throw new IllegalArgumentException(label + " names no grid to look up into");
                AppPageControl grid = page == null ? null : page.getControls().stream()
                        .filter(c -> column.gridControlId().equals(c.getControlId())).findFirst().orElse(null);
                if (grid == null || !"grid".equals(grid.getType()))
                    throw new IllegalArgumentException(label + " looks up into a grid that is not on this page: "
                            + column.gridControlId());
            } else {
                if (isBlank(column.datasetName()))
                    throw new IllegalArgumentException(label + " names no static dataset to look up into");
                if (!datasetKnown.test(column.datasetName()))
                    throw new IllegalArgumentException(label + " names an unknown static dataset: " + column.datasetName());
            }
            if (isBlank(column.lookupColumn()))
                throw new IllegalArgumentException(label + " does not say which grid column to look up");
            if (isBlank(column.keyColumn()))
                throw new IllegalArgumentException(label + " does not say which " + source + " column is the row key");
            // The whole-row lookup brings back every column the matched row has, which is the point
            // of it, so it is the one lookup with nothing to name here.
            if (!wholeRow && isBlank(column.returnColumn()))
                throw new IllegalArgumentException(label + " does not say which " + source + " column to bring back");
        }
    }

    /** What to call a whole-row lookup in a message, having no name of its own: its prefix, or its key. */
    private static String describeWholeRow(AppPageEnrichColumn column) {
        if (!isBlank(column.prefix())) return column.prefix().trim() + "*";
        return "whole row on " + (isBlank(column.lookupColumn()) ? "?" : column.lookupColumn().trim());
    }

    /**
     * A regex column: the column it reads, a pattern that compiles, and a replacement whose group
     * references the pattern actually has. A pattern that will not compile is worth refusing at the
     * save rather than leaving a column that is empty on every row and says nothing about why; so is
     * a {@code $3} in a pattern with two groups, which is the same silent emptiness by a different
     * route.
     */
    private static void checkRegexColumn(AppPageEnrichColumn column, String label) {
        if (isBlank(column.lookupColumn()))
            throw new IllegalArgumentException(label + " does not say which grid column to run its pattern over");
        if (isBlank(column.expression()))
            throw new IllegalArgumentException(label + " has no pattern to run");
        Pattern pattern;
        try {
            pattern = Pattern.compile(regexBody(column.expression()));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(label + " is not a pattern this page can run: " + e.getMessage());
        }
        int groups = pattern.matcher("").groupCount();
        Matcher reference = REGEX_GROUP_REFERENCE.matcher(column.replacement() == null ? "" : column.replacement());
        while (reference.find()) {
            int group = Integer.parseInt(reference.group(1));
            if (group > groups)
                throw new IllegalArgumentException(label + " puts $" + group + " in what the cell gets, but its "
                        + "pattern has " + (groups == 0 ? "no capturing groups" : "only " + groups));
        }
    }

    /** {@code $1}, {@code $2} … in a regex column's replacement, and never {@code $$1}, an escaped one. */
    private static final Pattern REGEX_GROUP_REFERENCE = Pattern.compile("(?<!\\$)\\$(\\d+)");

    /** A regex column's pattern written {@code /body/flags} carries flags; the flags are not the pattern. */
    private static final Pattern REGEX_WITH_FLAGS = Pattern.compile("^/(.*)/([gimsuy]*)$", Pattern.DOTALL);

    /**
     * The pattern out of what a regex column carries, which may be written {@code /pattern/flags}. Only
     * the pattern is checked here: the flags are the browser's to read, and an unknown one leaves the
     * text as the pattern rather than being refused — the same reading apppage.js does.
     */
    static String regexBody(String expression) {
        String text = expression == null ? "" : expression.trim();
        Matcher slashes = REGEX_WITH_FLAGS.matcher(text);
        return slashes.matches() && !slashes.group(1).isEmpty() ? slashes.group(1) : text;
    }

    /**
     * The group-by on an action and on each of its further targets — see {@link AppPagePivot}. It
     * reshapes rows into a table, so whatever carries one has to be filling a grid: a grid, a new
     * grid, or a fan-out collecting its answers into one. A tab per row is refused it — every call
     * there has a grid of its own, and grouping each one separately is not the table anyone asked for.
     */
    static void validatePivots(AppPage page, AppPageAction action, String where) {
        if (action.hasPivot()) {
            if (action.isTabsPerRow())
                throw new IllegalArgumentException(where + " gives each row its own tab, so there is no one grid for "
                        + "its group-by to fill — collect the answers into one grid, or take the group-by off");
            requirePivotGrid(page, action.getTargetControlId(), where);
            checkPivot(action.getPivot(), where);
        }
        int n = 1;
        for (AppPageBinding binding : action.getExtraBindings()) {
            n++;
            if (binding == null || binding.getPivot() == null || !binding.getPivot().groupsAnything()) continue;
            String on = where + " target " + n;
            requirePivotGrid(page, binding.getTargetControlId(), on);
            checkPivot(binding.getPivot(), on);
        }
    }

    private static void requirePivotGrid(AppPage page, String target, String where) {
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " groups its rows but has no grid to show them in — "
                    + "aim it at a grid, or take the group-by off");
        if (AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl control = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (control == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " groups its rows into a table, which only a grid can show — "
                    + "its target is a " + control.getType());
    }

    private static void checkPivot(AppPagePivot pivot, String where) {
        for (String field : pivot.getRows()) {
            if (pivot.getCols().contains(field))
                throw new IllegalArgumentException(where + " groups by " + field + " both down and across — pick one");
        }
        for (AppPagePivot.Value value : pivot.getValues()) {
            if (value == null || value.agg() == null || !AppPagePivot.AGGS.contains(value.agg()))
                throw new IllegalArgumentException(where + " has a group-by value that works out something unknown: "
                        + (value == null ? null : value.agg()));
            if (!value.countsRows() && isBlank(value.field()))
                throw new IllegalArgumentException(where + " has a group-by value (" + value.agg()
                        + ") that names no column to work it out over");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String actionName(AppPageAction action, String fallback) {
        return action.getActionLabel() != null && !action.getActionLabel().isBlank() ? action.getActionLabel() : fallback;
    }

    private void requireInstance(String instanceId, String where) {
        if (instanceId == null || instanceId.isBlank())
            throw new IllegalArgumentException(where + " names no use case instance");
        if (getInstance(instanceId) == null)
            throw new IllegalArgumentException(where + " names an unknown instance: " + instanceId);
    }

    private static String describe(AppPageControl control) {
        if (control.getLabel() != null && !control.getLabel().isBlank())         return control.getLabel();
        if (control.getFieldName() != null && !control.getFieldName().isBlank()) return control.getFieldName();
        return control.getType();
    }

    // -------------------------------------------------------------------------
    // Persistence — one JSON array per collection under ${DATADIR}/appcatalog/
    // -------------------------------------------------------------------------

    private static void requireName(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " is required");
    }

    private Path resolvePath(String fileName) {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(DIR).resolve(fileName);
    }

    private <T> List<T> read(String fileName, TypeReference<List<T>> type) {
        Path path = resolvePath(fileName);
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, type);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void write(String fileName, List<?> contents) throws Exception {
        Path target = resolvePath(fileName);
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), contents);
    }
}
