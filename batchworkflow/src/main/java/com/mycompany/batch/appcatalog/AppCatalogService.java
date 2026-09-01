package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final List<AppDefinition>           apps         = new CopyOnWriteArrayList<>();
    private final List<AppEnvironment>          environments = new CopyOnWriteArrayList<>();
    private final List<AppUseCase>              useCases     = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstance>      instances    = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstanceGroup> groups       = new CopyOnWriteArrayList<>();
    private final List<AppPage>                 pages        = new CopyOnWriteArrayList<>();

    public AppCatalogService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
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

    /** Control types that hold a value the operator supplies, and so need a field name. */
    private static final List<String> VALUE_TYPES =
            List.of("text", "textarea", "number", "date", "hidden", "select", "checkbox");

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
     * fields of each row. Mirrors TARGET_TYPES in apppage.html.
     */
    private static final List<String> TARGET_TYPES = List.of("grid", "select", "text", "textarea", "link", "pie");

    /**
     * Control types another control can write a value into. Wider than {@link #TARGET_TYPES}: a
     * response needs somewhere that can hold rows or be read back, while an assignment is only a
     * value being put somewhere — every value control takes one, and a label takes one to show.
     */
    private static final List<String> ASSIGN_TYPES =
            List.of("text", "textarea", "number", "date", "hidden", "select", "checkbox", "label");

    /**
     * Control types nothing sets off, and which therefore have nothing to set or run. A hidden field
     * belongs here with the grids, the labels and the tab sets: it carries a value the rest of the
     * page reads, but it is never drawn, so it is never clicked or changed — and a value arriving in
     * it deliberately does not fire its own trigger either. An assignment written on one would sit in
     * the saved page looking wired up and never once run.
     */
    private static final List<String> TRIGGERLESS_TYPES = List.of("grid", "label", "tabs", "hidden", "pie");

    private void validateControls(AppPage page) {
        List<String> controlIds = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<String> transformNames = validateTransforms(page);
        List<String> actionIds = validatePageActions(page, transformNames);

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
            }
            validateSlices(control, where);
            validateLinkUrl(control, where);
            if ("select".equals(control.getType()) && control.getOptionSource() != null) {
                AppPageOptionSource source = control.getOptionSource();
                if ("USECASE".equals(source.getMode())) {
                    requireInstance(source.getAppUseCaseInstanceId(), where + " option source");
                } else if ("ENVIRONMENTS".equals(source.getMode())) {
                    if (source.getAppName() == null || source.getAppName().isBlank())
                        throw new IllegalArgumentException(where + " option source names no app");
                    if (getApp(source.getAppName()) == null)
                        throw new IllegalArgumentException(where + " option source names an unknown app: " + source.getAppName());
                }
            }
        }

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

        validateTabs(page);
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
            // one thing this check exists to prevent. Mirrors #sliceNumber in apppage.html.
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
     * A link's own address, when the designer gave it one. Only {@code http}, {@code https} and a
     * path rooted on this server are allowed through, and it is the same rule the running page
     * applies to an address an action binds — the value ends up in an href either way, and a
     * {@code javascript:} one there would be whatever was typed running as the page.
     *
     * <p>Refused at the save rather than left to the browser, which drops such an address silently:
     * the page would store a link that looked wired up and went nowhere, with nothing anywhere to
     * say why. Here the message can name the link and the address it was given.
     */
    static void validateLinkUrl(AppPageControl control, String where) {
        if (!"link".equals(control.getType())) return;
        String url = control.getDefaultValue() == null ? "" : control.getDefaultValue().trim();
        if (url.isEmpty()) return;
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
    static void validateColumnLinks(AppPage page, AppPageControl control, List<String> actionIds) {
        if (control.getColumnLinks().isEmpty()) return;
        String where = "Control '" + describe(control) + "'";
        if (!"grid".equals(control.getType()))
            throw new IllegalArgumentException(where + " is a " + control.getType()
                    + " — only a grid has clickable columns");
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
     * A tab set holds grids that are on the same page and holds each of them once. Both checks are
     * about the same thing: a tab is only a place to put a grid, so a name in the list that answers
     * to no grid — or to a grid another tab set has already claimed — leaves the page with a tab
     * that shows nothing, or with a grid whose home has two answers. Neither survives a save.
     */
    private void validateTabs(AppPage page) {
        List<String> claimed = new ArrayList<>();
        for (AppPageControl control : page.getControls()) {
            if (!"tabs".equals(control.getType())) continue;
            String where = "Tabs control '" + describe(control) + "'";
            for (String id : control.getTabControlIds()) {
                AppPageControl child = page.getControls().stream()
                        .filter(c -> id.equals(c.getControlId())).findFirst().orElse(null);
                if (child == null)
                    throw new IllegalArgumentException(where + " holds a control that is not on this page: " + id);
                if (!"grid".equals(child.getType()))
                    throw new IllegalArgumentException(where + " holds a " + child.getType() + " — a tab set holds grids");
                if (claimed.contains(id))
                    throw new IllegalArgumentException("Grid '" + describe(child) + "' is in more than one tab set");
                claimed.add(id);
            }
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
     * Checks the page's transform library and hands back its names for the actions to be checked
     * against. A blank name would be unnameable and a duplicate would make "which step does this
     * action run" unanswerable, so both are refused rather than silently picking one. Only a JSONata
     * step needs an expression: an XML-to-JSON step is fully described by its type.
     */
    private List<String> validateTransforms(AppPage page) {
        List<String> names = new ArrayList<>();
        for (AppPageTransform transform : page.getTransforms()) {
            requireName(transform.getName(), "Transform name");
            if (names.contains(transform.getName()))
                throw new IllegalArgumentException("Duplicate transform name: " + transform.getName());
            names.add(transform.getName());
            if (!transform.isXml2Json()
                    && (transform.getExpression() == null || transform.getExpression().isBlank()))
                throw new IllegalArgumentException("Transform '" + transform.getName() + "' has no JSONata expression");
        }
        return names;
    }

    /** The instance an action runs, the transforms it chains and the control it fills all have to be real. */
    private void validateAction(AppPage page, AppPageAction action, List<String> transformNames, String where) {
        requireInstance(action.getAppUseCaseInstanceId(), where);
        for (String name : action.getTransformNames()) {
            if (!transformNames.contains(name))
                throw new IllegalArgumentException(where + " applies a transform that is not on this page: " + name);
        }

        String target = action.getTargetControlId();
        if (target == null || target.isBlank() || AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl targetControl = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (targetControl == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        if (!TARGET_TYPES.contains(targetControl.getType()))
            throw new IllegalArgumentException(where
                    + " must target a grid, select, text, text area, link or pie chart, not a "
                    + targetControl.getType());
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
