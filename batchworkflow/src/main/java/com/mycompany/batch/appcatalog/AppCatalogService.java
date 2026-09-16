package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
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

    private final List<AppDefinition>           apps         = new CopyOnWriteArrayList<>();
    private final List<AppEnvironment>          environments = new CopyOnWriteArrayList<>();
    private final List<AppUseCase>              useCases     = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstance>      instances    = new CopyOnWriteArrayList<>();
    private final List<AppUseCaseInstanceGroup> groups       = new CopyOnWriteArrayList<>();
    private final List<AppPage>                 pages        = new CopyOnWriteArrayList<>();

    public AppCatalogService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader,
                             StaticDatasetService staticDatasets) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
        this.staticDatasets = staticDatasets;
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
     * fields of each row. Mirrors TARGET_TYPES in apppage.html.
     */
    private static final List<String> TARGET_TYPES =
            List.of("grid", "select", "multiselect", "text", "textarea", "link", "pie", "piegrid", "bar", "timeseries");

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
     */
    private static final List<String> TRIGGERLESS_TYPES = List.of("grid", "label", "tabs", "hidden", "pie", "piegrid", "bar", "timeseries", "page");

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
            validateDisplayFilterExpression(control, where);
            validateGridStatus(control, variableNames, where);
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
     * What a slice colour may be written as: a name, a #hex, or an rgb()/hsl() function — the same
     * characters {@code cssColor} in apppage.html keeps, and no others.
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
     * every grid it opens. Mirrors GRID_OWNER_TYPES in apppage.html.
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
     * What a tab set may hold: a grid, or any of the charts. A chart is the same kind of thing as a
     * grid as far as a tab is concerned — one reading of one call, wanting the full width and a few
     * rows of height — and a page that answers a question with a grid, a pie of it and a bar chart
     * beside them reads far better as three tabs than as three controls down a screen.
     *
     * <p>A tab set is not on the list, and cannot be: a set holding itself has no depth at which it
     * stops being drawn.
     */
    private static final List<String> TAB_CHILD_TYPES = List.of("grid", "pie", "piegrid", "bar", "timeseries");

    /**
     * A tab set holds grids and charts that are on the same page and holds each of them once. Both
     * checks are about the same thing: a tab is only a place to put one of those, so a name in the
     * list that answers to nothing on the page — or to a control another tab set has already claimed
     * — leaves the page with a tab that shows nothing, or with a grid whose home has two answers.
     * Neither survives a save.
     */
    static void validateTabs(AppPage page) {
        List<String> claimed = new ArrayList<>();
        for (AppPageControl control : page.getControls()) {
            if (!"tabs".equals(control.getType())) continue;
            String where = "Tabs control '" + describe(control) + "'";
            for (String id : control.getTabControlIds()) {
                AppPageControl child = page.getControls().stream()
                        .filter(c -> id.equals(c.getControlId())).findFirst().orElse(null);
                if (child == null)
                    throw new IllegalArgumentException(where + " holds a control that is not on this page: " + id);
                if (!TAB_CHILD_TYPES.contains(child.getType()))
                    throw new IllegalArgumentException(where + " holds a " + child.getType()
                            + " — a tab set holds grids and charts");
                if (claimed.contains(id))
                    throw new IllegalArgumentException("Control '" + describe(child) + "' is in more than one tab set");
                claimed.add(id);
            }
            String preferred = control.getDefaultTabControlId();
            if (preferred != null && !control.getTabControlIds().contains(preferred))
                throw new IllegalArgumentException(where + " opens on a tab it does not hold: " + preferred);
        }
    }

    /** The controls drawn out of rows. Mirrors CHART_TYPES in apppage.html. */
    private static final List<String> CHART_TYPES = List.of("pie", "piegrid", "bar", "timeseries");

    /**
     * A time series chart's own settings: it has to name the column its time is read from, and its
     * filters each have to name a column and a test that exists. Neither is saved on any other kind
     * of control, where nothing would ever read them.
     */
    static void validateTimeSeries(AppPageControl control, String where) {
        boolean series = "timeseries".equals(control.getType());
        if (!series) {
            if (control.getTimeField() != null)
                throw new IllegalArgumentException(where + " is a " + control.getType() + " — only a time series chart has a time column");
            if (!control.getChartFilters().isEmpty())
                throw new IllegalArgumentException(where + " is a " + control.getType() + " — only a time series chart filters its rows");
            return;
        }
        if (control.getTimeField() == null)
            throw new IllegalArgumentException(where + " is a time series chart but names no time column");
        for (AppPageRowFilter filter : control.getChartFilters()) {
            if (filter.column() == null || filter.column().isBlank())
                throw new IllegalArgumentException(where + " has a filter that names no column");
            if (!AppPageRowFilter.OPERATORS.contains(filter.operatorOrDefault()))
                throw new IllegalArgumentException(where + " has a filter with an unknown test: " + filter.operator());
        }
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
            validateTimeSeries(control, where);
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
        // Asked of every action, before the kinds part company: both describe fanning out, and a
        // performance action is refused one — so filters or columns written on either kind are
        // wiring that could never run.
        validateRowFilters(action, where);
        validateRowColumns(action, where);
        validateExtraBindings(page, action, transformNames, where);
        validateEnrichColumns(page, action, name -> staticDatasets.get(name) != null, where);
        validatePivots(page, action, where);
        validateActionRowErrorExpression(action, where);
        if (action.isPerformance()) {
            validatePerformanceAction(page, action, where);
            return;
        }
        requireInstance(action.getAppUseCaseInstanceId(), where);
        for (String name : action.getTransformNames()) {
            if (!transformNames.contains(name))
                throw new IllegalArgumentException(where + " applies a transform that is not on this page: " + name);
        }
        validateRowSource(page, action, where);
        validateActionTarget(page, action, where);
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
                    + " must target a grid, select, text, text area, link, pie chart, bar chart or time series chart, not a "
                    + targetControl.getType());
        // Every row's answer becoming a row of one grid only means anything where rows can go.
        if (action.isRowFanOut() && !"grid".equals(targetControl.getType()))
            throw new IllegalArgumentException(where + " runs once per row and collects the answers into one grid, "
                    + "so it must target a grid, not a " + targetControl.getType());
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
        for (AppPageRowFilter filter : action.getRowFilters()) {
            if (filter.column() == null || filter.column().isBlank())
                throw new IllegalArgumentException(where + " has a row filter that names no column");
            if (!AppPageRowFilter.OPERATORS.contains(filter.operatorOrDefault()))
                throw new IllegalArgumentException(where + " has a row filter with an unknown test: " + filter.operator());
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
            requireGridTarget(page, action.getTargetControlId(), action.isTabsPerRow(), where);
            checkEnrichColumns(page, action.getEnrichColumns(), datasetKnown, where);
        }
        int n = 1;
        for (AppPageBinding binding : action.getExtraBindings()) {
            n++;
            if (binding == null || binding.getEnrichColumns().isEmpty()) continue;
            String on = where + " target " + n;
            requireGridTarget(page, binding.getTargetControlId(), false, on);
            checkEnrichColumns(page, binding.getEnrichColumns(), datasetKnown, on);
        }
    }

    private static void requireGridTarget(AppPage page, String target, boolean tabsPerRow, String where) {
        if (target == null || target.isBlank())
            throw new IllegalArgumentException(where + " has enriched columns but no grid to add them to — "
                    + "aim it at a grid, or take the columns off");
        if (AppPageAction.NEW_GRID.equals(target)) return;
        AppPageControl control = page.getControls().stream()
                .filter(c -> target.equals(c.getControlId())).findFirst().orElse(null);
        if (control == null)
            throw new IllegalArgumentException(where + " targets a control that is not on this page");
        String wanted = tabsPerRow ? "tabs" : "grid";
        if (!wanted.equals(control.getType()))
            throw new IllegalArgumentException(where + " has enriched columns, which only a grid has rows for — "
                    + "its target is a " + control.getType());
    }

    private static void checkEnrichColumns(AppPage page, List<AppPageEnrichColumn> columns, Predicate<String> datasetKnown, String where) {
        List<String> names = new ArrayList<>();
        for (AppPageEnrichColumn column : columns) {
            if (column == null || column.name() == null || column.name().isBlank())
                throw new IllegalArgumentException(where + " has an enriched column with no name");
            String label = where + " enriched column '" + column.name() + "'";
            if (names.contains(column.name()))
                throw new IllegalArgumentException(where + " has two enriched columns called " + column.name());
            names.add(column.name());
            String kind = column.kindOrDefault();
            if (!AppPageEnrichColumn.KINDS.contains(kind))
                throw new IllegalArgumentException(label + " reads something this page has no idea how to read: "
                        + column.kind());
            boolean intoGrid = AppPageEnrichColumn.GRID_VLOOKUP.equals(kind);
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
            if (isBlank(column.returnColumn()))
                throw new IllegalArgumentException(label + " does not say which " + source + " column to bring back");
        }
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
