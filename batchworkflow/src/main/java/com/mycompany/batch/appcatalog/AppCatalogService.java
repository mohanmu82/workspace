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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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

    /** Control types an action can put a response into. */
    private static final List<String> TARGET_TYPES = List.of("grid", "select", "text", "textarea");

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

        for (AppPageControl control : page.getControls()) {
            for (String id : control.getActionIds()) {
                if (!actionIds.contains(id))
                    throw new IllegalArgumentException("Control '" + describe(control)
                            + "' triggers an action that is not on this page: " + id);
            }
            if (!ACTION_TYPES.contains(control.getType())) continue;
            for (AppPageAction action : control.getActions()) {
                validateAction(page, action, transformNames, "Action '" + actionName(action, describe(control)) + "'");
            }
        }

        for (String id : page.getOnLoadActionIds()) {
            if (!actionIds.contains(id))
                throw new IllegalArgumentException("The page's on-load list names an action that is not on this page: " + id);
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
        return ids;
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
            throw new IllegalArgumentException(where + " must target a grid, select, text or text area, not a "
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
