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

    /** Removes the environment and any instance pinned to it. */
    public synchronized void deleteEnvironment(String appName, String environment) throws Exception {
        environments.removeIf(e -> appName.equals(e.getAppName()) && environment.equals(e.getEnvironment()));
        List<String> orphaned = instances.stream()
                .filter(i -> appName.equals(i.getAppName()) && environment.equals(i.getAppEnvironment()))
                .map(AppUseCaseInstance::getAppUseCaseInstanceId)
                .collect(Collectors.toList());
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

    /** Saves an instance, generating the id when the caller did not supply one (i.e. on create). */
    public synchronized AppUseCaseInstance saveInstance(AppUseCaseInstance instance) throws Exception {
        requireName(instance.getAppName(), "appName");
        requireName(instance.getAppUseCaseName(), "appUseCaseName");
        requireName(instance.getAppEnvironment(), "appEnvironment");
        if (getUseCase(instance.getAppName(), instance.getAppUseCaseName()) == null)
            throw new IllegalArgumentException("Unknown use case: "
                    + instance.getAppName() + "/" + instance.getAppUseCaseName());
        if (getEnvironment(instance.getAppName(), instance.getAppEnvironment()) == null)
            throw new IllegalArgumentException("Unknown environment: "
                    + instance.getAppName() + "/" + instance.getAppEnvironment());

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
