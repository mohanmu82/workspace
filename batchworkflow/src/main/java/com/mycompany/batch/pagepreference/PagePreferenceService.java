package com.mycompany.batch.pagepreference;

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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Loads and persists {@link PagePreference} field-mapping definitions — all preferences for
 * every page live together as one JSON array at {@code ${DATADIR}/pagepreferences.json},
 * mirroring {@link com.mycompany.batch.dashboardview.DashboardViewService}'s save pattern so
 * preferences survive restarts and are shared across whoever opens the page.
 */
@Service
public class PagePreferenceService {

    private static final String CONFIG_RESOURCE = "pagepreferences.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<PagePreference> preferences = new CopyOnWriteArrayList<>();

    public PagePreferenceService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() throws Exception {
        ensureConfigFileExists();
        preferences.addAll(readConfigFile());
    }

    public List<PagePreference> list() {
        return new ArrayList<>(preferences);
    }

    public PagePreference get(String page, String datasetName) {
        for (PagePreference p : preferences) {
            if (p.getPage().equals(page) && p.getDatasetName().equals(datasetName)) return p;
        }
        return null;
    }

    public synchronized PagePreference save(PagePreference pref) throws Exception {
        if (pref.getPage() == null || pref.getPage().isBlank())
            throw new IllegalArgumentException("page is required");
        if (pref.getDatasetName() == null || pref.getDatasetName().isBlank())
            throw new IllegalArgumentException("datasetName is required");

        preferences.removeIf(p -> p.getPage().equals(pref.getPage()) && p.getDatasetName().equals(pref.getDatasetName()));
        preferences.add(pref);
        writeConfigFile();
        return pref;
    }

    public synchronized void delete(String page, String datasetName) throws Exception {
        preferences.removeIf(p -> p.getPage().equals(page) && p.getDatasetName().equals(datasetName));
        writeConfigFile();
    }

    // -------------------------------------------------------------------------
    // Persistence — same resolve/read/write pattern as DashboardViewService
    // -------------------------------------------------------------------------

    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private void ensureConfigFileExists() throws Exception {
        Path path = resolveConfigPath();
        if (Files.isRegularFile(path)) return;
        Files.createDirectories(path.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), new ArrayList<PagePreference>());
    }

    private List<PagePreference> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<PagePreference>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), preferences);
    }
}
