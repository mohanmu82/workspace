package com.mycompany.batch.dashboardview;

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
 * Loads and persists {@link DashboardView} definitions — all views for every page live
 * together as one JSON array at {@code ${DATADIR}/dashboardviews.json}, mirroring
 * {@link com.mycompany.batch.staticdataset.StaticDatasetService}'s save pattern so views
 * survive restarts and are shared across whoever opens the dashboard.
 */
@Service
public class DashboardViewService {

    private static final String CONFIG_RESOURCE = "dashboardviews.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<DashboardView> views = new CopyOnWriteArrayList<>();

    public DashboardViewService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() {
        views.addAll(readConfigFile());
    }

    public List<DashboardView> list(String page) {
        List<DashboardView> out = new ArrayList<>();
        for (DashboardView v : views) {
            if (page == null || page.equals(v.getPage())) out.add(v);
        }
        return out;
    }

    public synchronized DashboardView save(DashboardView view) throws Exception {
        if (view.getName() == null || view.getName().isBlank())
            throw new IllegalArgumentException("name is required");

        views.removeIf(v -> v.getName().equals(view.getName()) && v.getPage().equals(view.getPage()));
        views.add(view);
        writeConfigFile();
        return view;
    }

    public synchronized void delete(String page, String name) throws Exception {
        views.removeIf(v -> v.getName().equals(name) && v.getPage().equals(page));
        writeConfigFile();
    }

    // -------------------------------------------------------------------------
    // Persistence — ${DATADIR}/dashboardviews.json, same pattern as StaticDatasetService
    // -------------------------------------------------------------------------

    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private List<DashboardView> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<DashboardView>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), views);
    }
}
