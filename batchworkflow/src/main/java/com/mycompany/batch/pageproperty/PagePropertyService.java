package com.mycompany.batch.pageproperty;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Loads and persists {@link PageProperty} settings — all pages' properties live together as one
 * JSON array at {@code ${DATADIR}/pageproperties.json}, mirroring
 * {@link com.mycompany.batch.pagepreference.PagePreferenceService}'s save pattern so settings
 * survive restarts and are shared across whoever opens the page.
 */
@Service
public class PagePropertyService {

    private static final String CONFIG_RESOURCE = "pageproperties.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<PageProperty> pages = new CopyOnWriteArrayList<>();

    public PagePropertyService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() throws Exception {
        ensureConfigFileExists();
        pages.addAll(readConfigFile());
    }

    public List<PageProperty> list() {
        return new ArrayList<>(pages);
    }

    public Map<String, String> getProperties(String page) {
        for (PageProperty p : pages) {
            if (p.getPage().equals(page)) return new LinkedHashMap<>(p.getProperties());
        }
        return new LinkedHashMap<>();
    }

    public synchronized PageProperty setProperty(String page, String key, String value) throws Exception {
        if (page == null || page.isBlank()) throw new IllegalArgumentException("page is required");
        if (key == null || key.isBlank()) throw new IllegalArgumentException("key is required");

        PageProperty existing = null;
        for (PageProperty p : pages) {
            if (p.getPage().equals(page)) { existing = p; break; }
        }
        if (existing == null) {
            existing = new PageProperty();
            existing.setPage(page);
            pages.add(existing);
        }
        existing.getProperties().put(key, value);
        writeConfigFile();
        return existing;
    }

    public synchronized void deleteProperty(String page, String key) throws Exception {
        for (PageProperty p : pages) {
            if (p.getPage().equals(page)) {
                p.getProperties().remove(key);
                if (p.getProperties().isEmpty()) pages.remove(p);
                break;
            }
        }
        writeConfigFile();
    }

    // -------------------------------------------------------------------------
    // Persistence — same resolve/read/write pattern as PagePreferenceService
    // -------------------------------------------------------------------------

    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private void ensureConfigFileExists() throws Exception {
        Path path = resolveConfigPath();
        if (Files.isRegularFile(path)) return;
        Files.createDirectories(path.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), new ArrayList<PageProperty>());
    }

    private List<PageProperty> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<PageProperty>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), pages);
    }
}
