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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Loads and persists {@link McpToolSetting}s as one JSON array at {@code ${DATADIR}/mcptools.json} —
 * the same resolve/read/write pattern as {@link com.mycompany.batch.pagepreference.PagePreferenceService}.
 */
@Service
public class McpToolSettingService {

    private static final String CONFIG_RESOURCE = "mcptools.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<McpToolSetting> settings = new CopyOnWriteArrayList<>();

    public McpToolSettingService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() {
        settings.addAll(readConfigFile());
    }

    public McpToolSetting get(String pageName, String actionKey) {
        for (McpToolSetting s : settings) {
            if (s.pageName().equals(pageName) && s.actionKey().equals(actionKey)) return s;
        }
        return null;
    }

    public synchronized McpToolSetting save(McpToolSetting setting) throws Exception {
        if (setting.pageName() == null || setting.pageName().isBlank())
            throw new IllegalArgumentException("pageName is required");
        if (setting.actionKey() == null || setting.actionKey().isBlank())
            throw new IllegalArgumentException("actionKey is required");

        put(setting);
        writeConfigFile();
        return setting;
    }

    /** Several settings in one write, e.g. switching every action on a page. */
    public synchronized void saveAll(List<McpToolSetting> batch) throws Exception {
        batch.forEach(this::put);
        writeConfigFile();
    }

    private void put(McpToolSetting setting) {
        settings.removeIf(s -> s.pageName().equals(setting.pageName()) && s.actionKey().equals(setting.actionKey()));
        settings.add(setting);
    }

    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private List<McpToolSetting> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<McpToolSetting>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), settings);
    }
}
