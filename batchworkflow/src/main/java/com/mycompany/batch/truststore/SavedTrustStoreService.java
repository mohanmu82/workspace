package com.mycompany.batch.truststore;

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
 * Loads and persists {@link SavedTrustStore} presets — all of them live together as one JSON
 * array at {@code ${DATADIR}/savedtruststores.json}, mirroring
 * {@link com.mycompany.batch.dashboardview.DashboardViewService}'s save pattern.
 */
@Service
public class SavedTrustStoreService {

    private static final String CONFIG_RESOURCE = "savedtruststores.json";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<SavedTrustStore> presets = new CopyOnWriteArrayList<>();

    public SavedTrustStoreService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() {
        presets.addAll(readConfigFile());
    }

    public List<SavedTrustStore> list() {
        return new ArrayList<>(presets);
    }

    public synchronized SavedTrustStore save(SavedTrustStore preset) throws Exception {
        if (preset.getName() == null || preset.getName().isBlank())
            throw new IllegalArgumentException("name is required");

        if (preset.isDefaultStore()) {
            presets.forEach(p -> p.setDefaultStore(false));
        }
        presets.removeIf(p -> p.getName().equals(preset.getName()));
        presets.add(preset);
        writeConfigFile();
        return preset;
    }

    public synchronized void delete(String name) throws Exception {
        presets.removeIf(p -> p.getName().equals(name));
        writeConfigFile();
    }

    public synchronized SavedTrustStore setDefault(String name) throws Exception {
        SavedTrustStore match = presets.stream().filter(p -> p.getName().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No saved trust store named '" + name + "'"));
        presets.forEach(p -> p.setDefaultStore(p == match));
        writeConfigFile();
        return match;
    }

    // -------------------------------------------------------------------------
    // Persistence — ${DATADIR}/savedtruststores.json, same pattern as DashboardViewService
    // -------------------------------------------------------------------------

    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private List<SavedTrustStore> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<SavedTrustStore>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), presets);
    }
}
