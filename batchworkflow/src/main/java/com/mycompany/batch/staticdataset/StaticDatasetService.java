package com.mycompany.batch.staticdataset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mycompany.batch.config.ServerPropertiesLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Loads, persists, and refreshes {@link StaticDatasetDef} definitions.
 *
 * <p>All definitions — including each dataset's saved {@link FilterFavorite} favorites —
 * live together as a single JSON array at {@code ${DATADIR}/staticdatasets.json}, (re)loaded
 * at startup. Edits made through the UI are written back to the same file.
 *
 * <p>Row data fetched from each dataset's configured source (file or HTTP) is cached in
 * memory keyed by dataset name; consumers such as the Service Dashboard read the cached
 * rows rather than re-fetching on every page load.
 */
@Service
public class StaticDatasetService {

    private static final String CONFIG_RESOURCE = "staticdatasets.json";

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Configuration JSONPATH_CONF = Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS);

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public record DatasetState(
            List<Map<String, Object>> rows,
            List<String> attributes,
            String loadedTime,
            String error) {}

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;

    private final Map<String, StaticDatasetDef> defs  = new ConcurrentHashMap<>();
    private final Map<String, DatasetState>     state = new ConcurrentHashMap<>();

    public StaticDatasetService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void loadAll() {
        for (StaticDatasetDef def : readConfigFile()) {
            if (def.getName() == null || def.getName().isBlank()) continue;
            defs.put(def.getName(), def);
        }
        // Best-effort eager load so consumers see data immediately after startup.
        for (String name : new ArrayList<>(defs.keySet())) {
            try { reload(name); } catch (Exception ignored) { }
        }
    }

    public List<StaticDatasetDef> list() {
        return new ArrayList<>(defs.values());
    }

    public StaticDatasetDef get(String name) {
        return defs.get(name);
    }

    public DatasetState getState(String name) {
        return state.get(name);
    }

    public synchronized StaticDatasetDef save(StaticDatasetDef def) throws Exception {
        if (def.getName() == null || !def.getName().matches("[\\w\\-]+"))
            throw new IllegalArgumentException("name is required and must contain only word characters or dashes");
        if (def.getSource() == null || !List.of("file", "http", "paste").contains(def.getSource()))
            throw new IllegalArgumentException("source must be 'file', 'http' or 'paste'");
        if (def.getLocation() == null || def.getLocation().isBlank())
            throw new IllegalArgumentException("location is required");

        defs.put(def.getName(), def);
        writeConfigFile();
        return def;
    }

    public synchronized StaticDatasetDef addOrUpdateFavorite(String datasetName, FilterFavorite favorite) throws Exception {
        StaticDatasetDef def = defs.get(datasetName);
        if (def == null) throw new IllegalArgumentException("Unknown static dataset: " + datasetName);
        if (favorite.getName() == null || favorite.getName().isBlank())
            throw new IllegalArgumentException("favorite name is required");

        List<FilterFavorite> favs = def.getFavorites();
        favs.removeIf(f -> favorite.getName().equals(f.getName()));
        favs.add(favorite);
        writeConfigFile();
        return def;
    }

    public synchronized void deleteFavorite(String datasetName, String favoriteName) throws Exception {
        StaticDatasetDef def = defs.get(datasetName);
        if (def == null) throw new IllegalArgumentException("Unknown static dataset: " + datasetName);
        def.getFavorites().removeIf(f -> favoriteName.equals(f.getName()));
        writeConfigFile();
    }

    public synchronized void delete(String name) throws Exception {
        defs.remove(name);
        state.remove(name);
        writeConfigFile();
    }

    /** Fetches fresh data from the dataset's source, updates the cached state and discovered attributes. */
    public DatasetState reload(String name) throws Exception {
        StaticDatasetDef def = defs.get(name);
        if (def == null) throw new IllegalArgumentException("Unknown static dataset: " + name);

        try {
            List<Map<String, Object>> rows = switch (def.getSource()) {
                case "file"  -> loadFromFile(def.getLocation());
                case "paste" -> loadFromPaste(def.getLocation());
                default      -> loadFromHttp(def.getLocation(), def.getArrayElement());
            };

            List<String> attributes = computeAttributes(rows);
            def.setAttributes(attributes);
            try { synchronized (this) { writeConfigFile(); } } catch (Exception ignored) { }

            DatasetState s = new DatasetState(rows, attributes, LocalDateTime.now().format(FMT), null);
            state.put(name, s);
            return s;
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            DatasetState previous = state.get(name);
            DatasetState s = new DatasetState(
                    previous != null ? previous.rows() : List.of(),
                    previous != null ? previous.attributes() : def.getAttributes(),
                    previous != null ? previous.loadedTime() : null,
                    msg);
            state.put(name, s);
            throw e;
        }
    }

    // -------------------------------------------------------------------------
    // Data loading
    // -------------------------------------------------------------------------

    private List<Map<String, Object>> loadFromFile(String path) throws Exception {
        return parseDelimitedLines(Files.readAllLines(Path.of(path)));
    }

    /** Rows pasted (as tab-separated text) directly from Excel; {@code raw} is stored verbatim as the dataset's location. */
    private List<Map<String, Object>> loadFromPaste(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        List<String> lines = Arrays.asList(raw.replace("\r\n", "\n").replace("\r", "\n").split("\n", -1));
        return parseDelimitedLines(lines);
    }

    /** Splits header + data lines on the first delimiter found among tab, comma, pipe (in that priority order). */
    private List<Map<String, Object>> parseDelimitedLines(List<String> lines) {
        if (lines.isEmpty()) return List.of();

        String header    = lines.get(0);
        String delimiter = header.contains("\t") ? "\t" : header.split(",", -1).length > 1 ? "," : "|";
        String[] headers = Arrays.stream(header.split(Pattern.quote(delimiter), -1))
                .map(String::trim).toArray(String[]::new);
        String delimPat = Pattern.quote(delimiter);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isBlank()) continue;
            String[] vals = line.split(delimPat, -1);
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < headers.length; j++) {
                row.put(headers[j], j < vals.length ? vals[j].trim() : "");
            }
            rows.add(row);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadFromHttp(String url, String arrayElement) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new RuntimeException("HTTP " + resp.statusCode() + " fetching " + url);
        }

        String path = arrayElement == null || arrayElement.isBlank() ? "$" : arrayElement.trim();
        Object document = JsonPath.using(JSONPATH_CONF).parse(resp.body()).json();
        Object extracted = JsonPath.using(JSONPATH_CONF).parse(document).read(path);

        if (!(extracted instanceof List<?> list)) {
            throw new RuntimeException("arrayElement '" + path + "' did not resolve to an array");
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> m) rows.add((Map<String, Object>) m);
        }
        return rows;
    }

    private List<String> computeAttributes(List<Map<String, Object>> rows) {
        List<String> attrs = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            for (String k : row.keySet()) {
                if (seen.add(k)) attrs.add(k);
            }
        }
        return attrs;
    }

    // -------------------------------------------------------------------------
    // Persistence — single staticdatasets.json holding every dataset (+ its favorites)
    // -------------------------------------------------------------------------

    /**
     * Resolves the on-disk location of {@code staticdatasets.json} under {@code ${DATADIR}}.
     * Reads and writes always go through this same method so they can never diverge.
     */
    private Path resolveConfigPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve(CONFIG_RESOURCE);
    }

    private List<StaticDatasetDef> readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return new ArrayList<>();
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<List<StaticDatasetDef>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void writeConfigFile() throws Exception {
        List<StaticDatasetDef> all = new ArrayList<>(defs.values());
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), all);
    }
}
