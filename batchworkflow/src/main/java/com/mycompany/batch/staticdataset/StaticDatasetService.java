package com.mycompany.batch.staticdataset;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.mycompany.batch.appcatalog.JsonataLibraryService;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.onedrive.OneDriveClient;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
 * <p>Row data fetched from each dataset's configured source (file, HTTP, pasted text or OneDrive) is cached in
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
    private final OneDriveClient oneDriveClient;
    private final JsonataLibraryService jsonataLibrary;

    private final Map<String, StaticDatasetDef> defs  = new ConcurrentHashMap<>();
    private final Map<String, DatasetState>     state = new ConcurrentHashMap<>();

    public StaticDatasetService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader,
                                OneDriveClient oneDriveClient, JsonataLibraryService jsonataLibrary) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
        this.oneDriveClient = oneDriveClient;
        this.jsonataLibrary = jsonataLibrary;
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

    // -------------------------------------------------------------------------
    // Filtering — the same AND-ed attribute/operator/value conditions the UI has always
    // offered, evaluated here rather than in the browser. Moving it server-side is what lets a
    // caller ask for "the rows that match" instead of downloading every row to throw most of
    // them away, and is what makes a saved favourite usable by a caller that has no UI at all.
    // -------------------------------------------------------------------------

    /**
     * Rows matching a filter, alongside how many there were before it — {@code total} is what
     * makes "23 of 500 rows match" answerable without a second call.
     */
    public record FilterResult(
            List<Map<String, Object>> rows,
            int total,
            List<String> attributes,
            String loadedTime,
            String error) {}

    /**
     * Applies {@code favoriteName}'s saved conditions and {@code conditions} together — every one
     * of them has to match, so naming a favourite and passing conditions narrows that favourite
     * rather than replacing it. Both empty matches every row.
     *
     * @throws IllegalArgumentException if the dataset, or a named favourite, does not exist. An
     *         unknown favourite is refused rather than quietly matching everything: a caller asking
     *         for one filter's worth of services should not silently be handed all of them.
     */
    public FilterResult filter(String name, String favoriteName, List<FilterFavorite.FilterCondition> conditions) {
        StaticDatasetDef def = defs.get(name);
        if (def == null) throw new IllegalArgumentException("Unknown static dataset: " + name);

        List<FilterFavorite.FilterCondition> active = new ArrayList<>();
        if (favoriteName != null && !favoriteName.isBlank()) {
            active.addAll(favoriteConditions(def, favoriteName.trim()));
        }
        if (conditions != null) active.addAll(conditions);
        active.removeIf(c -> c == null || c.getAttribute() == null || c.getAttribute().isBlank());

        DatasetState s = state.get(name);
        List<Map<String, Object>> all = s != null ? s.rows() : List.of();

        List<Map<String, Object>> matched;
        if (active.isEmpty()) {
            matched = new ArrayList<>(all);
        } else {
            matched = new ArrayList<>();
            for (Map<String, Object> row : all) {
                if (matchesAll(row, active)) matched.add(row);
            }
        }
        return new FilterResult(
                matched,
                all.size(),
                s != null ? s.attributes() : def.getAttributes(),
                s != null ? s.loadedTime() : null,
                s != null ? s.error() : null);
    }

    private List<FilterFavorite.FilterCondition> favoriteConditions(StaticDatasetDef def, String favoriteName) {
        for (FilterFavorite f : def.getFavorites()) {
            if (favoriteName.equals(f.getName())) return f.getConditions();
        }
        throw new IllegalArgumentException(
                "Unknown favorite '" + favoriteName + "' on static dataset '" + def.getName() + "'");
    }

    private boolean matchesAll(Map<String, Object> row, List<FilterFavorite.FilterCondition> conditions) {
        for (FilterFavorite.FilterCondition c : conditions) {
            if (!matches(row, c)) return false;
        }
        return true;
    }

    /** Case-insensitive, and a missing attribute reads as empty — the same rules the widget applied. */
    private boolean matches(Map<String, Object> row, FilterFavorite.FilterCondition c) {
        Object raw = row.get(c.getAttribute());
        String a = raw == null ? "" : String.valueOf(raw).toLowerCase(Locale.ROOT);
        String b = c.getValue() == null ? "" : c.getValue().toLowerCase(Locale.ROOT);
        return switch (c.getOp() == null ? "equals" : c.getOp()) {
            case "notEquals"   -> !a.equals(b);
            case "contains"    -> a.contains(b);
            case "notContains" -> !a.contains(b);
            case "startsWith"  -> a.startsWith(b);
            case "endsWith"    -> a.endsWith(b);
            case "equals"      -> a.equals(b);
            default            -> true;
        };
    }

    public synchronized StaticDatasetDef save(StaticDatasetDef def) throws Exception {
        if (def.getName() == null || !def.getName().matches("[\\w\\-]+"))
            throw new IllegalArgumentException("name is required and must contain only word characters or dashes");
        if (def.getSource() == null || !List.of("file", "http", "paste", "json", "onedrive").contains(def.getSource()))
            throw new IllegalArgumentException("source must be 'file', 'http', 'paste', 'json' or 'onedrive'");
        if (def.getLocation() == null || def.getLocation().isBlank())
            throw new IllegalArgumentException("location is required");
        // Compile the expression now rather than letting a typo surface as a failed load: a dataset
        // that cannot parse its own transform is worth refusing while the person is still looking at it.
        if (def.getJsonata() != null && JsonataLibraryService.refName(def.getJsonata()) == null) {
            try {
                Jsonata.jsonata(def.getJsonata().trim());
            } catch (Exception e) {
                throw new IllegalArgumentException("jsonata does not parse: " + describe(e));
            }
        }

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
                case "json"  -> extractRows(def.getLocation(), def.getArrayElement(), def.getJsonata());
                case "onedrive" -> loadFromOneDrive(def.getLocation(), def.getArrayElement());
                default      -> loadFromHttp(def.getLocation(), def.getArrayElement(), def.getJsonata());
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
            // Don't trim the whole line: trim() strips tabs too, which would drop a leading empty
            // cell and shift every value one column left. Each value is trimmed below instead.
            String line = lines.get(i);
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

    /**
     * An Excel workbook (or a delimited .csv/.txt) in work OneDrive; {@code sheet} is the dataset's
     * arrayElement, naming the worksheet to read (blank = first sheet).
     */
    private List<Map<String, Object>> loadFromOneDrive(String location, String sheet) throws Exception {
        String lower = location.trim().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".csv") || lower.endsWith(".txt")) {
            String text = new String(oneDriveClient.download(location), StandardCharsets.UTF_8);
            return loadFromPaste(text.startsWith("﻿") ? text.substring(1) : text);
        }
        return oneDriveClient.readExcelRows(location, sheet);
    }

    private List<Map<String, Object>> loadFromHttp(String url, String arrayElement, String jsonata) throws Exception {
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

        return extractRows(resp.body(), arrayElement, jsonata);
    }

    /**
     * Pulls the rows out of a JSON document — the body of an HTTP response (source=http), or the JSON
     * pasted into the dataset itself (source=json). {@code arrayElement} is a JSONPath selecting the
     * array of objects (default {@code $}); {@code jsonata}, when set, then reshapes what it selected.
     * Either way each object's keys become the attributes.
     *
     * <p>The JSONPath runs first and the JSONata second, so adding an expression to a dataset that
     * already had an arrayElement starts from the rows that arrayElement was picking instead of
     * having to find them again. Nothing needs to be an array until the end of that pipeline: with a
     * JSONata set, what the path selected may be an object the expression turns into rows — a map
     * keyed by id, needing {@code $each}, being the usual reason to want one.
     */
    private List<Map<String, Object>> extractRows(String json, String arrayElement, String jsonata) throws Exception {
        if (json == null || json.isBlank()) return List.of();

        String path = arrayElement == null || arrayElement.isBlank() ? "$" : arrayElement.trim();
        Object document = JsonPath.using(JSONPATH_CONF).parse(json).json();
        Object extracted = JsonPath.using(JSONPATH_CONF).parse(document).read(path);

        boolean transformed = jsonata != null && !jsonata.isBlank();
        if (transformed) extracted = applyJsonata(extracted, jsonata);

        return toRows(extracted, path, transformed);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> toRows(Object value, String path, boolean transformed) {
        // An expression that matched nothing returns the empty sequence, not an empty array: a filter
        // no row satisfies is an answer, so it loads as a dataset with no rows rather than as a
        // failure. Without one, a null is the JSONPath missing, which is still worth reporting.
        if (value == null && transformed) return List.of();

        // A single object is read as a one-row dataset rather than refused — a document is often
        // narrowed down to one object before it gets pasted, and an expression that builds one row
        // is a reasonable thing to have written.
        if (value instanceof Map<?, ?> single) return List.of((Map<String, Object>) single);

        if (!(value instanceof List<?> list)) {
            throw new RuntimeException(transformed
                    ? "the JSONata did not return an array of objects"
                    : "arrayElement '" + path + "' did not resolve to an array");
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> m) rows.add((Map<String, Object>) m);
        }
        return rows;
    }

    /**
     * Runs the dataset's expression over what the JSONPath selected. The value is round-tripped
     * through Jackson first so the evaluator is handed plain maps and lists rather than whatever
     * types the JsonPath provider happens to return — the same thing
     * {@link com.mycompany.batch.service.BatchService} does before evaluating one. Key order is
     * insertion order on both sides of the trip, so the columns keep the order the document had.
     */
    private Object applyJsonata(Object value, String expression) throws Exception {
        String expr = resolveJsonataExpression(expression);
        Object input = objectMapper.readValue(objectMapper.writeValueAsString(value), Object.class);
        try {
            return Jsonata.jsonata(expr).evaluate(input);
        } catch (Exception e) {
            throw new RuntimeException("JSONata failed: " + describe(e), e);
        }
    }

    /**
     * The expression itself, or the one the shared library holds under a {@code catalog:<name>}
     * reference — the same form the rest of the tools accept, so a dataset can name a transform
     * that is maintained in one place instead of keeping its own copy of it.
     */
    private String resolveJsonataExpression(String value) {
        String libraryName = JsonataLibraryService.refName(value);
        if (libraryName == null) return value.trim();
        String expression = jsonataLibrary.expressionOf(libraryName);
        if (expression == null)
            throw new IllegalArgumentException("JSONata '" + libraryName + "' is not in the shared library");
        return expression;
    }

    private static String describe(Exception e) {
        return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
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
