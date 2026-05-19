package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.cache.CacheFactory;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import com.mycompany.batch.service.JoinDatasetCacheService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/batch")
public class JoinDatasetController {

    private final BatchService            batchService;
    private final BatchController         batchController;
    private final JoinDatasetCacheService dsCache;
    private final ObjectMapper            objectMapper;
    private final CacheFactory            cacheFactory;

    public JoinDatasetController(BatchService batchService, BatchController batchController,
                                 JoinDatasetCacheService dsCache, ObjectMapper objectMapper,
                                 CacheFactory cacheFactory) {
        this.batchService    = batchService;
        this.batchController = batchController;
        this.dsCache         = dsCache;
        this.objectMapper    = objectMapper;
        this.cacheFactory    = cacheFactory;
    }

    // -------------------------------------------------------------------------
    // POST /batch/joindataset/{jdName}/load/{dsName}
    //   Body: the dataset "request" block, or {"templateName":"myTemplate"}
    //   Returns: { dsName, count, loadedTime, columns }
    // -------------------------------------------------------------------------
    @PostMapping(value = "/joindataset/{jdName}/load/{dsName}",
                 consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> loadDataset(@PathVariable String jdName,
                                         @PathVariable String dsName,
                                         @RequestBody Map<String, Object> requestBody) {
        if (!jdName.matches("[\\w\\-]+") || !dsName.matches("[\\w\\-]+"))
            return badRequest("invalid name");

        RunRequest req;
        try {
            Object tmpl = requestBody.get("templateName");
            if (tmpl instanceof String tmplName && !tmplName.isBlank()) {
                req = batchController.resolveTemplateToRequest(tmplName);
            } else {
                req = batchController.deserializeRunRequest(requestBody);
            }
        } catch (Exception e) {
            return badRequest("Invalid request: " + e.getMessage());
        }

        Map<String, Object> config = loadConfig(jdName);
        List<Map<String, Object>> columnDefs = getColumnDefs(config, dsName);

        BatchService.BatchResult result;
        try {
            result = batchService.run(req);
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return badRequest("Load failed: " + msg);
        }

        List<Map<String, Object>> rows = result.results() != null
                ? new ArrayList<>(result.results()) : new ArrayList<>();

        // Apply responseProcessor if specified — mirrors what BatchController does
        if (req.responseProcessor() != null && !req.responseProcessor().isBlank()) {
            try {
                Map<String, Object> httpResponse = batchController.buildHttpResponse(
                        req.operation(), result, req.httpThreadCount());
                Object processed = batchService.applyResponseProcessor(httpResponse, req.responseProcessor(), req.operation());
                rows = extractRowList(processed);
            } catch (Exception e) {
                return badRequest("responseProcessor failed: " + e.getMessage());
            }
        }

        // If no columns configured, auto-generate string column defs from data keys
        if (columnDefs.isEmpty() && !rows.isEmpty()) {
            columnDefs = rows.get(0).keySet().stream().map(k -> {
                Map<String, Object> def = new LinkedHashMap<>();
                def.put("name", k); def.put("attribute", k); def.put("type", "string");
                return def;
            }).collect(Collectors.toList());
        }

        rows = remapColumns(rows, columnDefs);
        rows = applyColumnTypes(rows, columnDefs);
        dsCache.store(jdName, dsName, rows);
        storeInCacheFactory(dsName, rows, dsCache.getLoadedTime(jdName, dsName));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("dsName",     dsName);
        response.put("count",      rows.size());
        response.put("loadedTime", dsCache.getLoadedTime(jdName, dsName));
        response.put("columns",    rows.isEmpty() ? List.of() : new ArrayList<>(rows.get(0).keySet()));
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // POST /batch/joindataset/{jdName}/dataset  — add a new dataset definition
    //   Body: { name, request, uniqueKey? }
    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    @PostMapping(value = "/joindataset/{jdName}/dataset",
                 consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> addDataset(@PathVariable String jdName,
                                        @RequestBody Map<String, Object> body) {
        if (!jdName.matches("[\\w\\-]+")) return badRequest("invalid joindataset name");

        String dsName = body.get("name") instanceof String s ? s.trim() : "";
        if (dsName.isBlank() || !dsName.matches("[\\w\\-]+"))
            return badRequest("name is required and must contain only word characters or dashes");

        Object requestObj = body.get("request");
        if (requestObj == null) return badRequest("request is required");

        List<String> uniqueKeyList = new ArrayList<>();
        Object ukRaw = body.get("uniqueKey");
        if (ukRaw instanceof List<?> l) {
            l.forEach(k -> { if (k != null && !k.toString().isBlank()) uniqueKeyList.add(k.toString().trim()); });
        } else if (ukRaw instanceof String uks) {
            Arrays.stream(uks.split("[,\\n]")).map(String::trim)
                  .filter(s -> !s.isBlank()).forEach(uniqueKeyList::add);
        }

        Map<String, Object> config = loadConfig(jdName);
        if (config == null) {
            config = new LinkedHashMap<>();
            config.put("name", jdName);
            config.put("type", "joindataset");
            config.put("datasets", new ArrayList<>());
        }

        List<Map<String, Object>> datasets = config.get("datasets") instanceof List<?>
                ? (List<Map<String, Object>>) config.get("datasets") : new ArrayList<>();

        if (datasets.stream().anyMatch(d -> dsName.equals(d.get("name"))))
            return badRequest("Dataset '" + dsName + "' already exists in " + jdName);

        Map<String, Object> newDs = new LinkedHashMap<>();
        newDs.put("name", dsName);
        newDs.put("request", requestObj);
        if (!uniqueKeyList.isEmpty()) newDs.put("uniqueKey", uniqueKeyList);
        datasets.add(newDs);
        config.put("datasets", datasets);

        try {
            saveConfig(jdName, config);
        } catch (Exception e) {
            return badRequest("Failed to save config: " + e.getMessage());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "added");
        response.put("jdName", jdName);
        response.put("dsName", dsName);
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // POST /batch/joindataset/{jdName}/search
    //   Body: { datasets?: [...], joinAll?: boolean, filters: [{column,op,value}] }
    //   Returns: { data: [...], count: N }
    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    @PostMapping(value = "/joindataset/{jdName}/search",
                 consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> searchDatasets(@PathVariable String jdName,
                                            @RequestBody Map<String, Object> body) {
        if (!jdName.matches("[\\w\\-]+")) return badRequest("invalid joindataset name");

        Map<String, Object> config = loadConfig(jdName);
        if (config == null) return badRequest("joindataset not found: " + jdName);

        List<Map<String, Object>> dsList =
                config.get("datasets") instanceof List<?> l
                        ? (List<Map<String, Object>>) l : List.of();

        List<String> requestedDs = body.get("datasets") instanceof List<?> l
                ? (List<String>) l : null;

        boolean joinAll = !Boolean.FALSE.equals(body.get("joinAll"));

        List<Map<String, String>> filters = body.get("filters") instanceof List<?> fl
                ? fl.stream().map(f -> (Map<String, String>) f).collect(Collectors.toList())
                : List.of();

        // Collect loaded datasets (preserving config order)
        Map<String, List<Map<String, Object>>> loadedData = new LinkedHashMap<>();
        for (Map<String, Object> ds : dsList) {
            String n = (String) ds.get("name");
            if (requestedDs != null && !requestedDs.contains(n)) continue;
            JoinDatasetCacheService.DatasetState state = dsCache.get(jdName, n);
            if (state != null) loadedData.put(n, state.rows());
        }

        if (loadedData.isEmpty())
            return badRequest("No loaded datasets found. Load at least one dataset first.");

        List<Map<String, Object>> result = joinAll && loadedData.size() > 1
                ? joinDatasets(loadedData, dsList)
                : new ArrayList<>(loadedData.values().iterator().next());

        Map<String, Map<String, Object>> columnMeta =
                buildColumnMetaMap(config, new ArrayList<>(loadedData.keySet()));
        result = applyFilters(result, filters, columnMeta);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("data",  result);
        response.put("count", result.size());
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // GET /batch/joindataset/{jdName}/dataset/{dsName}  — cache status
    // -------------------------------------------------------------------------
    @GetMapping("/joindataset/{jdName}/dataset/{dsName}")
    public ResponseEntity<?> getDatasetInfo(@PathVariable String jdName,
                                            @PathVariable String dsName) {
        if (!jdName.matches("[\\w\\-]+") || !dsName.matches("[\\w\\-]+"))
            return badRequest("invalid name");
        JoinDatasetCacheService.DatasetState state = dsCache.get(jdName, dsName);
        Map<String, Object> response = new LinkedHashMap<>();
        if (state == null) {
            response.put("loaded", false);
        } else {
            response.put("loaded",     true);
            response.put("count",      state.rows().size());
            response.put("loadedTime", state.loadedTime());
            response.put("columns",    state.columns());
        }
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> joinDatasets(Map<String, List<Map<String, Object>>> loadedData,
                                                   List<Map<String, Object>> dsList) {
        Set<String> loaded = loadedData.keySet();

        String baseName = null;
        for (Map<String, Object> ds : dsList) {
            String n = (String) ds.get("name");
            if (!loaded.contains(n)) continue;
            List<?> fks = (List<?>) ds.get("foreignKey");
            if (fks == null || fks.isEmpty()) { baseName = n; break; }
        }
        if (baseName == null) baseName = loaded.iterator().next();

        List<Map<String, Object>> result = new ArrayList<>(loadedData.get(baseName));
        Set<String> joined = new LinkedHashSet<>();
        joined.add(baseName);

        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map<String, Object> ds : dsList) {
                String dsName = (String) ds.get("name");
                if (joined.contains(dsName) || !loaded.contains(dsName)) continue;
                List<Map<String, Object>> fks = (List<Map<String, Object>>) ds.get("foreignKey");
                if (fks == null) continue;
                Map<String, Object> fk = fks.stream()
                        .filter(f -> {
                            Object ref = f.get("reference");
                            return ref instanceof Map<?, ?> m && joined.contains(m.get("dataset"));
                        })
                        .findFirst().orElse(null);
                if (fk == null) continue;

                changed = true;
                joined.add(dsName);
                String fkCol = (String) fk.get("column");
                Map<String, Object> ref = (Map<String, Object>) fk.get("reference");
                String refCol = (String) ref.get("column");
                final String finalDsName = dsName;

                // Case-insensitive index on the FK column
                Map<String, List<Map<String, Object>>> idx = new HashMap<>();
                for (Map<String, Object> row : loadedData.get(dsName)) {
                    String k = String.valueOf(row.getOrDefault(fkCol, "")).toLowerCase(Locale.ROOT);
                    idx.computeIfAbsent(k, x -> new ArrayList<>()).add(row);
                }

                List<Map<String, Object>> newResult = new ArrayList<>();
                Set<Map<String, Object>> matchedRightRows =
                        Collections.newSetFromMap(new IdentityHashMap<>());
                List<Map<String, Object>> rightRows = loadedData.get(dsName);

                for (Map<String, Object> row : result) {
                    String kVal = String.valueOf(row.getOrDefault(refCol, "")).toLowerCase(Locale.ROOT);
                    List<Map<String, Object>> matches = idx.get(kVal);
                    if (matches != null && !matches.isEmpty()) {
                        for (Map<String, Object> m : matches) {
                            matchedRightRows.add(m);
                            Map<String, Object> merged = new LinkedHashMap<>(row);
                            for (Map.Entry<String, Object> e : m.entrySet()) {
                                if (e.getKey().equals(fkCol)) continue;
                                String newKey = merged.containsKey(e.getKey())
                                        ? finalDsName + "_" + e.getKey() : e.getKey();
                                merged.put(newKey, e.getValue());
                            }
                            newResult.add(merged);
                        }
                    } else {
                        newResult.add(new LinkedHashMap<>(row));
                    }
                }

                // Full outer join: include right-side rows with no matching left-side row
                for (Map<String, Object> rightRow : rightRows) {
                    if (!matchedRightRows.contains(rightRow)) {
                        newResult.add(new LinkedHashMap<>(rightRow));
                    }
                }

                result = newResult;
            }
        }
        return result;
    }

    private List<Map<String, Object>> applyFilters(List<Map<String, Object>> rows,
                                                   List<Map<String, String>> filters,
                                                   Map<String, Map<String, Object>> columnMeta) {
        if (filters == null || filters.isEmpty()) return rows;
        return rows.stream()
                .filter(row -> filters.stream().allMatch(f -> matchesFilter(row, f, columnMeta)))
                .collect(Collectors.toList());
    }

    private boolean matchesFilter(Map<String, Object> row, Map<String, String> filter,
                                  Map<String, Map<String, Object>> columnMeta) {
        String col = filter.get("column");
        String op  = filter.get("op");
        String val = filter.get("value");
        if (col == null || col.isBlank() || op == null) return true;

        Object rawVal = row.get(col);
        String strVal = rawVal != null ? rawVal.toString() : "";
        String fVal   = val != null ? val : "";

        // Date-aware comparison for date-typed columns
        Map<String, Object> meta = columnMeta != null ? columnMeta.get(col) : null;
        if (meta != null && "date".equals(meta.get("type"))) {
            String fmt = meta.get("format") instanceof String s ? s : null;
            if (fmt != null) {
                return switch (op) {
                    case "greaterThan"        -> compareDates(strVal, fVal, fmt) > 0;
                    case "lessThan"           -> compareDates(strVal, fVal, fmt) < 0;
                    case "greaterThanOrEqual" -> compareDates(strVal, fVal, fmt) >= 0;
                    case "lessThanOrEqual"    -> compareDates(strVal, fVal, fmt) <= 0;
                    case "equals"             -> compareDates(strVal, fVal, fmt) == 0;
                    case "notEquals"          -> compareDates(strVal, fVal, fmt) != 0;
                    default                   -> matchStringOp(op, strVal, fVal);
                };
            }
        }

        return matchStringOp(op, strVal, fVal);
    }

    private boolean matchStringOp(String op, String strVal, String fVal) {
        return switch (op) {
            case "equals"             -> strVal.equalsIgnoreCase(fVal);
            case "notEquals"          -> !strVal.equalsIgnoreCase(fVal);
            case "startsWith"         -> strVal.toLowerCase().startsWith(fVal.toLowerCase());
            case "endsWith"           -> strVal.toLowerCase().endsWith(fVal.toLowerCase());
            case "contains"           -> strVal.toLowerCase().contains(fVal.toLowerCase());
            case "notContains"        -> !strVal.toLowerCase().contains(fVal.toLowerCase());
            case "greaterThan"        -> compareNum(strVal, fVal) > 0;
            case "lessThan"           -> compareNum(strVal, fVal) < 0;
            case "greaterThanOrEqual" -> compareNum(strVal, fVal) >= 0;
            case "lessThanOrEqual"    -> compareNum(strVal, fVal) <= 0;
            case "regex"              -> { try { yield strVal.matches(fVal); }
                                          catch (Exception e) { yield false; } }
            case "blank"              -> strVal.isBlank();
            case "notBlank"           -> !strVal.isBlank();
            default                   -> true;
        };
    }

    private int compareDates(String a, String b, String oracleFormat) {
        try {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern(toJavaDatePattern(oracleFormat));
            return parseDateTime(a, fmt).compareTo(parseDateTime(b, fmt));
        } catch (Exception e) {
            return a.compareToIgnoreCase(b);
        }
    }

    private LocalDateTime parseDateTime(String s, DateTimeFormatter fmt) {
        try {
            return LocalDateTime.parse(s, fmt);
        } catch (DateTimeParseException e) {
            return LocalDate.parse(s, fmt).atStartOfDay();
        }
    }

    /**
     * Converts Oracle-style date format tokens to Java DateTimeFormatter pattern.
     * Month (mm->MM) is replaced BEFORE minutes (MI/mi->mm) to avoid token collision.
     */
    private String toJavaDatePattern(String oracleFormat) {
        return oracleFormat
                .replace("YYYY", "yyyy")
                .replace("HH24", "HH")
                .replace("HH12", "hh")
                .replace("mm",   "MM")   // Oracle mm = month -> Java MM (before minute replacement)
                .replace("MI",   "mm")   // Oracle MI = minutes -> Java mm
                .replace("mi",   "mm")   // Oracle mi = minutes -> Java mm
                .replace("DD",   "dd")
                .replace("SS",   "ss")
                .replace("YY",   "yy");
    }

    private int compareNum(String a, String b) {
        try { return Double.compare(Double.parseDouble(a), Double.parseDouble(b)); }
        catch (NumberFormatException e) { return a.compareToIgnoreCase(b); }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> buildColumnMetaMap(Map<String, Object> config,
                                                                List<String> dsNames) {
        Map<String, Map<String, Object>> meta = new LinkedHashMap<>();
        if (config == null) return meta;
        List<Map<String, Object>> dsList = config.get("datasets") instanceof List<?>
                ? (List<Map<String, Object>>) config.get("datasets") : List.of();
        for (Map<String, Object> ds : dsList) {
            if (!dsNames.contains(ds.get("name"))) continue;
            List<Map<String, Object>> cols = ds.get("columns") instanceof List<?>
                    ? (List<Map<String, Object>>) ds.get("columns") : List.of();
            for (Map<String, Object> col : cols) {
                if (!"date".equals(col.get("type"))) continue;
                String colName = col.get("name") instanceof String s ? s : null;
                if (colName == null) continue;
                Map<String, Object> colMeta = new LinkedHashMap<>();
                colMeta.put("type", "date");
                if (col.get("format") instanceof String fmt) colMeta.put("format", fmt);
                meta.put(colName, colMeta);
            }
        }
        return meta;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getColumnDefs(Map<String, Object> config, String dsName) {
        if (config == null) return List.of();
        List<Map<String, Object>> dsList = config.get("datasets") instanceof List<?>
                ? (List<Map<String, Object>>) config.get("datasets") : List.of();
        return dsList.stream()
                .filter(d -> dsName.equals(d.get("name")))
                .findFirst()
                .map(d -> d.get("columns") instanceof List<?> c
                        ? (List<Map<String, Object>>) c : List.<Map<String, Object>>of())
                .orElse(List.of());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadConfig(String jdName) {
        String path = "joindataset/" + jdName + ".json";
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) return null;
            return objectMapper.readValue(is, Map.class);
        } catch (Exception e) {
            return null;
        }
    }

    private void saveConfig(String jdName, Map<String, Object> config) throws Exception {
        String resourcePath = "joindataset/" + jdName + ".json";
        URL url = getClass().getClassLoader().getResource(resourcePath);
        Path target;
        if (url != null && "file".equals(url.getProtocol())) {
            target = Path.of(url.toURI());
        } else {
            // New file – find the joindataset directory via any existing resource
            URL dirUrl = getClass().getClassLoader().getResource("joindataset/");
            if (dirUrl != null && "file".equals(dirUrl.getProtocol())) {
                target = Path.of(dirUrl.toURI()).resolve(jdName + ".json");
            } else {
                Path fallback = Path.of("src/main/resources/joindataset");
                Files.createDirectories(fallback);
                target = fallback.resolve(jdName + ".json");
            }
        }
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), config);
    }

    private List<Map<String, Object>> remapColumns(List<Map<String, Object>> rows,
                                                   List<Map<String, Object>> columnDefs) {
        if (columnDefs == null || columnDefs.isEmpty()) return rows;
        Map<String, String> attrToName = new LinkedHashMap<>();
        for (Map<String, Object> c : columnDefs) {
            Object attr = c.get("attribute"), name = c.get("name");
            if (attr instanceof String a && name instanceof String n) attrToName.put(a, n);
        }
        if (attrToName.isEmpty()) return rows;
        return rows.stream().map(row -> {
            Map<String, Object> out = new LinkedHashMap<>();
            row.forEach((k, v) -> out.put(attrToName.getOrDefault(k, k), v));
            return out;
        }).collect(Collectors.toList());
    }

    private List<Map<String, Object>> applyColumnTypes(List<Map<String, Object>> rows,
                                                       List<Map<String, Object>> columnDefs) {
        if (columnDefs == null || columnDefs.isEmpty()) return rows;
        Map<String, String> nameToType = new LinkedHashMap<>();
        for (Map<String, Object> c : columnDefs) {
            if (c.get("name") instanceof String n && c.get("type") instanceof String t)
                nameToType.put(n, t);
        }
        if (nameToType.isEmpty()) return rows;
        return rows.stream().map(row -> {
            Map<String, Object> out = new LinkedHashMap<>();
            row.forEach((k, v) -> {
                String type = nameToType.getOrDefault(k, "string");
                if ("string".equals(type) && v != null && !(v instanceof String)) {
                    out.put(k, v.toString());
                } else {
                    out.put(k, v);
                }
            });
            return out;
        }).collect(Collectors.toList());
    }

    private void storeInCacheFactory(String dsName, List<Map<String, Object>> rows, String loadedTime) {
        try {
            String json = objectMapper.writeValueAsString(rows);
            cacheFactory.save("DATASET." + dsName.toUpperCase(), "rows", json, loadedTime);
        } catch (Exception ignored) {}
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractRowList(Object processed) {
        if (processed instanceof List<?> list) {
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) out.add((Map<String, Object>) m);
            }
            return out;
        }
        if (processed instanceof Map<?, ?> m) {
            Object data = m.get("data");
            if (data instanceof List<?> list) {
                List<Map<String, Object>> out = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof Map<?, ?> row) out.add((Map<String, Object>) row);
                }
                return out;
            }
        }
        return new ArrayList<>();
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("activity", "validation");
        err.put("message",  message);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("Errors", List.of(err));
        return ResponseEntity.badRequest().body(body);
    }
}
