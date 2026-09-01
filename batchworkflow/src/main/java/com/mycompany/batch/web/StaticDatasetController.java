package com.mycompany.batch.web;

import com.mycompany.batch.staticdataset.DatasetQuery;
import com.mycompany.batch.staticdataset.FilterFavorite;
import com.mycompany.batch.staticdataset.StaticDatasetDef;
import com.mycompany.batch.staticdataset.StaticDatasetService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages static dataset definitions — named row sets loaded once from a file or HTTP
 * endpoint and reused as input elsewhere (e.g. the Service Dashboard populates its
 * target list from a dataset's rows instead of manual entry).
 */
@RestController
@RequestMapping("/staticdataset")
public class StaticDatasetController {

    private final StaticDatasetService service;

    public StaticDatasetController(StaticDatasetService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> list() {
        List<Map<String, Object>> out = service.list().stream()
                .map(this::summarize)
                .collect(Collectors.toList());
        return ResponseEntity.ok(out);
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> get(@PathVariable String name) {
        StaticDatasetDef def = service.get(name);
        if (def == null) return notFound(name);
        return ResponseEntity.ok(summarize(def));
    }

    @GetMapping("/{name}/data")
    public ResponseEntity<?> data(@PathVariable String name) {
        StaticDatasetDef def = service.get(name);
        if (def == null) return notFound(name);
        StaticDatasetService.DatasetState s = service.getState(name);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name", name);
        response.put("attributes", s != null ? s.attributes() : def.getAttributes());
        response.put("rows", s != null ? s.rows() : List.of());
        response.put("count", s != null ? s.rows().size() : 0);
        response.put("loadedTime", s != null ? s.loadedTime() : null);
        response.put("error", s != null ? s.error() : null);
        response.put("favorites", def.getFavorites());
        return ResponseEntity.ok(response);
    }

    /**
     * The rows of {@code name} that match a filter — a saved favourite by name, ad-hoc conditions,
     * or both AND-ed — evaluated here rather than in the caller.
     *
     * <p>This is the server-side half of the dataset widget's filter: the browser sends the
     * condition and gets back only what matched, instead of pulling every row down to narrow it
     * itself. {@code countOnly} answers "how many match" without the rows, which is what keeps the
     * live match count cheap while someone is still typing a condition.
     */
    @PostMapping(value = "/{name}/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> query(@PathVariable String name, @RequestBody(required = false) DatasetQuery query) {
        if (service.get(name) == null) return notFound(name);
        DatasetQuery q = query != null ? query : new DatasetQuery();

        StaticDatasetService.FilterResult result;
        try {
            result = service.filter(name, q.getFavorite(), q.getConditions());
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name", name);
        response.put("attributes", result.attributes());
        response.put("count", result.rows().size());
        response.put("total", result.total());
        if (!q.isCountOnly()) response.put("rows", result.rows());
        response.put("loadedTime", result.loadedTime());
        response.put("error", result.error());
        return ResponseEntity.ok(response);
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> create(@RequestBody StaticDatasetDef def) {
        if (def.getName() != null && service.get(def.getName()) != null)
            return badRequest("Dataset '" + def.getName() + "' already exists");
        return saveAndLoad(def);
    }

    @PutMapping(value = "/{name}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> update(@PathVariable String name, @RequestBody StaticDatasetDef def) {
        StaticDatasetDef existing = service.get(name);
        if (existing == null) return notFound(name);
        def.setName(name);
        // The edit form only touches name/source/location/arrayElement — favorites are managed
        // through their own endpoints, so preserve whatever is already on disk.
        if (def.getFavorites() == null || def.getFavorites().isEmpty()) {
            def.setFavorites(existing.getFavorites());
        }
        return saveAndLoad(def);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> delete(@PathVariable String name) {
        if (service.get(name) == null) return notFound(name);
        try {
            service.delete(name);
        } catch (Exception e) {
            return badRequest("Failed to delete: " + e.getMessage());
        }
        return ResponseEntity.ok(Map.of("status", "deleted", "name", name));
    }

    @PostMapping("/{name}/reload")
    public ResponseEntity<?> reload(@PathVariable String name) {
        if (service.get(name) == null) return notFound(name);
        try {
            StaticDatasetService.DatasetState s = service.reload(name);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("name", name);
            response.put("attributes", s.attributes());
            response.put("count", s.rows().size());
            response.put("loadedTime", s.loadedTime());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return badRequest("Load failed: " + e.getMessage());
        }
    }

    @PostMapping(value = "/{name}/favorites", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> saveFavorite(@PathVariable String name, @RequestBody FilterFavorite favorite) {
        if (service.get(name) == null) return notFound(name);
        try {
            StaticDatasetDef def = service.addOrUpdateFavorite(name, favorite);
            return ResponseEntity.ok(Map.of("status", "saved", "favorites", def.getFavorites()));
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed to save favorite: " + e.getMessage());
        }
    }

    @DeleteMapping("/{name}/favorites/{favoriteName}")
    public ResponseEntity<?> deleteFavorite(@PathVariable String name, @PathVariable String favoriteName) {
        if (service.get(name) == null) return notFound(name);
        try {
            service.deleteFavorite(name, favoriteName);
            return ResponseEntity.ok(Map.of("status", "deleted"));
        } catch (Exception e) {
            return badRequest(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private ResponseEntity<?> saveAndLoad(StaticDatasetDef def) {
        try {
            service.save(def);
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed to save: " + e.getMessage());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        try {
            StaticDatasetService.DatasetState s = service.reload(def.getName());
            response.put("attributes", s.attributes());
            response.put("count", s.rows().size());
            response.put("loadedTime", s.loadedTime());
        } catch (Exception e) {
            response.put("attributes", def.getAttributes());
            response.put("count", 0);
            response.put("error", "Saved, but initial load failed: " + e.getMessage());
        }
        response.put("name", def.getName());
        response.put("status", "saved");
        return ResponseEntity.ok(response);
    }

    private Map<String, Object> summarize(StaticDatasetDef def) {
        StaticDatasetService.DatasetState s = service.getState(def.getName());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", def.getName());
        m.put("source", def.getSource());
        m.put("location", def.getLocation());
        m.put("arrayElement", def.getArrayElement());
        m.put("attributes", s != null ? s.attributes() : def.getAttributes());
        m.put("count", s != null ? s.rows().size() : 0);
        m.put("loadedTime", s != null ? s.loadedTime() : null);
        m.put("error", s != null ? s.error() : null);
        m.put("favorites", def.getFavorites());
        return m;
    }

    private ResponseEntity<Map<String, Object>> notFound(String name) {
        return ResponseEntity.status(404).body(Map.of("error", "Static dataset not found: " + name));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
