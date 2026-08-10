package com.mycompany.batch.web;

import com.mycompany.batch.pagepreference.PagePreference;
import com.mycompany.batch.pagepreference.PagePreferenceService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Manages saved per-page "load from static dataset" field mappings (see {@link PagePreference}) —
 * lets {@code staticdataset-widget.js} remember which dataset attribute feeds which field on a
 * given page, and lets the Admin page list/edit/remove those mappings directly.
 */
@RestController
@RequestMapping("/pagepreference")
public class PagePreferenceController {

    private final PagePreferenceService service;

    public PagePreferenceController(PagePreferenceService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<PagePreference>> list() {
        return ResponseEntity.ok(service.list());
    }

    @GetMapping("/{page}/{datasetName}")
    public ResponseEntity<?> get(@PathVariable String page, @PathVariable String datasetName) {
        PagePreference pref = service.get(page, datasetName);
        if (pref == null) return notFound(page, datasetName);
        return ResponseEntity.ok(pref);
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> save(@RequestBody PagePreference pref) {
        try {
            return ResponseEntity.ok(service.save(pref));
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed to save preference: " + e.getMessage());
        }
    }

    @DeleteMapping("/{page}/{datasetName}")
    public ResponseEntity<?> delete(@PathVariable String page, @PathVariable String datasetName) {
        try {
            service.delete(page, datasetName);
            return ResponseEntity.ok(Map.of("status", "deleted", "page", page, "datasetName", datasetName));
        } catch (Exception e) {
            return badRequest(e.getMessage());
        }
    }

    private ResponseEntity<Map<String, Object>> notFound(String page, String datasetName) {
        return ResponseEntity.status(404).body(Map.of("error",
                "No preference for page '" + page + "' / dataset '" + datasetName + "'"));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
