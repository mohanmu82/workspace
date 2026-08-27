package com.mycompany.batch.web;

import com.mycompany.batch.truststore.SavedTrustStore;
import com.mycompany.batch.truststore.SavedTrustStoreService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Manages saved trust store name+path presets for the HTTPS Trust Store Checker page — see
 * {@link SavedTrustStore}.
 */
@RestController
@RequestMapping("/truststorepresets")
public class SavedTrustStoreController {

    private final SavedTrustStoreService service;

    public SavedTrustStoreController(SavedTrustStoreService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<SavedTrustStore>> list() {
        return ResponseEntity.ok(service.list());
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> save(@RequestBody SavedTrustStore preset) {
        try {
            return ResponseEntity.ok(service.save(preset));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to save trust store: " + e.getMessage()));
        }
    }

    @PostMapping("/{name}/default")
    public ResponseEntity<?> setDefault(@PathVariable String name) {
        try {
            return ResponseEntity.ok(service.setDefault(name));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to set default: " + e.getMessage()));
        }
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> delete(@PathVariable String name) {
        try {
            service.delete(name);
            return ResponseEntity.ok(Map.of("status", "deleted", "name", name));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
