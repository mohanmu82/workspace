package com.mycompany.batch.web;

import com.mycompany.batch.pageproperty.PageProperty;
import com.mycompany.batch.pageproperty.PagePropertyService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Manages admin-editable per-page settings (see {@link PageProperty}) — e.g. websocketproxy.html's
 * default listen port. Lets a page read its own settings on load and lets the Admin page list/edit/
 * remove them directly.
 */
@RestController
@RequestMapping("/pageproperty")
public class PagePropertyController {

    private final PagePropertyService service;

    public PagePropertyController(PagePropertyService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<PageProperty>> list() {
        return ResponseEntity.ok(service.list());
    }

    @GetMapping("/{page}")
    public ResponseEntity<Map<String, String>> get(@PathVariable String page) {
        return ResponseEntity.ok(service.getProperties(page));
    }

    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> save(@RequestBody Map<String, String> body) {
        try {
            String page = body.get("page");
            String key = body.get("key");
            String value = body.get("value");
            return ResponseEntity.ok(service.setProperty(page, key, value));
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed to save property: " + e.getMessage());
        }
    }

    @DeleteMapping("/{page}/{key}")
    public ResponseEntity<?> delete(@PathVariable String page, @PathVariable String key) {
        try {
            service.deleteProperty(page, key);
            return ResponseEntity.ok(Map.of("status", "deleted", "page", page, "key", key));
        } catch (Exception e) {
            return badRequest(e.getMessage());
        }
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
