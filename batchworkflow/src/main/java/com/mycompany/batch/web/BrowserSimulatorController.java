package com.mycompany.batch.web;

import com.mycompany.batch.service.BrowserSimulatorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST control plane for {@link BrowserSimulatorService}. Each "session" is a dedicated headless
 * browser process this server launched — these endpoints start/stop sessions, poll their status,
 * trigger a fresh memory-metrics capture on an already-loaded page, and tail the network calls
 * and JavaScript errors the page has produced.
 */
@RestController
@RequestMapping("/browsersimulator")
public class BrowserSimulatorController {

    private final BrowserSimulatorService service;

    public BrowserSimulatorController(BrowserSimulatorService service) {
        this.service = service;
    }

    @PostMapping("/start")
    public ResponseEntity<?> start(@RequestBody Map<String, Object> body) {
        String url = String.valueOf(body.getOrDefault("url", "")).trim();
        if (url.isEmpty()) return error("url is required");
        if (!url.matches("(?i)^https?://.+")) url = "https://" + url;
        Integer settleMs = body.get("settleMs") instanceof Number n ? n.intValue() : null;
        try {
            String id = service.start(url, settleMs);
            return ResponseEntity.ok(Map.of("id", id));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/status")
    public List<Map<String, Object>> status() {
        return service.status();
    }

    @GetMapping("/status/{id}")
    public ResponseEntity<?> statusOne(@PathVariable String id) {
        try {
            return ResponseEntity.ok(service.describeOne(id));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/refresh/{id}")
    public ResponseEntity<?> refresh(@PathVariable String id) {
        try {
            return ResponseEntity.ok(service.refresh(id));
        } catch (IllegalStateException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/log/{id}")
    public ResponseEntity<?> log(@PathVariable String id) {
        try {
            return ResponseEntity.ok(service.logOf(id));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/clearlog/{id}")
    public ResponseEntity<?> clearLog(@PathVariable String id) {
        try {
            return ResponseEntity.ok(service.clearLog(id));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/stop/{id}")
    public ResponseEntity<?> stop(@PathVariable String id) {
        boolean stopped = service.stop(id);
        return ResponseEntity.ok(Map.of("stopped", stopped));
    }

    private ResponseEntity<?> error(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message != null ? message : "Bad request"));
    }
}
