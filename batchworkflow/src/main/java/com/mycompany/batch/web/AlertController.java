package com.mycompany.batch.web;

import com.mycompany.batch.alert.AlertConfig;
import com.mycompany.batch.alert.AlertService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Configures and fires the Service Dashboard's down/recovery webhook — see {@link AlertService}.
 */
@RestController
@RequestMapping("/servicemonitor")
public class AlertController {

    private final AlertService alertService;

    public AlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    @GetMapping("/alertconfig")
    public ResponseEntity<AlertConfig> getConfig() {
        return ResponseEntity.ok(alertService.getConfig());
    }

    @PutMapping(value = "/alertconfig", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> saveConfig(@RequestBody AlertConfig config) {
        try {
            return ResponseEntity.ok(alertService.saveConfig(config));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to save alert config: " + e.getMessage()));
        }
    }

    @PostMapping(value = "/alertconfig/test", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> test(@RequestBody AlertConfig config) {
        try {
            Map<String, Object> event = Map.of(
                    "name", "Test Alert",
                    "url", "(manual test)",
                    "status", "DOWN",
                    "previousStatus", "UP",
                    "error", "This is a test notification from the Service Dashboard.");
            return ResponseEntity.ok(alertService.sendTest(config, event));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping(value = "/alert", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> alert(@RequestBody Map<String, Object> event) {
        Object keyObj = event.get("key");
        String key = keyObj != null ? String.valueOf(keyObj) : String.valueOf(event.get("url"));
        if (key == null || key.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "key or url is required"));
        }
        return ResponseEntity.ok(alertService.notify(key, event));
    }
}
