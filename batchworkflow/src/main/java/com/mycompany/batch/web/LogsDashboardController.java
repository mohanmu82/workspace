package com.mycompany.batch.web;

import com.mycompany.batch.service.LogsDashboardService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/logsdashboard")
public class LogsDashboardController {

    private final LogsDashboardService service;

    public LogsDashboardController(LogsDashboardService service) {
        this.service = service;
    }

    // -------------------------------------------------------------------------
    // POST /logsdashboard/scan — walk a top-level unix path, find files modified
    // in the last N minutes, and grep them for CRITICAL/ERROR/exception markers.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/scan", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> scan(@RequestBody Map<String, Object> body) {
        String rootPath = str(body.get("rootPath"));
        int minutesBack = intVal(body.get("minutesBack"), 15);
        minutesBack = Math.max(1, Math.min(minutesBack, 24 * 60));
        String fileNamePattern = str(body.get("fileNamePattern"));
        String searchPattern = str(body.get("searchPattern"));

        if (rootPath.isBlank()) {
            return ResponseEntity.badRequest().body(error("rootPath is required"));
        }

        try {
            return ResponseEntity.ok(service.scan(rootPath, minutesBack, fileNamePattern, searchPattern));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(error("Unexpected error: " + e.getMessage()));
        }
    }

    // -------------------------------------------------------------------------
    // POST /logsdashboard/guess-log-path — best-effort: query a Jolokia endpoint
    // for MBean attributes / system properties that look like a log file/dir path.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/guess-log-path", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> guessLogPath(@RequestBody Map<String, Object> body) {
        String jolokiaUrl = str(body.get("jolokiaUrl"));
        if (jolokiaUrl.isBlank()) {
            return ResponseEntity.badRequest().body(error("jolokiaUrl is required"));
        }
        long timeoutMs = longVal(body.get("timeoutMs"), 10000);

        try {
            return ResponseEntity.ok(service.guessLogPath(jolokiaUrl, timeoutMs));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(error("Unexpected error: " + e.getMessage()));
        }
    }

    private Map<String, Object> error(String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", message);
        return m;
    }

    private String str(Object o) {
        return o == null ? "" : o.toString().trim();
    }

    private int intVal(Object o, int def) {
        if (o == null) return def;
        try { return Integer.parseInt(o.toString().trim()); } catch (Exception e) { return def; }
    }

    private long longVal(Object o, long def) {
        if (o == null) return def;
        try { return Long.parseLong(o.toString().trim()); } catch (Exception e) { return def; }
    }
}
