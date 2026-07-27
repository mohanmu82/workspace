package com.mycompany.batch.web;

import com.mycompany.batch.service.ContainerReadinessService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/containerreadiness")
public class ContainerReadinessController {

    private final ContainerReadinessService service;

    public ContainerReadinessController(ContainerReadinessService service) {
        this.service = service;
    }

    // -------------------------------------------------------------------------
    // POST /containerreadiness/check — clone a GitHub repo (shallow, discarded
    // afterward) and run container-readiness checks against its Maven modules.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/check", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> check(@RequestBody Map<String, Object> body) {
        String repoUrl = str(body.get("repoUrl"));
        String ref = str(body.get("ref"));
        int timeoutSeconds = intVal(body.get("timeoutSeconds"), 120);
        timeoutSeconds = Math.max(10, Math.min(timeoutSeconds, 300));

        long start = System.currentTimeMillis();
        try {
            Map<String, Object> result = service.analyzeRepo(repoUrl, ref.isBlank() ? null : ref, timeoutSeconds);
            result.put("durationMs", System.currentTimeMillis() - start);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(error(e.getMessage()));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error("I/O error: " + e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error("Unexpected error: " + e.getMessage()));
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
}
