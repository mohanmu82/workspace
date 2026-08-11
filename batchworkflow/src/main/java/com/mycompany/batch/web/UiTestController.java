package com.mycompany.batch.web;

import com.mycompany.batch.service.UiTestService;
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
@RequestMapping("/uitest")
public class UiTestController {

    private final UiTestService service;

    public UiTestController(UiTestService service) {
        this.service = service;
    }

    // -------------------------------------------------------------------------
    // POST /uitest/run — launch a browser via Playwright, navigate to a URL, run a scripted
    // sequence of steps (click/fill/assert/screenshot/...), and report per-step results.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/run", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> run(@RequestBody Map<String, Object> body) {
        Object urlObj = body.get("url");
        if (urlObj == null || urlObj.toString().isBlank()) {
            return ResponseEntity.badRequest().body(error("url is required"));
        }
        try {
            return ResponseEntity.ok(service.run(body));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(error(e.getMessage()));
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
}
