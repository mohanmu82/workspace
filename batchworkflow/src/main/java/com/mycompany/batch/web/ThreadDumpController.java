package com.mycompany.batch.web;

import com.mycompany.batch.service.ThreadDumpService;
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
@RequestMapping("/threaddump")
public class ThreadDumpController {

    private final ThreadDumpService service;

    public ThreadDumpController(ThreadDumpService service) {
        this.service = service;
    }

    // -------------------------------------------------------------------------
    // POST /threaddump/analyze — read a stdout/log file, locate embedded Java thread
    // dump(s), and analyze them for deadlocks, contention, pool exhaustion and OOM signals.
    // -------------------------------------------------------------------------

    @PostMapping(value = "/analyze", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> analyze(@RequestBody Map<String, Object> body) {
        String filePath = str(body.get("filePath"));
        String text = str(body.get("text"));
        String jolokiaUrl = str(body.get("jolokiaUrl"));
        if (filePath.isBlank() && text.isBlank() && jolokiaUrl.isBlank()) {
            return ResponseEntity.badRequest().body(error("filePath, text, or jolokiaUrl is required"));
        }

        try {
            Map<String, Object> result;
            if (!jolokiaUrl.isBlank()) {
                result = service.analyzeJolokia(jolokiaUrl, str(body.get("username")), str(body.get("password")));
            } else if (!text.isBlank()) {
                result = service.analyzeText(text);
            } else {
                result = service.analyze(filePath);
            }
            return ResponseEntity.ok(result);
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

    private String str(Object o) {
        return o == null ? "" : o.toString().trim();
    }
}
