package com.mycompany.batch.web;

import com.mycompany.batch.apm.ApmForwardingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Runtime register/unregister for the APM server every transaction gets forwarded to (actual
 * forwarding happens in {@link com.mycompany.batch.apm.ApmTransactionFilter} +
 * {@link ApmForwardingService}), plus read access to the forwarded-request ring buffer for the
 * {@code apm.html} page.
 */
@RestController
@RequestMapping("/apm")
public class ApmController {

    private final ApmForwardingService forwardingService;

    public ApmController(ApmForwardingService forwardingService) {
        this.forwardingService = forwardingService;
    }

    @PostMapping("/register")
    public ResponseEntity<Map<String, Object>> register(@RequestBody Map<String, Object> req) {
        String serverUrl = str(req, "serverUrl");
        if (serverUrl == null || serverUrl.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "serverUrl is required", "ok", false));
        }
        Map<String, Object> status = forwardingService.register(
                serverUrl, str(req, "secretToken"), str(req, "apiKey"),
                str(req, "serviceName"), str(req, "serviceVersion"), str(req, "environment"));
        return ResponseEntity.ok(status);
    }

    @PostMapping("/unregister")
    public ResponseEntity<Map<String, Object>> unregister() {
        return ResponseEntity.ok(forwardingService.unregister());
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(forwardingService.status());
    }

    @GetMapping("/requests")
    public ResponseEntity<Map<String, Object>> requests(
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String method,
            @RequestParam(required = false) Integer status,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Long sinceMs,
            @RequestParam(defaultValue = "200") int limit) {
        List<Map<String, Object>> results = forwardingService.listRequests(type, method, status, q, sinceMs, limit);
        return ResponseEntity.ok(Map.of("requests", results, "count", results.size()));
    }

    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v instanceof String s ? s.trim() : null;
    }
}
