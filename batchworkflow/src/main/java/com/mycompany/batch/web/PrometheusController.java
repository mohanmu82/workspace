package com.mycompany.batch.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic proxy for a Prometheus text-exposition endpoint (Spring Boot Actuator's
 * /actuator/prometheus, a Prometheus server's own /metrics, a Pushgateway, etc). The
 * frontend supplies the URL at runtime — nothing is stored server-side — this endpoint
 * just makes the call so the browser doesn't have to (avoids CORS when the metrics port
 * differs from the page's own origin).
 */
@RestController
@RequestMapping("/prometheus")
public class PrometheusController {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    @PostMapping("/fetch")
    public ResponseEntity<Map<String, Object>> fetch(@RequestBody Map<String, Object> req) {
        String url = str(req, "url");
        if (url == null || url.isBlank()) return err("url is required");

        long start = System.currentTimeMillis();
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "text/plain,application/openmetrics-text;q=0.9,*/*;q=0.8")
                    .GET()
                    .build();

            HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            long latency = System.currentTimeMillis() - start;

            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", resp.statusCode() < 400);
            m.put("statusCode", resp.statusCode());
            m.put("latencyMs", latency);
            if (resp.statusCode() >= 400) {
                m.put("error", "HTTP " + resp.statusCode());
                m.put("body", truncate(resp.body(), 2000));
            } else {
                m.put("body", resp.body());
            }
            return ResponseEntity.ok(m);
        } catch (Exception e) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", false);
            m.put("error", e.getMessage());
            m.put("latencyMs", System.currentTimeMillis() - start);
            return ResponseEntity.ok(m);
        }
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() > max ? s.substring(0, max) + "…" : s;
    }

    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v instanceof String s ? s.trim() : null;
    }

    private ResponseEntity<Map<String, Object>> err(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg, "ok", false));
    }
}
