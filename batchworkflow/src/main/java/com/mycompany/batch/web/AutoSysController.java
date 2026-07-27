package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic proxy for an AutoSys (or similar) jobs REST API. The frontend supplies the URL and
 * optional credentials at runtime — nothing is stored server-side — this endpoint just makes
 * the call so the browser doesn't have to (avoids CORS and keeps internal-network URLs off the
 * client's direct fetch path).
 */
@RestController
@RequestMapping("/autosys")
public class AutoSysController {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final ObjectMapper objectMapper;

    public AutoSysController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostMapping("/fetch")
    public ResponseEntity<Map<String, Object>> fetch(@RequestBody Map<String, Object> req) {
        String url = str(req, "url");
        if (url == null || url.isBlank()) return err("url is required");
        String auth = buildAuth(str(req, "username"), str(req, "password"), str(req, "token"));

        long start = System.currentTimeMillis();
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/json");
            if (auth != null) b.header("Authorization", auth);

            HttpResponse<String> resp = HTTP.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
            long latency = System.currentTimeMillis() - start;

            if (resp.statusCode() >= 400) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ok", false);
                m.put("statusCode", resp.statusCode());
                m.put("error", "HTTP " + resp.statusCode() + " — " + truncate(resp.body()));
                m.put("latencyMs", latency);
                return ResponseEntity.ok(m);
            }

            Object data;
            try {
                data = objectMapper.readValue(resp.body(), Object.class);
            } catch (Exception e) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("ok", false);
                m.put("statusCode", resp.statusCode());
                m.put("error", "Response was not valid JSON: " + e.getMessage());
                m.put("rawPreview", truncate(resp.body()));
                m.put("latencyMs", latency);
                return ResponseEntity.ok(m);
            }

            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", true);
            m.put("statusCode", resp.statusCode());
            m.put("data", data);
            m.put("latencyMs", latency);
            return ResponseEntity.ok(m);
        } catch (Exception e) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", false);
            m.put("error", e.getMessage());
            m.put("latencyMs", System.currentTimeMillis() - start);
            return ResponseEntity.ok(m);
        }
    }

    private String truncate(String s) {
        if (s == null) return null;
        String flat = s.replaceAll("[\\r\\n]+", " ");
        return flat.length() > 400 ? flat.substring(0, 400) + "…" : flat;
    }

    private String buildAuth(String username, String password, String token) {
        if (token != null && !token.isBlank()) return "Bearer " + token.trim();
        if (username != null && !username.isBlank()) {
            String cred = username + ":" + (password != null ? password : "");
            return "Basic " + Base64.getEncoder().encodeToString(cred.getBytes());
        }
        return null;
    }

    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v instanceof String s ? s.trim() : null;
    }

    private ResponseEntity<Map<String, Object>> err(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg, "ok", false));
    }
}
