package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/grafana")
public class GrafanaController {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final ObjectMapper objectMapper;

    public GrafanaController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostMapping("/health")
    public ResponseEntity<Map<String, Object>> health(@RequestBody Map<String, Object> req) {
        return singleCall(req, "/api/health");
    }

    @PostMapping("/org")
    public ResponseEntity<Map<String, Object>> org(@RequestBody Map<String, Object> req) {
        return singleCall(req, "/api/org");
    }

    @PostMapping("/dashboards")
    public ResponseEntity<Map<String, Object>> dashboards(@RequestBody Map<String, Object> req) {
        return singleCall(req, "/api/search?limit=5000&type=dash-db");
    }

    @PostMapping("/datasources")
    public ResponseEntity<Map<String, Object>> datasources(@RequestBody Map<String, Object> req) {
        return singleCall(req, "/api/datasources");
    }

    @PostMapping("/folders")
    public ResponseEntity<Map<String, Object>> folders(@RequestBody Map<String, Object> req) {
        return singleCall(req, "/api/folders?limit=1000");
    }

    @PostMapping("/inspect")
    public ResponseEntity<Map<String, Object>> inspect(@RequestBody Map<String, Object> req) {
        String baseUrl = str(req, "url");
        if (baseUrl == null || baseUrl.isBlank()) return err("url is required");
        baseUrl = baseUrl.replaceAll("/+$", "");
        String auth = buildAuth(str(req, "username"), str(req, "password"), str(req, "token"));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("health",      callSection(baseUrl + "/api/health", auth));
        result.put("org",         callSection(baseUrl + "/api/org", auth));
        result.put("dashboards",  callSection(baseUrl + "/api/search?limit=5000&type=dash-db", auth));
        result.put("datasources", callSection(baseUrl + "/api/datasources", auth));
        result.put("folders",     callSection(baseUrl + "/api/folders?limit=1000", auth));
        return ResponseEntity.ok(result);
    }

    private Map<String, Object> callSection(String url, String auth) {
        try {
            Object data = call(url, auth);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", true);
            m.put("data", data);
            return m;
        } catch (Exception e) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", false);
            m.put("error", e.getMessage());
            return m;
        }
    }

    private ResponseEntity<Map<String, Object>> singleCall(Map<String, Object> req, String path) {
        String baseUrl = str(req, "url");
        if (baseUrl == null || baseUrl.isBlank()) return err("url is required");
        baseUrl = baseUrl.replaceAll("/+$", "");
        String auth = buildAuth(str(req, "username"), str(req, "password"), str(req, "token"));
        Map<String, Object> result = callSection(baseUrl + path, auth);
        return ResponseEntity.ok(result);
    }

    private Object call(String url, String authHeader) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json");
        if (authHeader != null) b.header("Authorization", authHeader);

        HttpResponse<String> resp = HTTP.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new RuntimeException("HTTP " + resp.statusCode() + " — " +
                    resp.body().replaceAll("[\r\n]+", " ").substring(0, Math.min(resp.body().length(), 200)));
        }
        try {
            return objectMapper.readValue(resp.body(), Object.class);
        } catch (Exception e) {
            return resp.body();
        }
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
