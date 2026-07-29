package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes health checks across the three ways services in this environment expose status:
 * plain HTTP (200 = up), Prometheus text-exposition (Actuator/micrometer), and Jolokia
 * (JMX-over-HTTP, richer JVM detail like heap/pid). The frontend supplies url+type per target —
 * nothing is stored server-side — this just makes the call so the browser doesn't have to
 * (avoids CORS when the target port differs from the page's own origin).
 */
@RestController
@RequestMapping("/servicemonitor")
public class ServiceMonitorController {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final Pattern METRIC_LINE =
            Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{(.*)\\})?\\s+(\\S+)(?:\\s+\\S+)?\\s*$");
    private static final Pattern LABEL =
            Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:[^\"\\\\]|\\\\.)*)\"");

    private final ObjectMapper objectMapper;

    public ServiceMonitorController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostMapping("/check")
    public ResponseEntity<Map<String, Object>> check(@RequestBody Map<String, Object> req) {
        String url  = str(req, "url");
        String type = str(req, "type");
        if (url == null || url.isBlank())   return err("url is required");
        if (type == null || type.isBlank()) return err("type is required");

        Map<String, Object> result = switch (type) {
            case "http"       -> checkHttp(url);
            case "prometheus" -> checkPrometheus(url);
            case "jolokia"    -> checkJolokia(url);
            default -> null;
        };
        if (result == null) return err("Unknown type: " + type + " (expected http, prometheus, or jolokia)");
        return ResponseEntity.ok(result);
    }

    /**
     * Batch variant for dataset loads: dozens/hundreds of targets arrive at once, each of which
     * is a blocking HTTP call. Runs every check on its own virtual thread so the whole batch
     * completes in roughly the time of the single slowest target instead of the sum of all of them.
     */
    @PostMapping("/checkBatch")
    public ResponseEntity<Map<String, Object>> checkBatch(@RequestBody Map<String, Object> req) {
        Object rawTargets = req.get("targets");
        if (!(rawTargets instanceof List<?> rawList) || rawList.isEmpty()) {
            return err("targets is required and must be a non-empty array");
        }

        record Target(String id, String url, String type) {}
        List<Target> targets = new ArrayList<>();
        for (Object o : rawList) {
            if (!(o instanceof Map<?, ?> raw)) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> tm = (Map<String, Object>) raw;
            String id   = str(tm, "id");
            String url  = str(tm, "url");
            String type = str(tm, "type");
            if (url == null || url.isBlank() || type == null || type.isBlank()) continue;
            targets.add(new Target(id != null ? id : url, url, type));
        }
        if (targets.isEmpty()) return err("No valid targets supplied");

        List<Callable<Map<String, Object>>> tasks = new ArrayList<>(targets.size());
        for (Target t : targets) {
            tasks.add(() -> {
                Map<String, Object> r = switch (t.type()) {
                    case "http"       -> checkHttp(t.url());
                    case "prometheus" -> checkPrometheus(t.url());
                    case "jolokia"    -> checkJolokia(t.url());
                    default -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("ok", false);
                        m.put("status", "DOWN");
                        m.put("error", "Unknown type: " + t.type());
                        yield m;
                    }
                };
                Map<String, Object> withId = new LinkedHashMap<>();
                withId.put("id", t.id());
                withId.putAll(r);
                return withId;
            });
        }

        List<Map<String, Object>> results = new ArrayList<>(tasks.size());
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Map<String, Object>>> futures = pool.invokeAll(tasks);
            for (int i = 0; i < futures.size(); i++) {
                try {
                    results.add(futures.get(i).get());
                } catch (Exception e) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", targets.get(i).id());
                    m.put("ok", false);
                    m.put("status", "DOWN");
                    m.put("error", e.getMessage());
                    results.add(m);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return err("Interrupted while checking targets");
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("results", results);
        return ResponseEntity.ok(body);
    }

    // ── HTTP (status 200 = up) ──────────────────────────────────────────────
    private Map<String, Object> checkHttp(String url) {
        long start = System.currentTimeMillis();
        Map<String, Object> m = new LinkedHashMap<>();
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<Void> resp = HTTP.send(request, HttpResponse.BodyHandlers.discarding());
            long latency = System.currentTimeMillis() - start;
            m.put("ok", true);
            m.put("status", resp.statusCode() == 200 ? "UP" : "DOWN");
            m.put("statusCode", resp.statusCode());
            m.put("latencyMs", latency);
            m.put("metrics", Map.of());
        } catch (Exception e) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", e.getMessage());
            m.put("latencyMs", System.currentTimeMillis() - start);
        }
        return m;
    }

    // ── Prometheus text-exposition ──────────────────────────────────────────
    private Map<String, Object> checkPrometheus(String url) {
        long start = System.currentTimeMillis();
        Map<String, Object> m = new LinkedHashMap<>();
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .header("Accept", "text/plain,application/openmetrics-text;q=0.9,*/*;q=0.8")
                    .GET()
                    .build();
            HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            long latency = System.currentTimeMillis() - start;
            boolean up = resp.statusCode() < 400;
            m.put("ok", true);
            m.put("status", up ? "UP" : "DOWN");
            m.put("statusCode", resp.statusCode());
            m.put("latencyMs", latency);
            m.put("metrics", up ? parsePrometheusMetrics(resp.body()) : Map.of());
        } catch (Exception e) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", e.getMessage());
            m.put("latencyMs", System.currentTimeMillis() - start);
        }
        return m;
    }

    private Map<String, Object> parsePrometheusMetrics(String text) {
        Double uptime = null, cpu = null, threads = null;
        double heapUsed = 0, heapMax = 0;
        boolean foundUsed = false, maxUnbounded = false, foundMax = false;

        for (String line : text.split("\r?\n")) {
            if (line.isBlank() || line.startsWith("#")) continue;
            Matcher lm = METRIC_LINE.matcher(line);
            if (!lm.matches()) continue;
            String name = lm.group(1);
            String labelsRaw = lm.group(3);
            double value = parseValue(lm.group(4));

            switch (name) {
                case "process_uptime_seconds" -> uptime = value;
                case "process_cpu_usage" -> cpu = value;
                case "jvm_threads_live_threads" -> threads = value;
                case "jvm_memory_used_bytes" -> {
                    if (isHeapArea(labelsRaw)) { heapUsed += value; foundUsed = true; }
                }
                case "jvm_memory_max_bytes" -> {
                    if (isHeapArea(labelsRaw)) {
                        foundMax = true;
                        if (value < 0) maxUnbounded = true; else heapMax += value;
                    }
                }
                default -> { }
            }
        }

        Map<String, Object> metrics = new LinkedHashMap<>();
        if (uptime  != null) metrics.put("uptimeSeconds", uptime);
        if (cpu     != null) metrics.put("cpuUsage", cpu);
        if (threads != null) metrics.put("threadsLive", threads.intValue());
        if (foundUsed) metrics.put("memoryUsedBytes", (long) heapUsed);
        if (foundMax && !maxUnbounded) metrics.put("memoryMaxBytes", (long) heapMax);
        return metrics;
    }

    private boolean isHeapArea(String labelsRaw) {
        if (labelsRaw == null) return false;
        Matcher lm = LABEL.matcher(labelsRaw);
        while (lm.find()) {
            if (lm.group(1).equals("area")) return "heap".equals(lm.group(2));
        }
        return false;
    }

    private double parseValue(String raw) {
        return switch (raw) {
            case "+Inf" -> Double.POSITIVE_INFINITY;
            case "-Inf" -> Double.NEGATIVE_INFINITY;
            case "NaN"  -> Double.NaN;
            default -> {
                try { yield Double.parseDouble(raw); } catch (Exception e) { yield Double.NaN; }
            }
        };
    }

    // ── Jolokia (JMX-over-HTTP) ─────────────────────────────────────────────
    private Map<String, Object> checkJolokia(String baseUrl) {
        long start = System.currentTimeMillis();
        String base = baseUrl.replaceAll("/+$", "");
        Map<String, Object> m = new LinkedHashMap<>();
        try {
            String body = objectMapper.writeValueAsString(List.of(
                    Map.of("type", "read", "mbean", "java.lang:type=Memory", "attribute", "HeapMemoryUsage"),
                    Map.of("type", "read", "mbean", "java.lang:type=Runtime", "attribute", List.of("Uptime", "Name")),
                    Map.of("type", "read", "mbean", "java.lang:type=Threading", "attribute", "ThreadCount")
            ));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(base))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            long latency = System.currentTimeMillis() - start;

            if (resp.statusCode() >= 400) {
                m.put("ok", true);
                m.put("status", "DOWN");
                m.put("statusCode", resp.statusCode());
                m.put("latencyMs", latency);
                m.put("metrics", Map.of());
                return m;
            }

            Map<String, Object> metrics = new LinkedHashMap<>();
            boolean anyOk = false;
            for (JsonNode entry : objectMapper.readTree(resp.body())) {
                if (entry.path("status").asInt(0) != 200) continue;
                anyOk = true;
                String mbean = entry.path("request").path("mbean").asText("");
                JsonNode value = entry.path("value");
                if (mbean.contains("type=Memory")) {
                    if (value.has("used")) metrics.put("memoryUsedBytes", value.path("used").asLong());
                    if (value.has("max") && value.path("max").asLong() >= 0) {
                        metrics.put("memoryMaxBytes", value.path("max").asLong());
                    }
                } else if (mbean.contains("type=Runtime")) {
                    if (value.has("Uptime")) metrics.put("uptimeSeconds", value.path("Uptime").asLong() / 1000.0);
                    String name = value.path("Name").asText(null);
                    if (name != null && name.contains("@")) {
                        String pidPart = name.substring(0, name.indexOf('@'));
                        if (!pidPart.isEmpty() && pidPart.chars().allMatch(Character::isDigit)) {
                            metrics.put("pid", Long.parseLong(pidPart));
                        }
                    }
                } else if (mbean.contains("type=Threading") && value.isNumber()) {
                    metrics.put("threadsLive", value.asInt());
                }
            }

            m.put("ok", true);
            m.put("status", anyOk ? "UP" : "DOWN");
            m.put("statusCode", resp.statusCode());
            m.put("latencyMs", latency);
            m.put("metrics", metrics);
        } catch (Exception e) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", e.getMessage());
            m.put("latencyMs", System.currentTimeMillis() - start);
        }
        return m;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────
    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v instanceof String s ? s.trim() : null;
    }

    private ResponseEntity<Map<String, Object>> err(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg, "ok", false));
    }
}
