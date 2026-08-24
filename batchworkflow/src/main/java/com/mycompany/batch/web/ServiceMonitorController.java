package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.model.HttpBatchRequest;
import com.mycompany.batch.model.HttpMethod;
import com.mycompany.batch.service.AgentHttpDispatchService;
import com.mycompany.batch.util.Threads;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes health checks across the three ways services in this environment expose status:
 * plain HTTP (200 = up), Prometheus text-exposition (Actuator/micrometer), and Jolokia
 * (JMX-over-HTTP, richer JVM detail like heap/pid). The frontend supplies url+type (and,
 * optionally, credentials) per target — nothing about the target list itself is stored
 * server-side — this just makes the call so the browser doesn't have to (avoids CORS when the
 * target port differs from the page's own origin).
 *
 * <p>Recent results per target (keyed by type+url, independent of whatever id the browser
 * assigns) are kept in a small in-memory ring buffer so the UI can render a trend sparkline —
 * this is a best-effort cache, not a database: it resets on restart and isn't bounded per-key
 * beyond {@link #HISTORY_CAP}, only the overall key count is unbounded (fine for the hundreds,
 * not millions, of targets this page is meant for).
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

    private static final int HISTORY_CAP = 30;

    /** Per-request timeout is caller-supplied, floored here so a near-zero value can't spin
     *  the checker in a tight retry loop. */
    private static final long DEFAULT_TIMEOUT_MS = 3000;
    private static final long MIN_TIMEOUT_MS = 200;

    private final ObjectMapper objectMapper;
    private final AgentHttpDispatchService agentDispatch;
    private final Map<String, Deque<Map<String, Object>>> history = new ConcurrentHashMap<>();

    public ServiceMonitorController(ObjectMapper objectMapper, AgentHttpDispatchService agentDispatch) {
        this.objectMapper = objectMapper;
        this.agentDispatch = agentDispatch;
    }

    @PostMapping("/check")
    public ResponseEntity<Map<String, Object>> check(@RequestBody Map<String, Object> req) {
        String url  = str(req, "url");
        String type = str(req, "type");
        if (url == null || url.isBlank())   return err("url is required");
        if (type == null || type.isBlank()) return err("type is required");

        String authHeader = buildAuth(str(req, "authType"), str(req, "username"), str(req, "password"), str(req, "token"));
        Map<String, Object> result = runCheck(type, url, authHeader, timeoutMs(req), str(req, "target"), str(req, "agentId"));
        if (result == null) return err("Unknown type: " + type + " (expected http, prometheus, or jolokia)");
        return ResponseEntity.ok(result);
    }

    /**
     * Batch variant for dataset loads: dozens/hundreds of targets arrive at once, each of which
     * is a blocking HTTP call. Runs every check on its own virtual thread so the whole batch
     * completes in roughly the time of the single slowest target instead of the sum of all of them.
     * Returns only once every target has finished — see {@link #checkBatchStream} for the
     * incremental-results variant used by the dashboard's "Check All Now".
     */
    @PostMapping("/checkBatch")
    public ResponseEntity<Map<String, Object>> checkBatch(@RequestBody Map<String, Object> req) {
        List<Target> targets = parseTargets(req.get("targets"));
        if (targets.isEmpty()) return err("targets is required and must be a non-empty array of valid entries");
        long timeoutMs = timeoutMs(req);
        String target  = str(req, "target");
        String agentId = str(req, "agentId");

        List<Callable<Map<String, Object>>> tasks = new ArrayList<>(targets.size());
        for (Target t : targets) tasks.add(() -> buildResult(t, timeoutMs, target, agentId));

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

    /**
     * Streams results as an SSE event per target the moment it completes, instead of waiting
     * for the whole batch — at hundreds of targets, {@link #checkBatch} makes the UI sit idle
     * for the duration of the slowest single check before showing anything. Fires "result"
     * events as each target finishes, periodic "progress" events, and a final "done".
     *
     * <p>A plain {@code EventSource} can't POST a body, so the dashboard drives this with
     * {@code fetch()} + a manual reader over the {@code text/event-stream} body instead — the
     * wire format is the same, just consumed by hand rather than the browser's SSE client.
     */
    @PostMapping(value = "/checkBatchStream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter checkBatchStream(@RequestBody Map<String, Object> req) {
        SseEmitter emitter = new SseEmitter(0L);
        List<Target> targets = parseTargets(req.get("targets"));
        if (targets.isEmpty()) {
            sendEvent(emitter, "error", Map.of("error", "targets is required and must be a non-empty array of valid entries"));
            emitter.complete();
            return emitter;
        }
        long timeoutMs = timeoutMs(req);
        String target  = str(req, "target");
        String agentId = str(req, "agentId");

        AtomicBoolean cancelled = new AtomicBoolean(false);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        Runnable shutdown = () -> { cancelled.set(true); pool.shutdown(); };
        emitter.onCompletion(shutdown);
        emitter.onError(e -> shutdown.run());
        emitter.onTimeout(shutdown);

        Threads.startDaemon(() -> {
            CompletionService<Map<String, Object>> cs = new ExecutorCompletionService<>(pool);
            for (Target t : targets) cs.submit(() -> buildResult(t, timeoutMs, target, agentId));

            int total = targets.size();
            int done  = 0;
            while (done < total && !cancelled.get()) {
                try {
                    Future<Map<String, Object>> f = cs.poll(2, TimeUnit.SECONDS);
                    if (f == null) continue;
                    Map<String, Object> r = f.get();
                    done++;
                    sendEvent(emitter, "result", r);
                    sendEvent(emitter, "progress", Map.of("checked", done, "total", total));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    done++;
                }
            }
            if (!cancelled.get()) {
                sendEvent(emitter, "done", Map.of("total", total));
                emitter.complete();
            }
        });

        return emitter;
    }

    // ── Target parsing shared by both batch variants ────────────────────────
    private record Target(String id, String url, String type, String authHeader) {}

    private List<Target> parseTargets(Object rawTargets) {
        List<Target> targets = new ArrayList<>();
        if (!(rawTargets instanceof List<?> rawList)) return targets;
        for (Object o : rawList) {
            if (!(o instanceof Map<?, ?> raw)) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> tm = (Map<String, Object>) raw;
            String id   = str(tm, "id");
            String url  = str(tm, "url");
            String type = str(tm, "type");
            if (url == null || url.isBlank() || type == null || type.isBlank()) continue;
            String authHeader = buildAuth(str(tm, "authType"), str(tm, "username"), str(tm, "password"), str(tm, "token"));
            targets.add(new Target(id != null ? id : url, url, type, authHeader));
        }
        return targets;
    }

    private Map<String, Object> buildResult(Target t, long timeoutMs, String target, String agentId) {
        Map<String, Object> r = runCheck(t.type(), t.url(), t.authHeader(), timeoutMs, target, agentId);
        if (r == null) {
            r = new LinkedHashMap<>();
            r.put("ok", false);
            r.put("status", "DOWN");
            r.put("error", "Unknown type: " + t.type());
        }
        Map<String, Object> withId = new LinkedHashMap<>();
        withId.put("id", t.id());
        withId.putAll(r);
        return withId;
    }

    /** Floors the caller-supplied timeout at MIN_TIMEOUT_MS — no upper bound. */
    private long timeoutMs(Map<String, Object> req) {
        Object raw = req.get("timeoutMs");
        long ms = raw instanceof Number n ? n.longValue() : DEFAULT_TIMEOUT_MS;
        return Math.max(MIN_TIMEOUT_MS, ms);
    }

    // ── Dispatch + history recording, shared by /check, /checkBatch, /checkBatchStream ──────
    private Map<String, Object> runCheck(String type, String url, String authHeader, long timeoutMs,
                                          String target, String agentId) {
        Map<String, Object> m = switch (type) {
            case "http"       -> checkHttp(url, authHeader, timeoutMs, target, agentId);
            case "prometheus" -> checkPrometheus(url, authHeader, timeoutMs, target, agentId);
            case "jolokia"    -> checkJolokia(url, authHeader, timeoutMs, target, agentId);
            default -> null;
        };
        if (m == null) return null;

        Object latencyRaw = m.get("latencyMs");
        Long latencyMs = latencyRaw instanceof Number n ? n.longValue() : null;
        m.put("history", recordAndGetHistory(type + "|" + url, (String) m.get("status"), latencyMs));
        return m;
    }

    private List<Map<String, Object>> recordAndGetHistory(String key, String status, Long latencyMs) {
        Deque<Map<String, Object>> dq = history.computeIfAbsent(key, k -> new ArrayDeque<>());
        synchronized (dq) {
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("ts", System.currentTimeMillis());
            point.put("status", status);
            point.put("latencyMs", latencyMs);
            dq.addLast(point);
            while (dq.size() > HISTORY_CAP) dq.pollFirst();
            return new ArrayList<>(dq);
        }
    }

    // ── HTTP (status 200 = up) ──────────────────────────────────────────────
    private Map<String, Object> checkHttp(String url, String authHeader, long timeoutMs, String target, String agentId) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (authHeader != null) headers.put("Authorization", authHeader);
        FetchResult fr = doFetch("GET", url, headers, null, timeoutMs, target, agentId);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("executedVia", fr.executedVia());
        if (fr.error() != null) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", fr.error());
            m.put("latencyMs", fr.latencyMs());
            return m;
        }
        boolean up = fr.statusCode() == 200;
        m.put("ok", true);
        m.put("status", up ? "UP" : "DOWN");
        m.put("statusCode", fr.statusCode());
        m.put("latencyMs", fr.latencyMs());
        m.put("metrics", Map.of());
        if (!up) m.put("error", "Unexpected HTTP status: " + fr.statusCode());
        return m;
    }

    // ── Prometheus text-exposition ──────────────────────────────────────────
    private Map<String, Object> checkPrometheus(String url, String authHeader, long timeoutMs, String target, String agentId) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Accept", "text/plain,application/openmetrics-text;q=0.9,*/*;q=0.8");
        if (authHeader != null) headers.put("Authorization", authHeader);
        FetchResult fr = doFetch("GET", url, headers, null, timeoutMs, target, agentId);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("executedVia", fr.executedVia());
        if (fr.error() != null) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", fr.error());
            m.put("latencyMs", fr.latencyMs());
            return m;
        }
        boolean up = fr.statusCode() < 400;
        m.put("ok", true);
        m.put("status", up ? "UP" : "DOWN");
        m.put("statusCode", fr.statusCode());
        m.put("latencyMs", fr.latencyMs());
        m.put("metrics", up ? parsePrometheusMetrics(fr.body()) : Map.of());
        if (!up) m.put("error", "Unexpected HTTP status: " + fr.statusCode());
        return m;
    }

    /** What came back from an endpoint, however the call was routed. */
    private record FetchResult(int statusCode, String body, String error, long latencyMs, String executedVia) {}

    /**
     * Runs one HTTP call either from this server or through a connected remote agent — mirrors
     * {@code AppExecutionService.sendViaAgent}: the agent is a dumb pipe, everything about the
     * request (headers, body, timeout) is fully resolved on this side already.
     */
    private FetchResult doFetch(String method, String url, Map<String, String> headers, String body,
                                 long timeoutMs, String target, String agentId) {
        long start = System.currentTimeMillis();
        if ("AGENT".equalsIgnoreCase(target)) {
            try {
                HttpBatchRequest.Item item = new HttpBatchRequest.Item();
                item.setUrl(url);
                item.setMethod(HttpMethod.from(method));
                item.setHeaders(headers);
                item.setBody(body);
                item.setTimeoutMs((int) timeoutMs);

                Map<String, Object> reply = agentDispatch.dispatchSingle(item, (int) timeoutMs, agentId);
                long latency = System.currentTimeMillis() - start;
                String ranOn = describeAgent(reply.get("agentId"));

                Object failure = reply.get("error");
                if (failure != null) return new FetchResult(-1, null, ranOn + " reported: " + failure, latency, ranOn);
                if (!(reply.get("statusCode") instanceof Number statusCode))
                    return new FetchResult(-1, null, ranOn + " returned no status code", latency, ranOn);

                String respBody = reply.get("body") == null ? "" : String.valueOf(reply.get("body"));
                return new FetchResult(statusCode.intValue(), respBody, null, latency, ranOn);
            } catch (Exception e) {
                return new FetchResult(-1, null, e.getMessage(), System.currentTimeMillis() - start, "AGENT");
            }
        }

        try {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs));
            headers.forEach(b::header);
            b = b.method(method, body != null && !body.isBlank()
                    ? HttpRequest.BodyPublishers.ofString(body)
                    : HttpRequest.BodyPublishers.noBody());
            HttpResponse<String> resp = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            long latency = System.currentTimeMillis() - start;
            return new FetchResult(resp.statusCode(), resp.body(), null, latency, "LOCAL");
        } catch (Exception e) {
            return new FetchResult(-1, null, e.getMessage(), System.currentTimeMillis() - start, "LOCAL");
        }
    }

    /** How a result names where it ran — {@code LOCAL}, {@code AGENT} or {@code AGENT:orders-host1}. */
    private String describeAgent(Object agentId) {
        String id = agentId == null ? null : String.valueOf(agentId);
        return id == null || id.isBlank() || "null".equals(id) ? "AGENT" : "AGENT:" + id;
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
    private Map<String, Object> checkJolokia(String baseUrl, String authHeader, long timeoutMs, String target, String agentId) {
        String base = baseUrl.trim();
        Map<String, Object> m = new LinkedHashMap<>();
        String body;
        try {
            body = objectMapper.writeValueAsString(List.of(
                    Map.of("type", "read", "mbean", "java.lang:type=Memory", "attribute", "HeapMemoryUsage"),
                    Map.of("type", "read", "mbean", "java.lang:type=Runtime", "attribute", List.of("Uptime", "Name")),
                    Map.of("type", "read", "mbean", "java.lang:type=Threading", "attribute", "ThreadCount")
            ));
        } catch (Exception e) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", e.getMessage());
            return m;
        }

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        if (authHeader != null) headers.put("Authorization", authHeader);
        FetchResult fr = doFetch("POST", base, headers, body, timeoutMs, target, agentId);
        m.put("executedVia", fr.executedVia());

        if (fr.error() != null) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", fr.error());
            m.put("latencyMs", fr.latencyMs());
            return m;
        }

        if (fr.statusCode() >= 400) {
            m.put("ok", true);
            m.put("status", "DOWN");
            m.put("statusCode", fr.statusCode());
            m.put("latencyMs", fr.latencyMs());
            m.put("metrics", Map.of());
            m.put("error", "Unexpected HTTP status: " + fr.statusCode());
            return m;
        }

        try {
            Map<String, Object> metrics = new LinkedHashMap<>();
            boolean anyOk = false;
            for (JsonNode entry : objectMapper.readTree(fr.body())) {
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
            m.put("statusCode", fr.statusCode());
            m.put("latencyMs", fr.latencyMs());
            m.put("metrics", metrics);
            if (!anyOk) m.put("error", "No Jolokia attributes returned a 200 status");
        } catch (Exception e) {
            m.put("ok", false);
            m.put("status", "DOWN");
            m.put("error", e.getMessage());
            m.put("latencyMs", fr.latencyMs());
        }
        return m;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────
    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v instanceof String s ? s.trim() : null;
    }

    /** Mirrors AutoSysController's auth handling: bearer token wins if present, else basic. */
    private String buildAuth(String authType, String username, String password, String token) {
        if ("bearer".equals(authType) && token != null && !token.isBlank()) return "Bearer " + token.trim();
        if ("basic".equals(authType) && username != null && !username.isBlank()) {
            String cred = username + ":" + (password != null ? password : "");
            return "Basic " + Base64.getEncoder().encodeToString(cred.getBytes());
        }
        // Tolerate callers that don't set authType explicitly but do supply credentials.
        if (authType == null || authType.isBlank()) {
            if (token != null && !token.isBlank()) return "Bearer " + token.trim();
            if (username != null && !username.isBlank()) {
                String cred = username + ":" + (password != null ? password : "");
                return "Basic " + Base64.getEncoder().encodeToString(cred.getBytes());
            }
        }
        return null;
    }

    private void sendEvent(SseEmitter emitter, String event, Object data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            emitter.send(SseEmitter.event()
                    .name(event != null ? event : "message")
                    .data(json != null ? json : "null"));
        } catch (Exception ignored) {}
    }

    private ResponseEntity<Map<String, Object>> err(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg, "ok", false));
    }
}
