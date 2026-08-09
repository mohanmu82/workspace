package com.mycompany.batch.apm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds the currently-registered APM server (if any) and forwards each HTTP transaction
 * {@link ApmTransactionFilter} records as an Elastic APM intake v2 event (NDJSON POST to
 * {@code <serverUrl>/intake/v2/events}). Registration is runtime state — {@code POST /apm/register}
 * / {@code POST /apm/unregister} — not a database row; it resets on restart, falling back to
 * {@code apm.*} in application.properties if {@code apm.enabled=true} there.
 *
 * <p>Every recorded transaction (whether or not forwarding succeeded) is kept in a small
 * in-memory ring buffer so the UI can list/filter recent traffic — same "best-effort cache, not
 * a database" approach as {@code ServiceMonitorController}'s history buffer.
 */
@Service
public class ApmForwardingService {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final ApmProperties props;
    private final ObjectMapper objectMapper;
    private final Deque<Map<String, Object>> log = new ArrayDeque<>();
    private final AtomicLong forwardedCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();

    private volatile Registration registration;

    public record Registration(String serverUrl, String secretToken, String apiKey,
                                String serviceName, String serviceVersion, String environment,
                                Instant registeredAt) {}

    public ApmForwardingService(ApmProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
        if (props.isEnabled() && props.isConfigured()) {
            register(props.getServerUrl(), props.getSecretToken(), props.getApiKey(),
                    props.getServiceName(), props.getServiceVersion(), props.getEnvironment());
        }
    }

    public boolean isRegistered() {
        return registration != null;
    }

    public Map<String, Object> register(String serverUrl, String secretToken, String apiKey,
                                         String serviceName, String serviceVersion, String environment) {
        registration = new Registration(
                serverUrl.trim().replaceAll("/+$", ""),
                blankToNull(secretToken),
                blankToNull(apiKey),
                (serviceName == null || serviceName.isBlank()) ? "batchworkflow" : serviceName.trim(),
                serviceVersion != null ? serviceVersion.trim() : "",
                environment != null ? environment.trim() : "",
                Instant.now());
        return status();
    }

    public Map<String, Object> unregister() {
        registration = null;
        return status();
    }

    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        Registration r = registration;
        m.put("registered", r != null);
        if (r != null) {
            m.put("serverUrl", r.serverUrl());
            m.put("serviceName", r.serviceName());
            m.put("serviceVersion", r.serviceVersion());
            m.put("environment", r.environment());
            m.put("registeredAt", r.registeredAt().toString());
            m.put("hasSecretToken", r.secretToken() != null);
            m.put("hasApiKey", r.apiKey() != null);
        }
        m.put("forwardedCount", forwardedCount.get());
        m.put("failedCount", failedCount.get());
        synchronized (log) {
            m.put("bufferedCount", log.size());
        }
        return m;
    }

    /** Called off the request thread (see ApmTransactionFilter) so forwarding latency never adds to a real request. */
    public void recordAndForward(String method, String path, int statusCode, long latencyMs) {
        Registration r = registration;
        if (r == null) return;

        String txnId = randomHex(8);
        String traceId = randomHex(16);
        long tsMicros = Instant.now().toEpochMilli() * 1000;

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", UUID.randomUUID().toString());
        entry.put("ts", System.currentTimeMillis());
        entry.put("method", method);
        entry.put("path", path);
        entry.put("statusCode", statusCode);
        entry.put("latencyMs", latencyMs);
        entry.put("transactionId", txnId);
        entry.put("traceId", traceId);
        entry.put("serverUrl", r.serverUrl());
        entry.put("forwarded", false);
        entry.put("forwardError", null);

        try {
            String ndjson = buildNdjson(r, txnId, traceId, method, path, statusCode, latencyMs, tsMicros);
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(r.serverUrl() + "/intake/v2/events"))
                    .timeout(Duration.ofMillis(props.getTimeoutMs()))
                    .header("Content-Type", "application/x-ndjson")
                    .POST(HttpRequest.BodyPublishers.ofString(ndjson));
            if (r.secretToken() != null) b.header("Authorization", "Bearer " + r.secretToken());
            else if (r.apiKey() != null) b.header("Authorization", "ApiKey " + r.apiKey());

            HttpResponse<String> resp = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                entry.put("forwarded", true);
                forwardedCount.incrementAndGet();
            } else {
                entry.put("forwardError", "APM server returned HTTP " + resp.statusCode());
                failedCount.incrementAndGet();
            }
        } catch (Exception e) {
            entry.put("forwardError", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            failedCount.incrementAndGet();
        }

        synchronized (log) {
            log.addLast(entry);
            while (log.size() > props.getHistoryCap()) log.pollFirst();
        }
    }

    /**
     * Newest-first, optionally filtered. {@code type} is one of "forwarded"/"failed"/null(=all);
     * {@code q} substring-matches the request path (case-insensitive).
     */
    public List<Map<String, Object>> listRequests(String type, String method, Integer statusCode,
                                                   String q, Long sinceMs, int limit) {
        List<Map<String, Object>> snapshot;
        synchronized (log) {
            snapshot = new ArrayList<>(log);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = snapshot.size() - 1; i >= 0 && out.size() < limit; i--) {
            Map<String, Object> e = snapshot.get(i);
            if (method != null && !method.equalsIgnoreCase(String.valueOf(e.get("method")))) continue;
            if (statusCode != null && !statusCode.equals(e.get("statusCode"))) continue;
            if (sinceMs != null && ((Number) e.get("ts")).longValue() < sinceMs) continue;
            if (q != null && !q.isBlank() && !String.valueOf(e.get("path")).toLowerCase().contains(q.toLowerCase())) continue;
            boolean forwarded = Boolean.TRUE.equals(e.get("forwarded"));
            if ("forwarded".equals(type) && !forwarded) continue;
            if ("failed".equals(type) && forwarded) continue;
            out.add(e);
        }
        return out;
    }

    private String buildNdjson(Registration r, String txnId, String traceId, String method, String path,
                                int statusCode, long latencyMs, long tsMicros) throws Exception {
        Map<String, Object> service = new LinkedHashMap<>();
        service.put("name", r.serviceName());
        if (!r.serviceVersion().isBlank()) service.put("version", r.serviceVersion());
        if (!r.environment().isBlank()) service.put("environment", r.environment());
        service.put("agent", Map.of("name", "batchworkflow-custom", "version", "1.0.0"));
        service.put("language", Map.of("name", "java"));
        Map<String, Object> metadata = Map.of("metadata", Map.of("service", service));

        Map<String, Object> transaction = new LinkedHashMap<>();
        transaction.put("id", txnId);
        transaction.put("trace_id", traceId);
        transaction.put("parent_id", null);
        transaction.put("name", method + " " + path);
        transaction.put("type", "request");
        transaction.put("duration", (double) latencyMs);
        transaction.put("timestamp", tsMicros);
        transaction.put("result", resultBucket(statusCode));
        transaction.put("context", Map.of(
                "request", Map.of("method", method, "url", Map.of("pathname", path)),
                "response", Map.of("status_code", statusCode)));
        transaction.put("span_count", Map.of("started", 0));

        return objectMapper.writeValueAsString(metadata) + "\n"
                + objectMapper.writeValueAsString(Map.of("transaction", transaction)) + "\n";
    }

    private String resultBucket(int statusCode) {
        return "HTTP " + (statusCode / 100) + "xx";
    }

    private String randomHex(int bytes) {
        byte[] buf = new byte[bytes];
        RANDOM.nextBytes(buf);
        char[] out = new char[bytes * 2];
        for (int i = 0; i < bytes; i++) {
            out[i * 2] = HEX[(buf[i] >> 4) & 0xF];
            out[i * 2 + 1] = HEX[buf[i] & 0xF];
        }
        return new String(out);
    }

    private String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }
}
