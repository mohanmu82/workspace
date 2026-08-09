package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ObserveProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Polls Observe's OPAL export/query API (https://docs.observeinc.com/reference/exportquery) on a
 * fixed interval for server infrastructure metrics (CPU, memory, host health, ...) and keeps the
 * most recent result in memory — same "no persistence, just a cache" approach as
 * {@link ServiceMonitorController}'s history ring buffer, not a database.
 */
@Service
public class ObservePollingService {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final ObserveProperties props;
    private final ObjectMapper objectMapper;

    private volatile List<Map<String, Object>> latestRows = List.of();
    private volatile Instant lastAttemptAt;
    private volatile Instant lastSuccessAt;
    private volatile String lastError;

    public ObservePollingService(ObserveProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${observe.poll-interval-ms:60000}",
               initialDelayString = "${observe.poll-interval-ms:60000}")
    public void poll() {
        if (!props.isEnabled()) return;
        pollNow();
    }

    /** Runs the OPAL query immediately, outside the schedule — backs the manual /observe/refresh endpoint. */
    public Map<String, Object> pollNow() {
        lastAttemptAt = Instant.now();
        try {
            latestRows = runQuery();
            lastSuccessAt = Instant.now();
            lastError = null;
        } catch (Exception e) {
            lastError = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        }
        return status();
    }

    private List<Map<String, Object>> runQuery() throws Exception {
        if (!props.isConfigured()) {
            throw new IllegalStateException(
                    "observe.base-url, customer-id, api-token, dataset-id and opal-query must all be set");
        }

        Map<String, Object> stage = Map.of(
                "stageID", "main",
                "input", List.of(Map.of("inputName", "main", "datasetId", props.getDatasetId())),
                "pipeline", props.getOpalQuery());
        String bodyJson = objectMapper.writeValueAsString(Map.of("query", Map.of("stages", List.of(stage))));

        String url = props.getBaseUrl().replaceAll("/+$", "")
                + "/v1/meta/export/query?interval=" + props.getLookback();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(props.getTimeoutMs()))
                .header("Authorization", "Bearer " + props.getCustomerId() + " " + props.getApiToken())
                .header("Content-Type", "application/json")
                .header("Accept", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
                .build();

        HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IllegalStateException("Observe API returned HTTP " + resp.statusCode() + ": " + truncate(resp.body()));
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : resp.body().split("\r?\n")) {
            if (line.isBlank()) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> row = objectMapper.readValue(line, Map.class);
            rows.add(row);
        }
        return rows;
    }

    private String truncate(String s) {
        if (s == null) return "";
        return s.length() > 500 ? s.substring(0, 500) + "..." : s;
    }

    public List<Map<String, Object>> latestRows() {
        return latestRows;
    }

    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", props.isEnabled());
        m.put("configured", props.isConfigured());
        m.put("rowCount", latestRows.size());
        m.put("lastAttemptAt", lastAttemptAt != null ? lastAttemptAt.toString() : null);
        m.put("lastSuccessAt", lastSuccessAt != null ? lastSuccessAt.toString() : null);
        m.put("lastError", lastError);
        return m;
    }
}
