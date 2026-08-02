package com.mycompany.batch.alert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fires a webhook (Slack-formatted or a raw generic JSON payload) when a monitored service
 * transitions UP↔DOWN. The dashboard is the one that notices a transition (it already holds
 * the previous result per target to render the card), so it calls {@link #notify} — this class
 * just owns the config, the per-target cooldown (so several open dashboard tabs polling the
 * same targets don't all fire the same alert), and the actual HTTP POST.
 */
@Service
public class AlertService {

    private static final String CONFIG_RESOURCE = "alertconfig.json";

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final ObjectMapper objectMapper;
    private volatile AlertConfig config = new AlertConfig();
    private final Map<String, Long> lastAlertAt = new ConcurrentHashMap<>();

    public AlertService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void load() {
        AlertConfig loaded = readConfigFile();
        if (loaded != null) config = loaded;
    }

    public AlertConfig getConfig() {
        return config;
    }

    public synchronized AlertConfig saveConfig(AlertConfig newConfig) throws Exception {
        config = newConfig;
        writeConfigFile(newConfig);
        return config;
    }

    /** Sends immediately, ignoring cooldown — used by the "Test Webhook" button. */
    public Map<String, Object> sendTest(AlertConfig cfg, Map<String, Object> event) throws Exception {
        if (cfg.getWebhookUrl() == null || cfg.getWebhookUrl().isBlank()) {
            throw new IllegalArgumentException("webhookUrl is required");
        }
        return postWebhook(cfg, event);
    }

    /**
     * Sends the configured webhook for a transition, unless alerting is disabled, no webhook is
     * configured, or this key alerted within the cooldown window. Returns a small status map
     * describing what happened rather than throwing, since this is a best-effort side channel —
     * a failed alert should never fail the check that triggered it.
     */
    public Map<String, Object> notify(String key, Map<String, Object> event) {
        AlertConfig cfg = config;
        Map<String, Object> result = new LinkedHashMap<>();
        if (!cfg.isEnabled() || cfg.getWebhookUrl() == null || cfg.getWebhookUrl().isBlank()) {
            result.put("sent", false);
            result.put("reason", "alerting disabled or no webhook configured");
            return result;
        }

        long now = System.currentTimeMillis();
        Long last = lastAlertAt.get(key);
        long cooldownMs = cfg.getCooldownSeconds() * 1000L;
        if (last != null && now - last < cooldownMs) {
            result.put("sent", false);
            result.put("reason", "cooldown");
            return result;
        }

        try {
            postWebhook(cfg, event);
            lastAlertAt.put(key, now);
            result.put("sent", true);
        } catch (Exception e) {
            result.put("sent", false);
            result.put("reason", "webhook call failed: " + e.getMessage());
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Webhook payload + delivery
    // -------------------------------------------------------------------------

    private Map<String, Object> postWebhook(AlertConfig cfg, Map<String, Object> event) throws Exception {
        Object payload = "generic".equals(cfg.getFormat()) ? event : Map.of("text", buildSlackText(event));
        String body = objectMapper.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(cfg.getWebhookUrl()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("statusCode", resp.statusCode());
        if (resp.statusCode() >= 400) throw new RuntimeException("Webhook returned HTTP " + resp.statusCode());
        return m;
    }

    private String buildSlackText(Map<String, Object> event) {
        String name       = str(event, "name");
        String url         = str(event, "url");
        String status       = str(event, "status");
        String previousStatus = str(event, "previousStatus");
        String error       = str(event, "error");

        boolean recovered = "UP".equals(status) && previousStatus != null && !"UP".equals(previousStatus);
        String icon = "UP".equals(status) ? ":large_green_circle:" : ":red_circle:";
        StringBuilder sb = new StringBuilder();
        sb.append(icon).append(" *").append(name != null ? name : "service").append("* is now *").append(status).append("*");
        if (recovered) sb.append(" (recovered)");
        if (url != null) sb.append("\n").append(url);
        if (error != null && !error.isBlank() && !"UP".equals(status)) sb.append("\n").append(error);
        return sb.toString();
    }

    private String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v != null ? String.valueOf(v) : null;
    }

    // -------------------------------------------------------------------------
    // Persistence — same resolve/read/write pattern as the other *Service classes in this app
    // -------------------------------------------------------------------------

    private Path resolveConfigPath() {
        Path srcResources = Path.of("src/main/resources");
        if (Files.isDirectory(srcResources)) {
            return srcResources.resolve(CONFIG_RESOURCE);
        }
        try {
            URL url = getClass().getClassLoader().getResource(CONFIG_RESOURCE);
            if (url != null && "file".equals(url.getProtocol())) {
                return Path.of(url.toURI());
            }
            URL rootUrl = getClass().getClassLoader().getResource("application.properties");
            if (rootUrl != null && "file".equals(rootUrl.getProtocol())) {
                return Path.of(rootUrl.toURI()).getParent().resolve(CONFIG_RESOURCE);
            }
        } catch (Exception ignored) { }
        return srcResources.resolve(CONFIG_RESOURCE);
    }

    private AlertConfig readConfigFile() {
        Path path = resolveConfigPath();
        if (!Files.isRegularFile(path)) return null;
        try (InputStream is = Files.newInputStream(path)) {
            return objectMapper.readValue(is, new TypeReference<AlertConfig>() {});
        } catch (Exception e) {
            return null;
        }
    }

    private void writeConfigFile(AlertConfig cfg) throws Exception {
        Path target = resolveConfigPath();
        Files.createDirectories(target.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), cfg);
    }
}
