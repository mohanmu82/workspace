package com.mycompany.batch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.ServerPropertiesLoader;
import com.mycompany.batch.model.RouteRule;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Holds the runtime-editable set of {@code /gateway/**} route rules, persisted as a single
 * JSON array at {@code ${DATADIR}/gatewayrouter/routes.json}. Routes can be added, edited,
 * or removed at runtime through {@link com.mycompany.batch.web.GatewayRouterController}
 * without restarting the app.
 */
@Service
public class GatewayRouterService {

    public static final String GATEWAY_PREFIX = "/gateway";

    private final ObjectMapper objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final List<RouteRule> routes = new CopyOnWriteArrayList<>();

    public GatewayRouterService(ObjectMapper objectMapper, ServerPropertiesLoader serverPropertiesLoader) {
        this.objectMapper = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
    }

    @PostConstruct
    public void load() throws IOException {
        routes.clear();
        Path file = routesFile();
        if (Files.exists(file)) {
            List<RouteRule> loaded = objectMapper.readValue(file.toFile(), new TypeReference<List<RouteRule>>() {});
            if (loaded != null) routes.addAll(loaded);
        }
    }

    public List<RouteRule> list() {
        List<RouteRule> copy = new ArrayList<>(routes);
        copy.sort(Comparator.comparing(RouteRule::getPrefix));
        return copy;
    }

    public RouteRule save(RouteRule rule) throws IOException {
        String prefix = rule.getPrefix();
        if (prefix == null || prefix.isBlank())
            throw new IllegalArgumentException("prefix is required");
        prefix = normalizePrefix(prefix);
        if (!prefix.equals(GATEWAY_PREFIX) && !prefix.startsWith(GATEWAY_PREFIX + "/"))
            throw new IllegalArgumentException("prefix must start with " + GATEWAY_PREFIX + "/");
        rule.setPrefix(prefix);

        if (rule.getTargetUri() == null || rule.getTargetUri().isBlank())
            throw new IllegalArgumentException("targetUri is required");
        String targetUri = rule.getTargetUri().replaceAll("/+$", "");
        if (!targetUri.matches("(?i)^https?://.+"))
            throw new IllegalArgumentException("targetUri must be an absolute http(s) URL");
        rule.setTargetUri(targetUri);

        String id = rule.getId();
        if (id == null || id.isBlank()) id = slug(prefix);
        rule.setId(id);

        for (RouteRule existing : routes) {
            if (existing.getId().equals(id)) {
                routes.remove(existing);
                break;
            }
        }
        routes.add(rule);
        persist();
        return rule;
    }

    public void delete(String id) throws IOException {
        boolean removed = routes.removeIf(r -> r.getId().equals(id));
        if (!removed) throw new IllegalArgumentException("route not found: " + id);
        persist();
    }

    /**
     * Finds the enabled route whose prefix most specifically matches the given request path
     * (longest matching prefix wins, so a more specific route overrides a broader one).
     */
    public Optional<RouteRule> findMatch(String path) {
        RouteRule best = null;
        for (RouteRule r : routes) {
            if (!r.isEnabled()) continue;
            String p = r.getPrefix();
            if (path.equals(p) || path.startsWith(p + "/")) {
                if (best == null || p.length() > best.getPrefix().length()) best = r;
            }
        }
        return Optional.ofNullable(best);
    }

    private void persist() throws IOException {
        Path file = routesFile();
        Files.createDirectories(file.getParent());
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), list());
    }

    private Path routesFile() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir).resolve("gatewayrouter").resolve("routes.json");
    }

    private static String normalizePrefix(String prefix) {
        String p = prefix.trim().replaceAll("/+$", "");
        if (!p.startsWith("/")) p = "/" + p;
        return p;
    }

    private static String slug(String prefix) {
        String s = prefix.replaceAll("[^a-zA-Z0-9]+", "-").replaceAll("^-+|-+$", "").toLowerCase();
        return s.isBlank() ? "route-" + System.currentTimeMillis() : s;
    }
}
