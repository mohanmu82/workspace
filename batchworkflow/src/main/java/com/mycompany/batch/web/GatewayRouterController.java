package com.mycompany.batch.web;

import com.mycompany.batch.model.RouteRule;
import com.mycompany.batch.service.GatewayRouterService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Runtime gateway router.
 *
 * CRUD:   GET/POST /gatewayrouter/routes, DELETE /gatewayrouter/routes/{id}
 * Match:  GET /gatewayrouter/match?path=...  (dry-run — shows which route a path would hit)
 * Proxy:  /gateway/**  (any method) — forwarded to the matching route's targetUri
 */
@RestController
public class GatewayRouterController {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    /**
     * Request headers not forwarded downstream — hop-by-hop headers, plus the set the JDK's
     * HttpClient refuses to let callers set directly ("restricted headers": connection,
     * content-length, expect, host, upgrade).
     */
    private static final Set<String> EXCLUDED_REQUEST_HEADERS = Set.of(
            "host", "content-length", "connection", "keep-alive", "expect",
            "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade");

    /** Response headers not relayed back to the caller. */
    private static final Set<String> EXCLUDED_RESPONSE_HEADERS = Set.of(
            "content-length", "connection", "keep-alive",
            "transfer-encoding", "upgrade");

    private final GatewayRouterService routerService;

    public GatewayRouterController(GatewayRouterService routerService) {
        this.routerService = routerService;
    }

    // -------------------------------------------------------------------------
    // CRUD
    // -------------------------------------------------------------------------

    @GetMapping("/gatewayrouter/routes")
    public ResponseEntity<List<RouteRule>> listRoutes() {
        return ResponseEntity.ok(routerService.list());
    }

    @PostMapping("/gatewayrouter/routes")
    public ResponseEntity<?> saveRoute(@RequestBody RouteRule rule) {
        try {
            return ResponseEntity.ok(routerService.save(rule));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/gatewayrouter/routes/{id}")
    public ResponseEntity<?> deleteRoute(@PathVariable String id) {
        try {
            routerService.delete(id);
            return ResponseEntity.ok(Map.of("status", "deleted", "id", id));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/gatewayrouter/match")
    public ResponseEntity<?> match(@RequestParam String path) {
        Optional<RouteRule> match = routerService.findMatch(path);
        if (match.isEmpty()) return ResponseEntity.ok(Map.of("matched", false));
        RouteRule r = match.get();
        String remainder = path.substring(r.getPrefix().length());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("matched", true);
        result.put("id", r.getId());
        result.put("prefix", r.getPrefix());
        result.put("targetUri", r.getTargetUri());
        result.put("resolvedUrl", r.getTargetUri() + remainder);
        return ResponseEntity.ok(result);
    }

    /**
     * Issues a lightweight GET against each requested route's targetUri and reports the HTTP
     * status returned, so the UI can tell which routes are currently reachable. Body {@code ids}
     * selects which routes to test; omitted or empty means "test all routes".
     */
    @PostMapping("/gatewayrouter/routes/test")
    public ResponseEntity<List<Map<String, Object>>> testRoutes(@RequestBody(required = false) Map<String, List<String>> body) {
        List<String> ids = body != null ? body.get("ids") : null;
        List<RouteRule> toTest = new ArrayList<>();
        for (RouteRule r : routerService.list()) {
            if (ids == null || ids.isEmpty() || ids.contains(r.getId())) toTest.add(r);
        }

        List<Map<String, Object>> results = new ArrayList<>();
        for (RouteRule r : toTest) {
            results.add(testOne(r));
        }
        return ResponseEntity.ok(results);
    }

    private Map<String, Object> testOne(RouteRule r) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", r.getId());
        result.put("name", r.getName());
        result.put("prefix", r.getPrefix());
        result.put("targetUri", r.getTargetUri());

        long start = System.currentTimeMillis();
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(r.getTargetUri()))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<Void> response = HTTP.send(req, HttpResponse.BodyHandlers.discarding());
            result.put("statusCode", response.statusCode());
            result.put("ok", response.statusCode() < 400);
            result.put("error", null);
        } catch (Exception e) {
            result.put("statusCode", null);
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        result.put("latencyMs", System.currentTimeMillis() - start);
        return result;
    }

    // -------------------------------------------------------------------------
    // Proxy
    // -------------------------------------------------------------------------

    @RequestMapping(GatewayRouterService.GATEWAY_PREFIX + "/**")
    public ResponseEntity<byte[]> proxy(HttpServletRequest request) throws IOException, InterruptedException {
        String path = request.getRequestURI();

        Optional<RouteRule> match = routerService.findMatch(path);
        if (match.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(("{\"error\":\"no gateway route matches " + path + "\"}").getBytes());
        }
        RouteRule rule = match.get();

        String remainder = path.substring(rule.getPrefix().length());
        String targetUrl = rule.getTargetUri() + remainder;
        if (request.getQueryString() != null) targetUrl += "?" + request.getQueryString();

        String method = request.getMethod();
        byte[] body = request.getInputStream().readAllBytes();
        boolean bodyless = body.length == 0 || method.equalsIgnoreCase("GET") || method.equalsIgnoreCase("HEAD");
        HttpRequest.BodyPublisher bodyPublisher = bodyless
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(Duration.ofSeconds(30))
                .method(method, bodyPublisher);

        Collections.list(request.getHeaderNames()).forEach(name -> {
            if (!EXCLUDED_REQUEST_HEADERS.contains(name.toLowerCase())) {
                Collections.list(request.getHeaders(name)).forEach(value -> builder.header(name, value));
            }
        });

        HttpResponse<byte[]> response;
        try {
            response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(("{\"error\":\"" + e.getMessage() + "\"}").getBytes());
        }

        HttpHeaders headers = new HttpHeaders();
        response.headers().map().forEach((name, values) -> {
            if (!EXCLUDED_RESPONSE_HEADERS.contains(name.toLowerCase())) headers.addAll(name, values);
        });

        return ResponseEntity.status(response.statusCode()).headers(headers).body(response.body());
    }
}
