package com.mycompany.batch.web;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientSseClientTransport;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.annotation.PreDestroy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Backend for mcpclient.html: connects to any MCP server (streamable HTTP or HTTP+SSE) and relays
 * list / call / read requests to it. The connection lives here rather than in the browser so remote
 * servers need no CORS, and the SDK handles sessions, SSE framing and the initialize handshake.
 */
@RestController
@RequestMapping("/mcpclient")
public class McpClientController {

    public record ConnectRequest(String url, String transport, Map<String, String> headers, Integer timeoutSeconds) {}

    public record CallRequest(String method, Map<String, Object> params) {}

    private record Session(String id, String url, String transport, McpSyncClient client) {}

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    @PostMapping("/connect")
    public ResponseEntity<?> connect(@RequestBody ConnectRequest req) {
        if (req.url() == null || req.url().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "url is required"));
        }
        McpSyncClient client = null;
        try {
            URI uri = URI.create(req.url().trim());
            if (uri.getScheme() == null || !uri.getScheme().startsWith("http") || uri.getHost() == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "url must be http(s)://host[:port]/path"));
            }
            String base = uri.getScheme() + "://" + uri.getRawAuthority();
            String path = (uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath())
                    + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
            Map<String, String> headers = req.headers() == null ? Map.of() : req.headers();
            boolean sse = "sse".equalsIgnoreCase(req.transport());
            // SDK 0.16 SSE transport drops stream errors (onErrorDropped) and initialize() just hangs until timeout
            if (sse) {
                String err = probe(uri, headers, true);
                if (err != null) return ResponseEntity.badRequest().body(Map.of("error", err));
            }
            McpClientTransport transport = sse
                    ? HttpClientSseClientTransport.builder(base).sseEndpoint(path)
                        .customizeRequest(b -> headers.forEach(b::header)).build()
                    : HttpClientStreamableHttpTransport.builder(base).endpoint(path)
                        .customizeRequest(b -> headers.forEach(b::header)).build();
            Duration timeout = Duration.ofSeconds(req.timeoutSeconds() == null ? 60 : req.timeoutSeconds());
            client = McpClient.sync(transport)
                    .clientInfo(new McpSchema.Implementation("batchworkflow-mcpclient", "1.0.0"))
                    .requestTimeout(timeout)
                    .initializationTimeout(timeout)
                    .build();
            McpSchema.InitializeResult init = client.initialize();
            Session s = new Session(UUID.randomUUID().toString(), req.url().trim(), sse ? "sse" : "streamable", client);
            sessions.put(s.id(), s);
            Map<String, Object> body = describe(s);
            body.put("initialize", init);
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            if (client != null) client.close();
            // SDK reports HTTP failures as "DummyEvent[...]"; re-ask the server to surface its real status/body
            String err = null;
            if (!"sse".equalsIgnoreCase(req.transport())) {
                try {
                    err = probe(URI.create(req.url().trim()), req.headers() == null ? Map.of() : req.headers(), false);
                } catch (IllegalArgumentException ignored) { }
            }
            return ResponseEntity.badRequest().body(Map.of("error", err != null ? err : message(e)));
        }
    }

    /** Plain HTTP check of the endpoint; returns "HTTP status: body" when the server rejects it, else null. */
    private static String probe(URI uri, Map<String, String> headers, boolean sse) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15));
            headers.forEach(b::header);
            if (sse) {
                b.header("Accept", "text/event-stream").GET();
            } else {
                b.header("Accept", "application/json, text/event-stream").header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"jsonrpc\":\"2.0\",\"id\":\"probe\","
                                + "\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\","
                                + "\"capabilities\":{},\"clientInfo\":{\"name\":\"batchworkflow-mcpclient\",\"version\":\"1.0.0\"}}}"));
            }
            HttpResponse<InputStream> r = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()
                    .send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream in = r.body()) {
                if (r.statusCode() / 100 == 2) return null; // don't read on success: an SSE body never ends
                return "HTTP " + r.statusCode() + ": " + new String(in.readNBytes(2000), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            return "Cannot reach " + uri + " — " + message(e);
        }
    }

    @GetMapping("/sessions")
    public List<Map<String, Object>> sessions() {
        return sessions.values().stream().map(this::describe).toList();
    }

    @DeleteMapping("/sessions/{id}")
    public ResponseEntity<?> disconnect(@PathVariable String id) {
        Session s = sessions.remove(id);
        if (s == null) return ResponseEntity.notFound().build();
        s.client().closeGracefully();
        return ResponseEntity.ok(Map.of("status", "closed"));
    }

    /** One MCP request on an open session; {@code method} is the MCP method name, e.g. {@code tools/call}. */
    @PostMapping("/sessions/{id}/call")
    public ResponseEntity<?> call(@PathVariable String id, @RequestBody CallRequest req) {
        Session s = sessions.get(id);
        if (s == null) return ResponseEntity.status(404).body(Map.of("error", "Session not found — reconnect"));
        Map<String, Object> p = req.params() == null ? Map.of() : req.params();
        McpSyncClient c = s.client();
        long start = System.currentTimeMillis();
        try {
            Object result = switch (req.method() == null ? "" : req.method()) {
                case "ping" -> { c.ping(); yield Map.of(); }
                case "tools/list" -> c.listTools();
                case "tools/call" -> c.callTool(new McpSchema.CallToolRequest(str(p, "name"), map(p, "arguments")));
                case "resources/list" -> c.listResources();
                case "resources/templates/list" -> c.listResourceTemplates();
                case "resources/read" -> c.readResource(new McpSchema.ReadResourceRequest(str(p, "uri")));
                case "prompts/list" -> c.listPrompts();
                case "prompts/get" -> c.getPrompt(new McpSchema.GetPromptRequest(str(p, "name"), map(p, "arguments")));
                case "logging/setLevel" -> {
                    c.setLoggingLevel(McpSchema.LoggingLevel.valueOf(str(p, "level").toUpperCase()));
                    yield Map.of();
                }
                default -> throw new IllegalArgumentException("Unsupported method: " + req.method());
            };
            return ResponseEntity.ok(Map.of("result", result, "elapsedMs", System.currentTimeMillis() - start));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", message(e),
                    "elapsedMs", System.currentTimeMillis() - start));
        }
    }

    @PreDestroy
    void closeAll() {
        sessions.values().forEach(s -> s.client().close());
        sessions.clear();
    }

    private Map<String, Object> describe(Session s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.id());
        m.put("url", s.url());
        m.put("transport", s.transport());
        m.put("serverInfo", s.client().getServerInfo());
        m.put("capabilities", s.client().getServerCapabilities());
        m.put("instructions", s.client().getServerInstructions());
        return m;
    }

    private static String str(Map<String, Object> p, String key) {
        Object v = p.get(key);
        if (v == null || v.toString().isBlank()) throw new IllegalArgumentException(key + " is required");
        return v.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> p, String key) {
        Object v = p.get(key);
        return v instanceof Map ? (Map<String, Object>) v : Map.of();
    }

    private static String message(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        String msg = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
        return root == e ? msg : e.getMessage() + " — " + msg;
    }
}
