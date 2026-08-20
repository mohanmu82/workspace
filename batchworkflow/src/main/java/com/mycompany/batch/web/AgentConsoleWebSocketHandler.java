package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.service.AgentRegistryService;
import com.mycompany.batch.service.AgentRegistryService.AgentConnection;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * WebSocket endpoint at {@code /agentconsole/ws} — the browser-facing side of remote agent
 * command execution. The UI asks for the current agent list and issues {@code exec} or
 * {@code http} requests here; this handler looks the target agent up in
 * {@link AgentRegistryService}, forwards the request over its {@code /agent/ws} session, and the
 * agent's reply is routed straight back to this session by {@link AgentWebSocketHandler}.
 *
 * <p>Protocol (browser → server):
 * <pre>
 * {"type":"list"}
 * {"type":"exec","agentId":"host1","command":"uptime"}
 * {"type":"http","agentId":"host1","url":"https://...","method":"POST","headers":{...},"body":"...","timeoutMs":30000}
 * </pre>
 *
 * <p>Protocol (server → browser):
 * <pre>
 * {"type":"agents","agents":[{"agentId":"host1","hostname":"...","connectedAt":"...","lastSeen":"..."}]}
 * {"type":"started","requestId":"...","agentId":"host1"}
 * {"type":"line","requestId":"...","text":"..."}
 * {"type":"done","requestId":"...","exitCode":0}
 * {"type":"httpResult","requestId":"...","statusCode":200,"headers":{...},"body":"...","durationMs":42}
 * {"type":"error","message":"..."}
 * </pre>
 */
@Component
public class AgentConsoleWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper         objectMapper;
    private final AgentRegistryService registry;

    public AgentConsoleWebSocketHandler(ObjectMapper objectMapper, AgentRegistryService registry) {
        this.objectMapper = objectMapper;
        this.registry     = registry;
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession ws, @NonNull TextMessage message) {
        try {
            Map<String, Object> params = objectMapper.readValue(
                    message.getPayload(), new TypeReference<>() {});
            switch (str(params, "type")) {
                case "list" -> sendAgentList(ws);
                case "exec" -> handleExec(ws, params);
                case "http" -> handleHttp(ws, params);
                default     -> sendError(ws, "Unknown message type: " + str(params, "type"));
            }
        } catch (Exception e) {
            sendError(ws, e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession ws, @NonNull CloseStatus status) {
        registry.dropRequestsFor(ws);
    }

    // -------------------------------------------------------------------------

    private void sendAgentList(WebSocketSession ws) {
        List<Map<String, Object>> agents = registry.list().stream()
                .map(this::describe)
                .toList();
        sendJson(ws, Map.of("type", "agents", "agents", agents));
    }

    private Map<String, Object> describe(AgentConnection a) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("agentId", a.getAgentId());
        m.put("hostname", a.getHostname());
        m.put("connectedAt", a.getConnectedAt().toString());
        m.put("lastSeen", a.getLastSeen().toString());
        return m;
    }

    private void handleExec(WebSocketSession ws, Map<String, Object> params) {
        String agentId = str(params, "agentId");
        String command = str(params, "command");
        if (agentId.isBlank() || command.isBlank()) {
            sendError(ws, "agentId and command are required");
            return;
        }

        AgentConnection conn = registry.get(agentId);
        if (conn == null || !conn.getSession().isOpen()) {
            sendError(ws, "Agent not connected: " + agentId);
            return;
        }

        String requestId = registry.trackRequest(ws);
        sendJson(ws, Map.of("type", "started", "requestId", requestId, "agentId", agentId));
        sendJson(conn.getSession(), Map.of("type", "exec", "requestId", requestId, "command", command));
    }

    private void handleHttp(WebSocketSession ws, Map<String, Object> params) {
        String agentId = str(params, "agentId");
        String url     = str(params, "url");
        if (agentId.isBlank() || url.isBlank()) {
            sendError(ws, "agentId and url are required");
            return;
        }

        AgentConnection conn = registry.get(agentId);
        if (conn == null || !conn.getSession().isOpen()) {
            sendError(ws, "Agent not connected: " + agentId);
            return;
        }

        String requestId = registry.trackRequest(ws);
        Map<String, Object> outbound = new LinkedHashMap<>();
        outbound.put("type", "http");
        outbound.put("requestId", requestId);
        outbound.put("url", url);
        outbound.put("method", params.getOrDefault("method", "GET"));
        outbound.put("headers", params.getOrDefault("headers", Map.of()));
        outbound.put("body", params.getOrDefault("body", ""));
        outbound.put("timeoutMs", params.getOrDefault("timeoutMs", 30000));

        sendJson(ws, Map.of("type", "started", "requestId", requestId, "agentId", agentId));
        sendJson(conn.getSession(), outbound);
    }

    // -------------------------------------------------------------------------
    // Messaging helpers
    // -------------------------------------------------------------------------

    private void sendError(WebSocketSession ws, String message) {
        sendJson(ws, Map.of("type", "error", "message", message != null ? message : "Unknown error"));
    }

    private void sendJson(WebSocketSession ws, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            if (json == null) return;
            synchronized (ws) {
                if (ws.isOpen()) ws.sendMessage(new TextMessage(json));
            }
        } catch (Exception ignored) {}
    }

    private static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString().trim() : "";
    }
}
