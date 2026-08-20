package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.service.AgentRegistryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket endpoint at {@code /agent/ws} — the control channel that standalone remote agent
 * jars dial into. The agent registers with an id/hostname/shared-secret token, then the server
 * forwards {@code exec}/{@code http} requests to it — from {@link AgentConsoleWebSocketHandler}
 * for browser-console use, or from {@code AgentHttpDispatchService} for server-side bulk
 * fan-out — and relays the agent's responses back to whichever requester asked: a browser
 * session for console requests, or a pending {@link com.mycompany.batch.service.AgentRegistryService}
 * future for server-dispatched ones.
 *
 * <p>Protocol (agent → server):
 * <pre>
 * {"type":"register","agentId":"host1","hostname":"host1.internal","token":"secret"}
 * {"type":"heartbeat"}
 * {"type":"line","requestId":"...","text":"..."}
 * {"type":"done","requestId":"...","exitCode":0}
 * {"type":"httpResult","requestId":"...","statusCode":200,"headers":{...},"body":"...","durationMs":42}
 * {"type":"error","requestId":"...","message":"..."}
 * </pre>
 *
 * <p>Protocol (server → agent):
 * <pre>
 * {"type":"registered","message":"..."}
 * {"type":"error","message":"..."}
 * {"type":"exec","requestId":"...","command":"..."}
 * {"type":"http","requestId":"...","url":"...","method":"POST","headers":{...},"body":"...","timeoutMs":30000}
 * </pre>
 */
@Component
public class AgentWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper          objectMapper;
    private final AgentRegistryService  registry;

    @Value("${agent.token:}")
    private String expectedToken;

    private final Map<String, String> sessionAgentIds = new ConcurrentHashMap<>();

    public AgentWebSocketHandler(ObjectMapper objectMapper, AgentRegistryService registry) {
        this.objectMapper = objectMapper;
        this.registry     = registry;
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession ws, @NonNull TextMessage message) {
        try {
            Map<String, Object> params = objectMapper.readValue(
                    message.getPayload(), new TypeReference<>() {});
            switch (str(params, "type")) {
                case "register"  -> handleRegister(ws, params);
                case "heartbeat" -> registry.touch(sessionAgentIds.get(ws.getId()));
                case "line"       -> forwardToBrowser(params, "line");
                case "done"       -> forwardToBrowser(params, "done");
                case "httpResult" -> forwardToBrowser(params, "httpResult");
                case "error"      -> forwardToBrowser(params, "error");
                default          -> sendError(ws, "Unknown message type: " + str(params, "type"));
            }
        } catch (Exception e) {
            sendError(ws, e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession ws, @NonNull CloseStatus status) {
        String agentId = sessionAgentIds.remove(ws.getId());
        if (agentId != null) registry.unregister(agentId, ws);
    }

    // -------------------------------------------------------------------------

    private void handleRegister(WebSocketSession ws, Map<String, Object> params) throws Exception {
        String agentId   = str(params, "agentId");
        String hostname  = str(params, "hostname");
        String token     = str(params, "token");

        if (agentId.isBlank()) {
            sendError(ws, "agentId is required");
            ws.close(CloseStatus.BAD_DATA);
            return;
        }
        if (!expectedToken.isBlank() && !expectedToken.equals(token)) {
            sendError(ws, "Invalid or missing agent token");
            ws.close(CloseStatus.POLICY_VIOLATION);
            return;
        }

        registry.register(agentId, hostname.isBlank() ? agentId : hostname, ws);
        sessionAgentIds.put(ws.getId(), agentId);
        send(ws, "registered", "Registered as " + agentId);
    }

    private void forwardToBrowser(Map<String, Object> params, String type) {
        String requestId = str(params, "requestId");
        if (requestId.isBlank()) return;

        Map<String, Object> outbound = new LinkedHashMap<>(params);
        outbound.put("type", type);

        boolean terminal = type.equals("done") || type.equals("error") || type.equals("httpResult");

        // Server-dispatched (bulk fan-out) requests are tracked by a future, not a browser session.
        if (terminal && registry.completeHttpRequest(requestId, outbound)) return;

        WebSocketSession browser = registry.resolveRequest(requestId, terminal);
        if (browser == null || !browser.isOpen()) return;
        sendJson(browser, outbound);
    }

    // -------------------------------------------------------------------------
    // Messaging helpers
    // -------------------------------------------------------------------------

    private void send(WebSocketSession ws, String type, String message) {
        sendJson(ws, Map.of("type", type, "message", message));
    }

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
