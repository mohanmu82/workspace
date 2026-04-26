package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket endpoint at {@code /logtail/ws}.
 *
 * <p>Client sends a JSON connect request:
 * <pre>
 * {
 *   "hostname":           "remotehost",
 *   "port":               22,
 *   "username":           "user",
 *   "logFilePath":        "/var/log/app/app.log",
 *   "privateKeyFilePath": "/home/user/.ssh/id_rsa",
 *   "tailLines":          200
 * }
 * </pre>
 *
 * <p>Server streams log lines back:
 * <pre>
 * {"type":"connected","message":"Connected to remotehost — tailing /var/log/app/app.log"}
 * {"type":"line","text":"2024-01-01 12:00:00 INFO Starting..."}
 * {"type":"error","message":"Connection refused"}
 * </pre>
 *
 * <p>Closing the WebSocket or sending {@code {"type":"stop"}} disconnects the SSH session.
 */
@Component
public class LogTailWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final Map<String, Session> sshSessions = new ConcurrentHashMap<>();

    public LogTailWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession ws, @NonNull TextMessage message) {
        try {
            Map<String, Object> params = objectMapper.readValue(
                    message.getPayload(), new TypeReference<>() {});

            if ("stop".equals(params.get("type"))) {
                disconnect(ws.getId());
                send(ws, "info", "Disconnected");
                return;
            }

            String hostname           = str(params, "hostname");
            String username           = str(params, "username");
            String logFilePath        = str(params, "logFilePath");
            String privateKeyFilePath = str(params, "privateKeyFilePath");
            int    port               = params.containsKey("port")
                    ? ((Number) params.get("port")).intValue() : 22;
            int    tailLines          = params.containsKey("tailLines")
                    ? ((Number) params.get("tailLines")).intValue() : 200;
            int    timeoutMs          = params.containsKey("timeoutMs")
                    ? ((Number) params.get("timeoutMs")).intValue() : 5000;

            if (hostname.isBlank() || username.isBlank() || logFilePath.isBlank() || privateKeyFilePath.isBlank()) {
                sendError(ws, "hostname, username, logFilePath and privateKeyFilePath are all required");
                return;
            }

            // Stop any existing tail for this WebSocket session
            disconnect(ws.getId());

            Thread.ofVirtual().name("logtail-" + ws.getId()).start(() -> tailLog(
                    ws, hostname, port, username, logFilePath, privateKeyFilePath, tailLines, timeoutMs));

        } catch (Exception e) {
            sendError(ws, e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession ws, @NonNull CloseStatus status) {
        disconnect(ws.getId());
    }

    // -------------------------------------------------------------------------

    private void tailLog(WebSocketSession ws,
                         String hostname, int port, String username,
                         String logFilePath, String privateKeyFilePath,
                         int tailLines, int timeoutMs) {
        Session sshSession = null;
        ChannelExec channel = null;
        try {
            JSch jsch = new JSch();
            jsch.addIdentity(privateKeyFilePath);

            sshSession = jsch.getSession(username, hostname, port);
            sshSession.setConfig("StrictHostKeyChecking", "no");
            sshSession.setConfig("PreferredAuthentications", "publickey");
            sshSession.connect(timeoutMs);

            sshSessions.put(ws.getId(), sshSession);

            send(ws, "connected", "Connected to " + hostname + " — tailing " + logFilePath);

            channel = (ChannelExec) sshSession.openChannel("exec");
            // Use single-quoted path to handle spaces; escape any single quotes in the path
            String safePath = logFilePath.replace("'", "'\"'\"'");
            channel.setCommand("tail -n " + tailLines + " -f '" + safePath + "'");
            channel.setErrStream(System.err);
            channel.connect(timeoutMs);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(channel.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null && ws.isOpen()) {
                    sendLine(ws, line);
                }
            }
        } catch (Exception e) {
            if (ws.isOpen()) sendError(ws, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        } finally {
            if (channel    != null && channel.isConnected())    channel.disconnect();
            if (sshSession != null && sshSession.isConnected()) sshSession.disconnect();
            sshSessions.remove(ws.getId());
        }
    }

    private void disconnect(String wsId) {
        Session s = sshSessions.remove(wsId);
        if (s != null && s.isConnected()) s.disconnect();
    }

    // -------------------------------------------------------------------------
    // Messaging helpers
    // -------------------------------------------------------------------------

    private void sendLine(WebSocketSession ws, String text) {
        sendJson(ws, Map.of("type", "line", "text", text));
    }

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
                if (ws.isOpen()) ws.sendMessage(new TextMessage((CharSequence) json));
            }
        } catch (Exception ignored) {}
    }

    private static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString().trim() : "";
    }
}
