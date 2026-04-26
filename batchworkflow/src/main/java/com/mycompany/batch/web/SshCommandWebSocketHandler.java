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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket endpoint at {@code /ssh/ws}.
 *
 * <p>Protocol (client → server):
 * <pre>
 * {"type":"connect","hostname":"h","port":22,"username":"u","privateKeyFilePath":"/k","timeoutMs":5000}
 * {"type":"exec","command":"ls -la"}
 * {"type":"stop"}
 * </pre>
 *
 * <p>Protocol (server → client):
 * <pre>
 * {"type":"connected","message":"Connected to h as u"}
 * {"type":"line","stream":"stdout","text":"..."}
 * {"type":"done","exitCode":0,"stderr":""}
 * {"type":"error","message":"..."}
 * </pre>
 */
@Component
public class SshCommandWebSocketHandler extends TextWebSocketHandler {

    private final ObjectMapper objectMapper;
    private final Map<String, Session> sshSessions = new ConcurrentHashMap<>();

    public SshCommandWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession ws, @NonNull TextMessage message) {
        try {
            Map<String, Object> params = objectMapper.readValue(
                    message.getPayload(), new TypeReference<>() {});

            switch (str(params, "type")) {
                case "connect" -> connectSsh(ws, params);
                case "exec"    -> executeCommand(ws, str(params, "command"));
                case "stop"    -> { disconnect(ws.getId()); send(ws, "info", "Disconnected"); }
                default        -> sendError(ws, "Unknown message type: " + str(params, "type"));
            }
        } catch (Exception e) {
            sendError(ws, e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession ws, @NonNull CloseStatus status) {
        disconnect(ws.getId());
    }

    // -------------------------------------------------------------------------

    private void connectSsh(WebSocketSession ws, Map<String, Object> params) {
        String hostname           = str(params, "hostname");
        String username           = str(params, "username");
        String privateKeyFilePath = str(params, "privateKeyFilePath");
        int    port               = params.containsKey("port")
                ? ((Number) params.get("port")).intValue() : 22;
        int    timeoutMs          = params.containsKey("timeoutMs")
                ? ((Number) params.get("timeoutMs")).intValue() : 5000;

        if (hostname.isBlank() || username.isBlank() || privateKeyFilePath.isBlank()) {
            sendError(ws, "hostname, username and privateKeyFilePath are required");
            return;
        }

        disconnect(ws.getId());

        try {
            JSch jsch = new JSch();
            jsch.addIdentity(privateKeyFilePath);

            Session session = jsch.getSession(username, hostname, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey");
            session.connect(timeoutMs);

            sshSessions.put(ws.getId(), session);
            send(ws, "connected", "Connected to " + hostname + " as " + username);
        } catch (Exception e) {
            sendError(ws, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    private void executeCommand(WebSocketSession ws, String command) {
        if (command.isBlank()) { sendError(ws, "Command is empty"); return; }
        Session session = sshSessions.get(ws.getId());
        if (session == null || !session.isConnected()) {
            sendError(ws, "Not connected — please connect first");
            return;
        }
        Thread.ofVirtual().name("sshcmd-" + ws.getId()).start(() -> runCommand(ws, session, command));
    }

    private void runCommand(WebSocketSession ws, Session session, String command) {
        ChannelExec channel = null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);

            ByteArrayOutputStream stderrBuf = new ByteArrayOutputStream();
            channel.setErrStream(stderrBuf);

            InputStream stdout = channel.getInputStream();
            channel.connect();

            byte[] buf     = new byte[4096];
            StringBuilder lineBuf = new StringBuilder();

            while (true) {
                while (stdout.available() > 0) {
                    int n = stdout.read(buf, 0, Math.min(buf.length, stdout.available()));
                    if (n < 0) break;
                    flushLines(ws, new String(buf, 0, n), lineBuf);
                }
                if (channel.isClosed()) {
                    int rem;
                    while ((rem = stdout.available()) > 0) {
                        int n = stdout.read(buf, 0, Math.min(buf.length, rem));
                        if (n < 0) break;
                        flushLines(ws, new String(buf, 0, n), lineBuf);
                    }
                    break;
                }
                //noinspection BusyWait
                Thread.sleep(50);
            }

            if (!lineBuf.isEmpty()) sendLine(ws, lineBuf.toString());

            int    exitCode = channel.getExitStatus();
            String stderr   = stderrBuf.toString().trim();
            sendDone(ws, exitCode, stderr);

        } catch (Exception e) {
            if (ws.isOpen()) sendError(ws, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        } finally {
            if (channel != null && channel.isConnected()) channel.disconnect();
        }
    }

    private void flushLines(WebSocketSession ws, String chunk, StringBuilder lineBuf) {
        int start = 0;
        for (int i = 0; i < chunk.length(); i++) {
            if (chunk.charAt(i) == '\n') {
                lineBuf.append(chunk, start, i);
                sendLine(ws, lineBuf.toString());
                lineBuf.setLength(0);
                start = i + 1;
            }
        }
        if (start < chunk.length()) lineBuf.append(chunk, start, chunk.length());
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

    private void sendDone(WebSocketSession ws, int exitCode, String stderr) {
        sendJson(ws, Map.of("type", "done", "exitCode", exitCode, "stderr", stderr));
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
