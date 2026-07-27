package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.mycompany.batch.util.Threads;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
 * {"type":"line","text":"..."}
 * {"type":"done","exitCode":0}
 * {"type":"error","message":"..."}
 * {"type":"system","message":"..."}
 * </pre>
 */
@Component
public class SshCommandWebSocketHandler extends TextWebSocketHandler {

    // Unique sentinel unlikely to appear in normal command output
    private static final String SENTINEL = "----SSH_CMD_DONE----";

    private final ObjectMapper objectMapper;

    // Persistent shell state per WebSocket connection
    private record SshState(Session sshSession, Channel shellChannel, OutputStream shellStdin) {}
    private final Map<String, SshState> sessions = new ConcurrentHashMap<>();

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

            // Open a persistent shell that reads commands from stdin.
            // bash --norc --noprofile avoids sourcing startup files that might print output.
            // 2>&1 merges stderr into stdout so the reader thread sees everything on one stream.
            ChannelExec shell = (ChannelExec) session.openChannel("exec");
            shell.setCommand("bash --norc --noprofile -s 2>&1");

            OutputStream stdin  = shell.getOutputStream();
            InputStream  stdout = shell.getInputStream();
            shell.connect();

            sessions.put(ws.getId(), new SshState(session, shell, stdin));
            send(ws, "connected", "Connected to " + hostname + " as " + username);

            // Dedicated reader thread streams shell output back to the client
            Threads.startDaemon("ssh-reader-" + ws.getId(), () -> readShellOutput(ws, stdout));

        } catch (Exception e) {
            sendError(ws, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    private void executeCommand(WebSocketSession ws, String command) {
        if (command.isBlank()) { sendError(ws, "Command is empty"); return; }
        SshState state = sessions.get(ws.getId());
        if (state == null || !state.sshSession().isConnected()) {
            sendError(ws, "Not connected — please connect first");
            return;
        }
        try {
            // Write the user's command, then a sentinel that captures its exit code.
            // The sentinel line is the only way to know when the command finished.
            String wrapped = command + "\necho \"" + SENTINEL + ":$?\"\n";
            state.shellStdin().write(wrapped.getBytes(StandardCharsets.UTF_8));
            state.shellStdin().flush();
        } catch (Exception e) {
            sendError(ws, "Failed to send command: " + e.getMessage());
        }
    }

    private void readShellOutput(WebSocketSession ws, InputStream stdout) {
        try {
            byte[]        buf     = new byte[4096];
            StringBuilder lineBuf = new StringBuilder();
            int n;
            while ((n = stdout.read(buf)) != -1) {
                String chunk = new String(buf, 0, n, StandardCharsets.UTF_8);
                int start = 0;
                for (int i = 0; i < chunk.length(); i++) {
                    if (chunk.charAt(i) == '\n') {
                        lineBuf.append(chunk, start, i);
                        dispatch(ws, lineBuf.toString().stripTrailing());
                        lineBuf.setLength(0);
                        start = i + 1;
                    }
                }
                if (start < chunk.length()) lineBuf.append(chunk, start, chunk.length());
            }
            if (!lineBuf.isEmpty()) dispatch(ws, lineBuf.toString().stripTrailing());
        } catch (Exception ignored) {
        } finally {
            if (ws.isOpen()) sendJson(ws, Map.of("type", "system", "message", "Shell session ended"));
        }
    }

    private void dispatch(WebSocketSession ws, String line) {
        if (line.startsWith(SENTINEL + ":")) {
            int exitCode;
            try {
                exitCode = Integer.parseInt(line.substring(SENTINEL.length() + 1).trim());
            } catch (NumberFormatException e) {
                exitCode = -1;
            }
            sendJson(ws, Map.of("type", "done", "exitCode", exitCode));
        } else {
            sendLine(ws, line);
        }
    }

    private void disconnect(String wsId) {
        SshState state = sessions.remove(wsId);
        if (state == null) return;
        try { state.shellStdin().close(); } catch (Exception ignored) {}
        if (state.shellChannel().isConnected()) state.shellChannel().disconnect();
        if (state.sshSession().isConnected())   state.sshSession().disconnect();
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
                if (ws.isOpen()) ws.sendMessage(new TextMessage(json));
            }
        } catch (Exception ignored) {}
    }

    private static String str(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString().trim() : "";
    }
}
