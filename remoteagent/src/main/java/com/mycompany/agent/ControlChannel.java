package com.mycompany.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Dials out to the batchworkflow server's {@code /agent/ws} endpoint and stays connected,
 * reconnecting with exponential backoff on any drop. Once registered, it dispatches incoming
 * {@code exec} commands to a {@link CommandExecutor} and streams the results back.
 *
 * <p>The agent always initiates the connection so the remote host needs no inbound firewall
 * rule — only outbound access to the server.
 */
public class ControlChannel {

    private static final int MAX_BACKOFF_MS = 30_000;

    private final URI               serverUri;
    private final String            agentId;
    private final String            hostname;
    private final String            token;
    private final ObjectMapper      objectMapper = new ObjectMapper();
    private final CommandExecutor   executor     = new CommandExecutor();
    private final HttpClient        httpClient   = HttpClient.newHttpClient();

    private volatile WebSocket webSocket;
    private volatile boolean   running = true;

    public ControlChannel(String serverUrl, String agentId, String hostname, String token) {
        this.serverUri = URI.create(serverUrl);
        this.agentId   = agentId;
        this.hostname  = hostname;
        this.token     = token;
    }

    public void start() {
        Thread connectLoop = new Thread(this::runLoop, "control-channel");
        connectLoop.setDaemon(true);
        connectLoop.start();

        Thread heartbeat = new Thread(this::heartbeatLoop, "heartbeat");
        heartbeat.setDaemon(true);
        heartbeat.start();
    }

    public boolean isConnected() {
        WebSocket ws = webSocket;
        return ws != null && !ws.isOutputClosed();
    }

    // -------------------------------------------------------------------------

    private void runLoop() {
        long backoffMs = 1000;
        while (running) {
            try {
                connectAndWaitUntilClosed();
                backoffMs = 1000; // clean cycle completed — reset backoff before retrying
            } catch (Exception e) {
                System.err.println("[agent] connection attempt failed: " + e.getMessage());
            }
            sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    private void connectAndWaitUntilClosed() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        StringBuilder  buffer = new StringBuilder();

        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket ws) {
                webSocket = ws;
                System.out.println("[agent] connected to " + serverUri);
                sendRegister(ws);
                ws.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                buffer.append(data);
                ws.request(1);
                if (last) {
                    String msg = buffer.toString();
                    buffer.setLength(0);
                    handleMessage(msg);
                }
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
                System.out.println("[agent] disconnected (" + statusCode + " " + reason + "), will retry");
                closed.countDown();
                return null;
            }

            @Override
            public void onError(WebSocket ws, Throwable error) {
                System.err.println("[agent] websocket error: " + error.getMessage());
                closed.countDown();
            }
        };

        webSocket = httpClient.newWebSocketBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .buildAsync(serverUri, listener)
                .get(15, TimeUnit.SECONDS);

        closed.await();
    }

    private void sendRegister(WebSocket ws) {
        Map<String, Object> register = new LinkedHashMap<>();
        register.put("type", "register");
        register.put("agentId", agentId);
        register.put("hostname", hostname);
        register.put("token", token);
        register.put("pid", ProcessHandle.current().pid());
        register.put("os", System.getProperty("os.name"));
        sendJson(ws, register);
    }

    private void heartbeatLoop() {
        while (running) {
            sleep(15_000);
            WebSocket ws = webSocket;
            if (ws != null && !ws.isOutputClosed()) {
                sendJson(ws, Map.of("type", "heartbeat"));
            }
        }
    }

    private void handleMessage(String json) {
        try {
            Map<String, Object> msg = objectMapper.readValue(json, new TypeReference<>() {});
            if ("exec".equals(msg.get("type"))) {
                String requestId = String.valueOf(msg.get("requestId"));
                String command   = String.valueOf(msg.get("command"));
                executor.execute(command,
                        line -> sendJson(webSocket, Map.of("type", "line", "requestId", requestId, "text", line)),
                        exitCode -> sendJson(webSocket, Map.of("type", "done", "requestId", requestId, "exitCode", exitCode)));
            } else if ("error".equals(msg.get("type"))) {
                System.err.println("[agent] server error: " + msg.get("message"));
            }
        } catch (Exception e) {
            System.err.println("[agent] failed to parse server message: " + e.getMessage());
        }
    }

    private void sendJson(WebSocket ws, Object payload) {
        if (ws == null) return;
        try {
            ws.sendText(objectMapper.writeValueAsString(payload), true);
        } catch (Exception ignored) {}
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
