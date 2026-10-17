package com.mycompany.batch.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.AppExecutionService;
import com.mycompany.batch.appcatalog.AppExecutionService.WebSocketTarget;
import com.mycompany.batch.appcatalog.LocalServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket endpoint at {@code /appcatalog/ws} — the relay behind a page's WEBSOCKET actions.
 *
 * <p>The page never connects to a WS app itself. It tells this server what to do, by instance, and
 * this server holds the real connection: the use case is resolved exactly as an HTTP call's request
 * is — environment URL, headers, variables, credentials — none of which a browser WebSocket could
 * send, and a {@code ws://} address on a host only this server can reach works the same as any other.
 *
 * <p>The page sends commands, each naming the connection by {@code key} (the instance id):
 * <pre>
 * {"op":"connect", "key":"…", "instanceId":"…", "environment":"uat", "inputs":{…}}
 * {"op":"send",    "key":"…", "message":"…"}
 * {"op":"pause" | "resume" | "stop" | "restart", "key":"…"}
 * </pre>
 * and is sent back:
 * <pre>
 * {"type":"status",  "key":"…", "state":"CONNECTING|OPEN|PAUSED|CLOSED|ERROR", "message":"…", "url":"…", "received":n, "skipped":n}
 * {"type":"message", "key":"…", "data":"…", "at":epochMillis}
 * </pre>
 *
 * <p>Pausing keeps the upstream connection open and drops what arrives on it until resumed; the
 * count dropped is reported, so a paused grid never pretends nothing happened. Every connection a
 * page opened is closed when that page's own socket closes — a tab closed mid-subscription leaves
 * nothing running here.
 */
@Component
public class AppWebSocketRelayHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(AppWebSocketRelayHandler.class);

    /** Headers the WebSocket client sets itself and refuses to be handed. */
    private static final Set<String> RESTRICTED_HEADERS = Set.of(
            "connection", "content-length", "expect", "host", "upgrade",
            "sec-websocket-accept", "sec-websocket-extensions", "sec-websocket-key", "sec-websocket-version");

    /** How long a page's socket may be backed up before it is given up on, and how much it may buffer. */
    private static final int SEND_TIME_LIMIT_MS = 10_000;
    private static final int BUFFER_SIZE_LIMIT  = 8 * 1024 * 1024;

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();

    private final AppExecutionService execution;
    private final LocalServerAddress localServer;
    private final ObjectMapper objectMapper;

    /** Per page socket: its thread-safe sender, and its connections by key. */
    private final Map<String, Page> pages = new ConcurrentHashMap<>();

    /** Every upstream connection by id, for the connections admin page; closed ones linger until pruned. */
    private final Map<String, Connection> registry = new ConcurrentHashMap<>();
    private static final int MAX_CLOSED_KEPT = 100;

    public AppWebSocketRelayHandler(AppExecutionService execution, LocalServerAddress localServer, ObjectMapper objectMapper) {
        this.execution = execution;
        this.localServer = localServer;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(@NonNull WebSocketSession session) {
        // Upstream listeners send from their own threads, and a raw session does not allow two
        // sends at once. Under a firehose the oldest buffered messages are dropped rather than the
        // page's socket being closed — the page is a view, and a view that falls behind should skip.
        pages.put(session.getId(), new Page(new ConcurrentWebSocketSessionDecorator(session,
                SEND_TIME_LIMIT_MS, BUFFER_SIZE_LIMIT, ConcurrentWebSocketSessionDecorator.OverflowStrategy.DROP)));
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession session, @NonNull CloseStatus status) {
        Page page = pages.remove(session.getId());
        if (page != null) page.connections.values().forEach(c -> c.close("page closed"));
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession session, @NonNull TextMessage message) {
        Page page = pages.get(session.getId());
        if (page == null) return;
        Map<String, Object> command;
        try {
            command = objectMapper.readValue(message.getPayload(), new TypeReference<>() {});
        } catch (Exception e) {
            page.send(Map.of("type", "status", "key", "", "state", "ERROR", "message", "Unreadable command: " + e.getMessage()));
            return;
        }
        String op  = String.valueOf(command.getOrDefault("op", "")).toLowerCase();
        String key = String.valueOf(command.getOrDefault("key", ""));
        if (key.isBlank()) {
            page.send(Map.of("type", "status", "key", "", "state", "ERROR", "message", "Command names no connection key"));
            return;
        }
        try {
            switch (op) {
                case "connect" -> {
                    ConnectRequest request = ConnectRequest.of(command);
                    page.lastRequests.put(key, request);
                    connect(page, key, request);
                }
                case "restart" -> {
                    ConnectRequest request = page.lastRequests.get(key);
                    if (request == null) throw new IllegalStateException("Nothing to restart — this connection has not been opened yet on this page");
                    connect(page, key, request);
                }
                case "send"   -> requireOpen(page, key).send(String.valueOf(command.getOrDefault("message", "")));
                case "pause"  -> requireOpen(page, key).pause(true);
                case "resume" -> requireOpen(page, key).pause(false);
                case "stop"   -> {
                    Connection c = page.connections.remove(key);
                    if (c == null) page.status(key, "CLOSED", "Not connected", null, 0, 0);
                    else c.close("stopped");
                }
                default -> throw new IllegalArgumentException("Unknown command: " + op);
            }
        } catch (Exception e) {
            page.status(key, "ERROR", rootMessage(e), null, 0, 0);
        }
    }

    private static Connection requireOpen(Page page, String key) {
        Connection c = page.connections.get(key);
        if (c == null) throw new IllegalStateException("Not connected — run the Connect action first");
        return c;
    }

    /** Closes whatever this key had open and opens it again from {@code request}. */
    private void connect(Page page, String key, ConnectRequest request) throws Exception {
        Connection previous = page.connections.remove(key);
        if (previous != null) previous.close("reconnecting");

        WebSocketTarget target = execution.prepareWebSocket(request.instanceId(), request.environment(), request.inputs());
        Connection connection = new Connection(page, key, target);
        page.connections.put(key, connection);
        registry.put(connection.id, connection);
        pruneClosed();
        connection.open();
    }

    private void pruneClosed() {
        List<Connection> closed = registry.values().stream().filter(c -> !c.live())
                .sorted(Comparator.comparingLong(c -> c.createdAt)).toList();
        for (int i = 0; i < closed.size() - MAX_CLOSED_KEPT; i++) registry.remove(closed.get(i).id);
    }

    /** Every connection this relay has open, and the most recent closed ones — newest first. */
    public List<Map<String, Object>> connections() {
        return registry.values().stream().sorted(Comparator.comparingLong((Connection c) -> c.createdAt).reversed())
                .map(Connection::describe).toList();
    }

    /** {@code pause}, {@code resume} or {@code disconnect} a connection from the admin page; the page it feeds is told. */
    public void control(String id, String op) {
        Connection c = registry.get(id);
        if (c == null) throw new IllegalArgumentException("No connection " + id);
        if (!c.live()) throw new IllegalStateException("Connection " + id + " is already " + c.state.toLowerCase());
        switch (op.toLowerCase()) {
            case "pause"      -> c.pause(true);
            case "resume"     -> c.pause(false);
            case "disconnect" -> { c.page.connections.remove(c.key, c); c.close("disconnected by admin"); }
            default -> throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    // -------------------------------------------------------------------------

    private record ConnectRequest(String instanceId, String environment, Map<String, Object> inputs) {
        @SuppressWarnings("unchecked")
        static ConnectRequest of(Map<String, Object> command) {
            Object id = command.get("instanceId");
            if (id == null || String.valueOf(id).isBlank()) throw new IllegalArgumentException("connect names no instance");
            Object env = command.get("environment");
            Object inputs = command.get("inputs");
            return new ConnectRequest(String.valueOf(id), env == null ? null : String.valueOf(env),
                    inputs instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
        }
    }

    /** One page socket: where its messages go, and what it has open. */
    private final class Page {
        final WebSocketSession out;
        final Map<String, Connection> connections = new ConcurrentHashMap<>();
        /** What each key last connected with, so Restart can reopen it after a Stop. */
        final Map<String, ConnectRequest> lastRequests = new ConcurrentHashMap<>();

        Page(WebSocketSession out) { this.out = out; }

        void send(Map<String, Object> payload) {
            if (!out.isOpen()) return;
            try {
                out.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
            } catch (Exception e) {
                log.debug("Could not relay to page socket {}: {}", out.getId(), e.getMessage());
            }
        }

        void status(String key, String state, String message, String url, long received, long skipped) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("type", "status");
            payload.put("key", key);
            payload.put("state", state);
            payload.put("message", message == null ? "" : message);
            if (url != null) payload.put("url", url);
            payload.put("received", received);
            payload.put("skipped", skipped);
            send(payload);
        }
    }

    /** One upstream subscription. */
    private final class Connection implements WebSocket.Listener {
        final Page page;
        final String key;
        final WebSocketTarget target;
        final AtomicBoolean paused = new AtomicBoolean();
        final AtomicBoolean closedByUs = new AtomicBoolean();
        final AtomicLong received = new AtomicLong();
        final AtomicLong skipped = new AtomicLong();
        final StringBuilder textFrames = new StringBuilder();
        final ByteArrayOutputStream binaryFrames = new ByteArrayOutputStream();
        final String id = UUID.randomUUID().toString();
        final long createdAt = System.currentTimeMillis();
        volatile long openedAt, lastReceivedAt, closedAt;
        volatile String state = "CONNECTING";
        volatile WebSocket socket;
        /** Sends chained one after another: the client allows only one outstanding at a time. */
        CompletableFuture<?> lastSend = CompletableFuture.completedFuture(null);

        Connection(Page page, String key, WebSocketTarget target) {
            this.page = page;
            this.key = key;
            this.target = target;
        }

        /** Whether this is still the connection the page has under this key — not one replaced by a restart. */
        boolean current() { return page.connections.get(key) == this; }

        void status(String state, String message) {
            if (live()) {
                this.state = state;
                if (!live()) closedAt = System.currentTimeMillis();
            }
            page.status(key, state, message, target.url(), received.get(), skipped.get());
        }

        boolean live() { return !"CLOSED".equals(state) && !"ERROR".equals(state); }

        Map<String, Object> describe() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("page", page.out.getId());
            m.put("key", key);
            m.put("url", target.url());
            m.put("type", target.url().regionMatches(true, 0, "wss:", 0, 4) ? "wss" : "ws");
            m.put("state", state);
            m.put("createdAt", createdAt);
            m.put("openedAt", openedAt == 0 ? null : openedAt);
            m.put("lastReceivedAt", lastReceivedAt == 0 ? null : lastReceivedAt);
            m.put("closedAt", closedAt == 0 ? null : closedAt);
            m.put("received", received.get());
            m.put("skipped", skipped.get());
            return m;
        }

        void open() {
            status("CONNECTING", "Connecting to " + target.url());
            URI uri = URI.create(target.url());
            // A wss:// address of this server itself needs the client that trusts its own certificate.
            String asHttps = target.url().replaceFirst("(?i)^wss://", "https://");
            HttpClient client = localServer.clientFor(asHttps, HTTP);

            WebSocket.Builder builder = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(Math.max(target.connectTimeoutMs(), 1000)));
            target.headers().forEach((name, value) -> {
                String lower = name.toLowerCase();
                if (lower.equals("sec-websocket-protocol")) {
                    String[] protocols = value.split("\\s*,\\s*");
                    if (protocols.length > 0 && !protocols[0].isBlank())
                        builder.subprotocols(protocols[0], Arrays.copyOfRange(protocols, 1, protocols.length));
                } else if (!RESTRICTED_HEADERS.contains(lower)) {
                    builder.header(name, value);
                }
            });

            builder.buildAsync(uri, this).whenComplete((ws, error) -> {
                if (error != null) {
                    if (current()) {
                        page.connections.remove(key, this);
                        status("ERROR", "Could not connect to " + target.url() + ": " + rootMessage(error));
                    }
                    return;
                }
                if (!current()) { ws.abort(); return; }
                socket = ws;
            });
        }

        void send(String message) {
            WebSocket ws = socket;
            if (ws == null) throw new IllegalStateException("Still connecting — try again once the connection is open");
            // A message typed on the page may use the use case's own variables, as its body does.
            String text = execution.substitute(message.isBlank() && target.openingMessage() != null
                    ? target.openingMessage() : message, target.variables());
            if (text == null || text.isBlank()) throw new IllegalArgumentException("Nothing to send — the message is empty");
            enqueue(ws, text);
        }

        private synchronized void enqueue(WebSocket ws, String text) {
            lastSend = lastSend.handle((r, e) -> null)
                    .thenCompose(x -> ws.sendText(text, true))
                    .whenComplete((r, e) -> {
                        if (e != null && current()) status("ERROR", "Send failed: " + rootMessage(e));
                    });
        }

        void pause(boolean pause) {
            paused.set(pause);
            status(pause ? "PAUSED" : "OPEN", pause
                    ? "Paused — the connection stays open, and messages arriving now are skipped"
                    : "Resumed" + (skipped.get() > 0 ? " — " + skipped.get() + " message(s) skipped while paused" : ""));
        }

        void close(String why) {
            closedByUs.set(true);
            WebSocket ws = socket;
            if (ws != null) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, why)
                        .orTimeout(2, TimeUnit.SECONDS)
                        .whenComplete((r, e) -> ws.abort());
            }
            status("CLOSED", "Closed (" + why + ")");
        }

        // ── WebSocket.Listener ──────────────────────────────────────────

        @Override
        public void onOpen(WebSocket ws) {
            socket = ws;
            openedAt = System.currentTimeMillis();
            ws.request(1);
            status("OPEN", "Connected to " + target.url()
                    + (target.openingMessage() != null ? " — opening message sent" : ""));
            if (target.openingMessage() != null) enqueue(ws, target.openingMessage());
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            textFrames.append(data);
            if (last) {
                String text = textFrames.toString();
                textFrames.setLength(0);
                deliver(text);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            binaryFrames.writeBytes(bytes);
            if (last) {
                String text = binaryFrames.toString(StandardCharsets.UTF_8);
                binaryFrames.reset();
                deliver(text);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            if (!closedByUs.get() && current()) {
                page.connections.remove(key, this);
                status("CLOSED", "Closed by the server (" + statusCode + (reason == null || reason.isBlank() ? "" : " " + reason) + ")");
            }
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            if (!closedByUs.get() && current()) {
                page.connections.remove(key, this);
                status("ERROR", rootMessage(error));
            }
        }

        private void deliver(String text) {
            if (!current()) return;
            received.incrementAndGet();
            lastReceivedAt = System.currentTimeMillis();
            if (paused.get()) { skipped.incrementAndGet(); return; }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("type", "message");
            payload.put("key", key);
            payload.put("data", text);
            payload.put("at", System.currentTimeMillis());
            page.send(payload);
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) t = t.getCause();
        String message = t.getMessage();
        return message == null || message.isBlank() ? t.getClass().getSimpleName() : message;
    }
}
