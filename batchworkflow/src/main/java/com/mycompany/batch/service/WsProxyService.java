package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.model.WsProxyLogEntry;
import com.mycompany.batch.model.WsProxyMode;
import com.mycompany.batch.util.Threads;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hand-rolled WebSocket proxy engine.
 *
 * <p>Each started instance opens a genuine listening {@link ServerSocket} on the requested port
 * (Spring's embedded servlet container only serves WebSocket endpoints on the app's own port, so
 * arbitrary listen ports are handled here with a minimal RFC 6455 server: text frames only, with
 * fragmentation/ping/pong support but no compression extension). Outbound connections to targets
 * reuse Spring's {@link StandardWebSocketClient} (backed by the container's JSR-356 client, e.g.
 * Tomcat's), which already handles client-side masking correctly.
 *
 * <p>Every request/response is logged in-memory (bounded, most recent {@link #MAX_LOG} entries)
 * for the query panel in websocketproxy.html.
 */
@Service
public class WsProxyService {

    private static final String  WS_GUID       = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final long    TARGET_TIMEOUT_MS   = 15_000;
    private static final long    RECONNECT_INTERVAL_MS = 3_000;
    private static final int     MAX_LOG       = 5_000;

    private final ObjectMapper objectMapper;
    private final Map<Integer, ProxyInstance> instances = new ConcurrentHashMap<>();
    private final Deque<WsProxyLogEntry> log = new ConcurrentLinkedDeque<>();

    public WsProxyService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public Map<String, Object> start(int port, List<String> targets, WsProxyMode mode) throws IOException {
        if (instances.containsKey(port)) {
            throw new IllegalStateException("A proxy is already running on port " + port);
        }
        ProxyInstance inst = new ProxyInstance(port, targets, mode);
        instances.put(port, inst);
        try {
            inst.start();
        } catch (IOException e) {
            instances.remove(port);
            throw e;
        }
        return inst.describe();
    }

    public void stop(int port) {
        ProxyInstance inst = instances.remove(port);
        if (inst != null) inst.stop();
    }

    public List<Map<String, Object>> status() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (ProxyInstance inst : instances.values()) out.add(inst.describe());
        out.sort(Comparator.comparingInt(m -> (int) m.get("port")));
        return out;
    }

    public Object sendManual(int port, String payload) {
        ProxyInstance inst = instances.get(port);
        if (inst == null) throw new IllegalStateException("No proxy running on port " + port);
        return inst.processPayload("ui-manual-test (port " + port + ")", payload);
    }

    // -------------------------------------------------------------------------
    // Log query
    // -------------------------------------------------------------------------

    public List<WsProxyLogEntry> queryLog(Integer port, Integer limit, Integer sinceMinutes) {
        long cutoff = sinceMinutes != null ? System.currentTimeMillis() - sinceMinutes * 60_000L : -1;
        List<WsProxyLogEntry> matched = new ArrayList<>();
        for (WsProxyLogEntry e : log) {
            if (port != null && e.port != port) continue;
            if (cutoff >= 0 && e.timestamp < cutoff) continue;
            matched.add(e);
        }
        // newest first
        Collections.reverse(matched);
        if (limit != null && limit >= 0 && matched.size() > limit) {
            matched = matched.subList(0, limit);
        }
        return matched;
    }

    private void appendLog(WsProxyLogEntry entry) {
        log.addLast(entry);
        while (log.size() > MAX_LOG) log.pollFirst();
    }

    // -------------------------------------------------------------------------
    // One running proxy (one listen port)
    // -------------------------------------------------------------------------

    private class ProxyInstance {
        final int port;
        final WsProxyMode mode;
        final List<TargetLink> targets = new CopyOnWriteArrayList<>();
        final Map<String, ClientConn> clients = new ConcurrentHashMap<>();
        final Map<String, CompletableFuture<Map<String, Object>>> pending = new ConcurrentHashMap<>();
        final AtomicInteger rrCounter = new AtomicInteger();
        final AtomicInteger clientSeq = new AtomicInteger();
        volatile boolean running = true;
        ServerSocket serverSocket;
        final StandardWebSocketClient wsClient = new StandardWebSocketClient();

        ProxyInstance(int port, List<String> targetUrls, WsProxyMode mode) {
            this.port = port;
            this.mode = mode;
            for (String url : targetUrls) targets.add(new TargetLink(url));
        }

        void start() throws IOException {
            serverSocket = new ServerSocket(port);
            Threads.startDaemon("wsproxy-accept-" + port, this::acceptLoop);
            for (TargetLink t : targets) {
                Threads.startDaemon("wsproxy-target-" + port + "-" + t.url.hashCode(), () -> connectLoop(t));
            }
        }

        void stop() {
            running = false;
            try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
            for (ClientConn c : clients.values()) c.close();
            clients.clear();
            for (TargetLink t : targets) {
                try { if (t.session != null && t.session.isOpen()) t.session.close(); } catch (Exception ignored) {}
            }
            for (CompletableFuture<Map<String, Object>> f : pending.values()) f.cancel(false);
            pending.clear();
        }

        Map<String, Object> describe() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("port", port);
            m.put("mode", mode.name());
            m.put("clientCount", clients.size());
            List<Map<String, Object>> tgt = new ArrayList<>();
            for (TargetLink t : targets) {
                Map<String, Object> tm = new LinkedHashMap<>();
                tm.put("url", t.url);
                tm.put("connected", t.connected);
                tm.put("lastError", t.lastError);
                tgt.add(tm);
            }
            m.put("targets", tgt);
            return m;
        }

        // -- outbound target connections -----------------------------------

        void connectLoop(TargetLink t) {
            while (running) {
                if (!t.connected) {
                    try {
                        WebSocketHandler handler = new TargetHandler(t);
                        WebSocketSession session = wsClient.execute(handler, t.url).get(5, TimeUnit.SECONDS);
                        t.session = session;
                        t.connected = true;
                        t.lastError = null;
                    } catch (Exception e) {
                        t.connected = false;
                        t.lastError = rootMessage(e);
                    }
                }
                try { Thread.sleep(RECONNECT_INTERVAL_MS); } catch (InterruptedException ignored) { return; }
            }
        }

        class TargetHandler extends TextWebSocketHandler {
            final TargetLink link;
            TargetHandler(TargetLink link) { this.link = link; }

            @Override
            protected void handleTextMessage(@NonNull WebSocketSession session, @NonNull TextMessage message) {
                Map<String, Object> resp = null;
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parsed = objectMapper.readValue(message.getPayload(), Map.class);
                    resp = parsed;
                } catch (Exception ignored) {
                    // target didn't reply with a JSON object at all — handled below
                }

                Object uuid = resp != null ? resp.get("uuid") : null;
                if (uuid != null) {
                    CompletableFuture<Map<String, Object>> fut = pending.remove(uuid.toString());
                    if (fut != null) {
                        fut.complete(resp);
                        return;
                    }
                }

                // Target replied in a shape we don't recognize (missing/foreign envelope, not
                // JSON at all, or a uuid we have no pending request for) — there's nothing to
                // correlate it to. Rather than drop it, repackage it into our own envelope
                // format (uuid "NA") and fan it out to every client currently on this port.
                Object payload = resp != null ? resp : tryParseJson(message.getPayload());
                Map<String, Object> env = envelope("NA", System.currentTimeMillis(), link.url,
                        "clients (port " + port + ")", payload, "success",
                        "Unmatched message from target — relayed as-is", null);
                logFromEnvelope(env, null);
                broadcastToClients(env);
            }

            @Override
            public void afterConnectionClosed(@NonNull WebSocketSession session, @NonNull CloseStatus status) {
                link.connected = false;
                link.session = null;
                link.lastError = "Connection closed: " + status;
            }

            @Override
            public void handleTransportError(@NonNull WebSocketSession session, @NonNull Throwable exception) {
                link.connected = false;
                link.session = null;
                link.lastError = rootMessage(exception);
            }
        }

        TargetLink nextActiveTarget() {
            List<TargetLink> active = new ArrayList<>();
            for (TargetLink t : targets) if (t.connected) active.add(t);
            if (active.isEmpty()) return null;
            int idx = Math.floorMod(rrCounter.getAndIncrement(), active.size());
            return active.get(idx);
        }

        // -- inbound client connections --------------------------------------

        void acceptLoop() {
            while (running) {
                Socket socket;
                try {
                    socket = serverSocket.accept();
                } catch (IOException e) {
                    if (running) { /* transient accept error; keep looping until stop() closes the socket */ }
                    continue;
                }
                if (!running) { try { socket.close(); } catch (IOException ignored) {} break; }
                Threads.startDaemon("wsproxy-client-" + port + "-" + clientSeq.incrementAndGet(), () -> handleClientSocket(socket));
            }
        }

        void handleClientSocket(Socket socket) {
            String remote = String.valueOf(socket.getRemoteSocketAddress());
            try {
                InputStream in = socket.getInputStream();
                if (!performHandshake(in, socket.getOutputStream())) { socket.close(); return; }

                ClientConn conn = new ClientConn(socket);
                String id = remote + "#" + clientSeq.get();
                clients.put(id, conn);

                try {
                    ByteArrayOutputStream messageBuf = new ByteArrayOutputStream();
                    Frame frame;
                    while (running && (frame = readFrame(in)) != null) {
                        switch (frame.opcode) {
                            case 0x8 -> { writeFrame(conn.out, 0x8, new byte[0]); return; } // close
                            case 0x9 -> writeFrame(conn.out, 0xA, frame.payload); // ping -> pong
                            case 0xA -> { /* pong: ignore */ }
                            case 0x1, 0x0 -> {
                                messageBuf.write(frame.payload);
                                if (frame.fin) {
                                    String text = messageBuf.toString(StandardCharsets.UTF_8);
                                    messageBuf.reset();
                                    Object result = processPayload(remote + " (port " + port + ")", text);
                                    writeFrame(conn.out, 0x1, toJsonBytes(result));
                                }
                            }
                            default -> { /* binary frames not supported */ }
                        }
                    }
                } finally {
                    clients.remove(id);
                    conn.close();
                }
            } catch (Exception ignored) {
                try { socket.close(); } catch (IOException ignored2) {}
            }
        }

        void broadcastToClients(Map<String, Object> env) {
            byte[] bytes = toJsonBytes(env);
            for (ClientConn conn : clients.values()) {
                try {
                    writeFrame(conn.out, 0x1, bytes);
                } catch (IOException ignored) {
                    // connection likely dead; the client's read loop will notice and clean up
                }
            }
        }

        byte[] toJsonBytes(Object o) {
            try {
                return objectMapper.writeValueAsBytes(o);
            } catch (Exception e) {
                return ("{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}").getBytes(StandardCharsets.UTF_8);
            }
        }

        // -- mode dispatch: builds/sends the envelope, returns the response to relay --

        Object processPayload(String source, String rawPayload) {
            Object payload = tryParseJson(rawPayload);
            String uuid = UUID.randomUUID().toString();
            long ts = System.currentTimeMillis();

            return switch (mode) {
                case REFLECT -> {
                    WsProxyLogEntry e = new WsProxyLogEntry();
                    e.uuid = uuid; e.timestamp = ts; e.port = port; e.mode = mode.name();
                    e.source = source; e.target = "none (REFLECT mode)";
                    e.requestPayload = payload; e.responsePayload = payload;
                    e.status = "success"; e.message = "Reflected — not proxied"; e.timeTakenMs = 0L;
                    appendLog(e);
                    yield envelope(uuid, ts, source, e.target, payload, "success", e.message, 0L);
                }
                case PROXY -> {
                    TargetLink target = nextActiveTarget();
                    yield sendToTarget(uuid, ts, source, target, payload);
                }
                case BROADCAST -> {
                    if (targets.isEmpty()) {
                        Map<String, Object> env = envelope(uuid, ts, source, "none", payload, "error", "No targets configured", null);
                        logFromEnvelope(env, payload);
                        yield List.of(env);
                    }
                    long deadline = ts + TARGET_TIMEOUT_MS;
                    List<Map<String, Object>> results = new ArrayList<>();
                    List<Object[]> awaiting = new ArrayList<>(); // [uuid, target, future]
                    for (TargetLink t : targets) {
                        String u2 = UUID.randomUUID().toString();
                        if (!t.connected) {
                            Map<String, Object> env = envelope(u2, ts, source, t.url, payload, "error", "Target not connected: " + t.lastError, null);
                            logFromEnvelope(env, payload);
                            results.add(env);
                            continue;
                        }
                        CompletableFuture<Map<String, Object>> fut = new CompletableFuture<>();
                        pending.put(u2, fut);
                        Map<String, Object> outEnv = envelope(u2, ts, source, t.url, payload, "success", "", null);
                        if (!send(t, outEnv)) {
                            pending.remove(u2);
                            Map<String, Object> env = envelope(u2, ts, source, t.url, payload, "error", "Failed to send to target", null);
                            logFromEnvelope(env, payload);
                            results.add(env);
                            continue;
                        }
                        awaiting.add(new Object[]{u2, t, fut});
                    }
                    for (Object[] a : awaiting) {
                        String u2 = (String) a[0];
                        TargetLink t = (TargetLink) a[1];
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Map<String, Object>> fut = (CompletableFuture<Map<String, Object>>) a[2];
                        long remaining = Math.max(0, deadline - System.currentTimeMillis());
                        try {
                            Map<String, Object> resp = fut.get(remaining, TimeUnit.MILLISECONDS);
                            resp.put("timeTakenMs", System.currentTimeMillis() - ts);
                            logFromEnvelope(resp, payload);
                            results.add(resp);
                        } catch (Exception e) {
                            pending.remove(u2);
                            Map<String, Object> env = envelope(u2, ts, source, t.url, payload, "error", "Timeout/error waiting for target response: " + rootMessage(e), null);
                            logFromEnvelope(env, payload);
                            results.add(env);
                        }
                    }
                    yield results;
                }
            };
        }

        Map<String, Object> sendToTarget(String uuid, long ts, String source, TargetLink target, Object payload) {
            if (target == null) {
                Map<String, Object> env = envelope(uuid, ts, source, "none", payload, "error", "No active target available", null);
                logFromEnvelope(env, payload);
                return env;
            }
            Map<String, Object> outEnv = envelope(uuid, ts, source, target.url, payload, "success", "", null);
            CompletableFuture<Map<String, Object>> fut = new CompletableFuture<>();
            pending.put(uuid, fut);
            if (!send(target, outEnv)) {
                pending.remove(uuid);
                Map<String, Object> env = envelope(uuid, ts, source, target.url, payload, "error", "Failed to send to target", null);
                logFromEnvelope(env, payload);
                return env;
            }
            try {
                Map<String, Object> resp = fut.get(TARGET_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                resp.put("timeTakenMs", System.currentTimeMillis() - ts);
                logFromEnvelope(resp, payload);
                return resp;
            } catch (Exception e) {
                pending.remove(uuid);
                Map<String, Object> env = envelope(uuid, ts, source, target.url, payload, "error", "Timeout/error waiting for target response: " + rootMessage(e), null);
                logFromEnvelope(env, payload);
                return env;
            }
        }

        boolean send(TargetLink target, Map<String, Object> envelope) {
            try {
                target.session.sendMessage(new TextMessage(objectMapper.writeValueAsBytes(envelope)));
                return true;
            } catch (Exception e) {
                target.lastError = rootMessage(e);
                return false;
            }
        }

        void logFromEnvelope(Map<String, Object> resp, Object requestPayload) {
            WsProxyLogEntry e = new WsProxyLogEntry();
            e.uuid = String.valueOf(resp.get("uuid"));
            e.timestamp = resp.get("timestamp") instanceof Number n ? n.longValue() : System.currentTimeMillis();
            e.port = port;
            e.mode = mode.name();
            e.source = String.valueOf(resp.get("source"));
            e.target = String.valueOf(resp.get("target"));
            e.requestPayload = requestPayload;
            e.responsePayload = resp.get("payload");
            e.status = String.valueOf(resp.get("status"));
            e.message = String.valueOf(resp.get("message"));
            Object tt = resp.get("timeTakenMs");
            e.timeTakenMs = tt instanceof Number n ? n.longValue() : null;
            appendLog(e);
        }
    }

    private Map<String, Object> envelope(String uuid, long ts, String source, String target, Object payload,
                                          String status, String message, Long timeTakenMs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("uuid", uuid);
        m.put("timestamp", ts);
        m.put("source", source);
        m.put("target", target);
        m.put("payload", payload);
        m.put("status", status);
        m.put("message", message);
        if (timeTakenMs != null) m.put("timeTakenMs", timeTakenMs);
        return m;
    }

    private Object tryParseJson(String raw) {
        try {
            return objectMapper.readValue(raw, Object.class);
        } catch (Exception e) {
            return raw;
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null) r = r.getCause();
        String m = r.getMessage();
        return m != null ? m : r.getClass().getSimpleName();
    }

    // -------------------------------------------------------------------------
    // Per-target outbound link state
    // -------------------------------------------------------------------------

    private static class TargetLink {
        final String url;
        volatile WebSocketSession session;
        volatile boolean connected = false;
        volatile String lastError;
        TargetLink(String url) { this.url = url; }
    }

    // -------------------------------------------------------------------------
    // Per-client inbound connection state
    // -------------------------------------------------------------------------

    private static class ClientConn {
        final Socket socket;
        final OutputStream out;
        ClientConn(Socket socket) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
        }
        void close() {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Minimal RFC 6455 handshake + framing (text frames, fragmentation, ping/pong, close)
    // -------------------------------------------------------------------------

    private static boolean performHandshake(InputStream in, OutputStream out) throws IOException {
        String line;
        String key = null;
        boolean sawGet = false;
        while ((line = readHeaderLine(in)) != null && !line.isEmpty()) {
            if (line.startsWith("GET ")) sawGet = true;
            int colon = line.indexOf(':');
            if (colon > 0) {
                String name = line.substring(0, colon).trim();
                String value = line.substring(colon + 1).trim();
                if (name.equalsIgnoreCase("Sec-WebSocket-Key")) key = value;
            }
        }
        if (!sawGet || key == null) return false;

        String accept;
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            accept = Base64.getEncoder().encodeToString(sha1.digest((key + WS_GUID).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            return false;
        }

        String response = "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
        out.write(response.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        return true;
    }

    private static String readHeaderLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        boolean sawCr = false;
        while ((b = in.read()) != -1) {
            if (b == '\r') { sawCr = true; continue; }
            if (b == '\n' && sawCr) break;
            sawCr = false;
            buf.write(b);
        }
        if (b == -1 && buf.size() == 0) return null;
        return buf.toString(StandardCharsets.US_ASCII);
    }

    private record Frame(boolean fin, int opcode, byte[] payload) {}

    private static Frame readFrame(InputStream in) throws IOException {
        int b1 = in.read();
        if (b1 == -1) return null;
        int b2 = in.read();
        if (b2 == -1) return null;

        boolean fin = (b1 & 0x80) != 0;
        int opcode = b1 & 0x0F;
        boolean masked = (b2 & 0x80) != 0;
        long len = b2 & 0x7F;

        if (len == 126) {
            len = ((long) readByte(in) << 8) | readByte(in);
        } else if (len == 127) {
            len = 0;
            for (int i = 0; i < 8; i++) len = (len << 8) | readByte(in);
        }
        if (len > 16 * 1024 * 1024) throw new IOException("Frame too large: " + len);

        byte[] mask = new byte[4];
        if (masked) {
            if (in.readNBytes(mask, 0, 4) != 4) throw new EOFException();
        }

        byte[] payload = new byte[(int) len];
        int read = 0;
        while (read < payload.length) {
            int n = in.read(payload, read, payload.length - read);
            if (n == -1) throw new EOFException();
            read += n;
        }
        if (masked) {
            for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
        }
        return new Frame(fin, opcode, payload);
    }

    private static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b == -1) throw new EOFException();
        return b;
    }

    private static void writeFrame(OutputStream out, int opcode, byte[] payload) throws IOException {
        synchronized (out) {
            out.write(0x80 | (opcode & 0x0F));
            int len = payload.length;
            if (len < 126) {
                out.write(len);
            } else if (len <= 0xFFFF) {
                out.write(126);
                out.write((len >> 8) & 0xFF);
                out.write(len & 0xFF);
            } else {
                out.write(127);
                for (int i = 7; i >= 0; i--) out.write((int) ((long) len >> (8 * i)) & 0xFF);
            }
            out.write(payload);
            out.flush();
        }
    }
}
