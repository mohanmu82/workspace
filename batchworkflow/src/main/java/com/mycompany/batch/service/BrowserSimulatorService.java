package com.mycompany.batch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.util.Threads;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Launches a real, local headless Chromium/Chrome/Edge process for a given URL and reports a
 * memory breakdown via the Chrome DevTools Protocol (CDP) — no Selenium/Playwright dependency
 * needed. The CDP connection itself reuses Spring's {@link StandardWebSocketClient} (same class
 * {@link WsProxyService} already uses for its outbound target connections), with a numeric
 * request-id -> {@link CompletableFuture} correlation map mirroring {@code WsProxyService}'s
 * uuid-keyed {@code pending} map.
 *
 * <p>Each {@link #start} call launches one dedicated browser process (its own {@code --user-data-dir}
 * and ephemeral {@code --remote-debugging-port}), navigates it to the requested URL, waits for the
 * page load event plus a short settle delay, then captures {@code Performance.getMetrics} and
 * {@code Runtime.getHeapUsage}. {@link #refresh} re-captures on the already-loaded page so memory
 * growth over time can be observed.
 */
@Service
public class BrowserSimulatorService {

    private static final long CDP_HTTP_READY_TIMEOUT_MS = 15_000;
    private static final long CDP_COMMAND_TIMEOUT_SEC = 10;
    private static final long PAGE_LOAD_TIMEOUT_SEC = 30;
    private static final int  DEFAULT_SETTLE_MS = 1500;
    private static final int  MAX_SETTLE_MS = 15_000;

    private final ObjectMapper objectMapper;
    private final StandardWebSocketClient cdpClient;
    private final Map<String, SimSession> sessions = new ConcurrentHashMap<>();

    public BrowserSimulatorService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        // CDP screenshot responses are a base64 PNG in one text frame — the JSR-356 container's
        // default 8KB buffer truncates/drops anything past a trivial page, so it must be widened
        // before any session connects.
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxTextMessageBufferSize(50 * 1024 * 1024);
        container.setDefaultMaxBinaryMessageBufferSize(50 * 1024 * 1024);
        this.cdpClient = new StandardWebSocketClient(container);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public String start(String url, Integer settleMs) {
        String id = UUID.randomUUID().toString().substring(0, 8);
        SimSession s = new SimSession(id, url);
        sessions.put(id, s);
        int settle = settleMs != null ? Math.max(0, Math.min(settleMs, MAX_SETTLE_MS)) : DEFAULT_SETTLE_MS;
        Threads.startDaemon("browsersim-" + id, () -> launchAndLoad(s, settle));
        return id;
    }

    public boolean stop(String id) {
        SimSession s = sessions.remove(id);
        if (s == null) return false;
        cleanup(s, "STOPPED");
        return true;
    }

    public List<Map<String, Object>> status() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (SimSession s : sessions.values()) out.add(describe(s, false));
        out.sort(Comparator.comparingLong(m -> (long) m.get("createdAt")));
        return out;
    }

    public Map<String, Object> describeOne(String id) {
        SimSession s = sessions.get(id);
        if (s == null) throw new IllegalStateException("No session " + id);
        return describe(s, true);
    }

    public Map<String, Object> refresh(String id) throws Exception {
        SimSession s = sessions.get(id);
        if (s == null) throw new IllegalStateException("No session " + id);
        if (!"READY".equals(s.status)) throw new IllegalStateException("Session not ready: " + s.status);
        captureMetrics(s);
        return describe(s, true);
    }

    // -------------------------------------------------------------------------
    // Launch + navigate + capture (runs on a daemon thread)
    // -------------------------------------------------------------------------

    private void launchAndLoad(SimSession s, int settleMs) {
        try {
            s.status = "LAUNCHING";
            String chromePath = findChromeExecutable();
            int port = findFreePort();
            Path userDataDir = Files.createTempDirectory("browsersim-" + s.id + "-");
            s.userDataDir = userDataDir;

            List<String> cmd = new ArrayList<>(List.of(chromePath,
                    "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
                    "--disable-extensions", "--hide-scrollbars", "--mute-audio",
                    // Container /dev/shm is commonly capped at 64MB, which crashes Chrome's shared-memory
                    // renderer transport; falling back to disk is slower but harmless outside containers.
                    "--disable-dev-shm-usage",
                    "--remote-debugging-port=" + port,
                    "--user-data-dir=" + userDataDir.toAbsolutePath(),
                    "--window-size=1280,800"));
            // Off by default: Chrome's sandbox refuses to start when the JVM runs as root (common in
            // Docker). Only opt in if you've confirmed the deployment needs it — it removes a layer of
            // isolation against a malicious page, and this tool navigates to arbitrary URLs.
            boolean noSandbox = Boolean.parseBoolean(
                    System.getProperty("browsersimulator.chrome.noSandbox", System.getenv("CHROME_NO_SANDBOX")));
            if (noSandbox) cmd.add("--no-sandbox");
            cmd.add("about:blank");
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            s.chromeProcess = pb.start();
            s.debugPort = port;

            waitForCdpHttp(port);
            String wsUrl = fetchPageWebSocketUrl(port);

            s.cdpSession = cdpClient.execute(new CdpHandler(s), wsUrl).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);

            sendCommand(s, "Page.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            sendCommand(s, "Runtime.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            sendCommand(s, "Performance.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);

            s.status = "LOADING";
            CompletableFuture<Void> loadWaiter = new CompletableFuture<>();
            s.loadWaiter = loadWaiter;
            Map<String, Object> navResult = sendCommand(s, "Page.navigate", Map.of("url", s.url))
                    .get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            Object errText = navResult.get("errorText");
            if (errText != null) {
                throw new IOException("Navigation failed: " + errText);
            }
            loadWaiter.get(PAGE_LOAD_TIMEOUT_SEC, TimeUnit.SECONDS);
            s.loadWaiter = null;

            Thread.sleep(settleMs);

            captureMetrics(s);
            s.status = "READY";
        } catch (Exception e) {
            s.status = "ERROR";
            s.error = rootMessage(e);
            cleanup(s, "ERROR");
        }
    }

    private void captureMetrics(SimSession s) throws Exception {
        Map<String, Object> perf = sendCommand(s, "Performance.getMetrics", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> list = (List<Map<String, Object>>) perf.getOrDefault("metrics", List.of());
        Map<String, Double> flat = new LinkedHashMap<>();
        for (Map<String, Object> m : list) {
            Object name = m.get("name");
            Object value = m.get("value");
            if (name != null && value instanceof Number n) flat.put(String.valueOf(name), n.doubleValue());
        }

        Map<String, Object> heap;
        try {
            heap = sendCommand(s, "Runtime.getHeapUsage", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            heap = Map.of(); // older browser builds may not support this command
        }

        String screenshot = null;
        try {
            Map<String, Object> shot = sendCommand(s, "Page.captureScreenshot", Map.of("format", "png"))
                    .get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            Object data = shot.get("data");
            screenshot = data != null ? data.toString() : null;
        } catch (Exception ignored) {
            // screenshot is a nice-to-have; metrics are still valid without it
        }

        s.lastMetrics = buildBreakdown(flat, heap);
        s.lastScreenshotBase64 = screenshot;
        s.lastCapturedAt = System.currentTimeMillis();
    }

    private Map<String, Object> buildBreakdown(Map<String, Double> flat, Map<String, Object> heap) {
        Map<String, Object> jsHeap = new LinkedHashMap<>();
        jsHeap.put("usedBytes", flat.get("JSHeapUsedSize"));
        jsHeap.put("totalBytes", flat.get("JSHeapTotalSize"));

        Map<String, Object> runtimeHeap = new LinkedHashMap<>();
        runtimeHeap.put("usedBytes", numOrNull(heap.get("usedSize")));
        runtimeHeap.put("totalBytes", numOrNull(heap.get("totalSize")));
        runtimeHeap.put("embedderHeapUsedBytes", numOrNull(heap.get("embedderHeapUsedSize")));
        runtimeHeap.put("backingStorageBytes", numOrNull(heap.get("backingStorageSize")));

        Map<String, Object> domCounters = new LinkedHashMap<>();
        domCounters.put("documents", flat.get("Documents"));
        domCounters.put("frames", flat.get("Frames"));
        domCounters.put("nodes", flat.get("Nodes"));
        domCounters.put("jsEventListeners", flat.get("JSEventListeners"));
        domCounters.put("layoutCount", flat.get("LayoutCount"));
        domCounters.put("recalcStyleCount", flat.get("RecalcStyleCount"));

        Map<String, Object> timing = new LinkedHashMap<>();
        timing.put("layoutDurationMs", secToMs(flat.get("LayoutDuration")));
        timing.put("recalcStyleDurationMs", secToMs(flat.get("RecalcStyleDuration")));
        timing.put("scriptDurationMs", secToMs(flat.get("ScriptDuration")));
        timing.put("taskDurationMs", secToMs(flat.get("TaskDuration")));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jsHeap", jsHeap);
        out.put("runtimeHeap", runtimeHeap);
        out.put("domCounters", domCounters);
        out.put("timing", timing);
        out.put("raw", flat);
        return out;
    }

    private static Double secToMs(Double sec) {
        return sec == null ? null : sec * 1000.0;
    }

    private static Object numOrNull(Object v) {
        return v instanceof Number ? v : null;
    }

    private Map<String, Object> describe(SimSession s, boolean includeScreenshot) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.id);
        m.put("url", s.url);
        m.put("status", s.status);
        m.put("error", s.error);
        m.put("createdAt", s.createdAt);
        m.put("lastCapturedAt", s.lastCapturedAt > 0 ? s.lastCapturedAt : null);
        m.put("pid", s.chromeProcess != null ? s.chromeProcess.pid() : null);
        m.put("metrics", s.lastMetrics);
        if (includeScreenshot) m.put("screenshotBase64", s.lastScreenshotBase64);
        return m;
    }

    // -------------------------------------------------------------------------
    // CDP command/response plumbing (mirrors WsProxyService's pending-future pattern)
    // -------------------------------------------------------------------------

    private CompletableFuture<Map<String, Object>> sendCommand(SimSession s, String method, Map<String, Object> params) throws IOException {
        long id = s.cmdIdSeq.incrementAndGet();
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("id", id);
        msg.put("method", method);
        msg.put("params", params != null ? params : Map.of());
        CompletableFuture<Map<String, Object>> fut = new CompletableFuture<>();
        s.pending.put(id, fut);
        s.cdpSession.sendMessage(new TextMessage(objectMapper.writeValueAsBytes(msg)));
        return fut;
    }

    private class CdpHandler extends TextWebSocketHandler {
        final SimSession session;
        CdpHandler(SimSession session) { this.session = session; }

        @Override
        protected void handleTextMessage(@NonNull WebSocketSession wsSession, @NonNull TextMessage message) {
            Map<String, Object> parsed;
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> p = objectMapper.readValue(message.getPayload(), Map.class);
                parsed = p;
            } catch (Exception e) {
                return;
            }

            Object idObj = parsed.get("id");
            if (idObj instanceof Number n) {
                CompletableFuture<Map<String, Object>> fut = session.pending.remove(n.longValue());
                if (fut != null) {
                    Object err = parsed.get("error");
                    if (err != null) {
                        fut.completeExceptionally(new RuntimeException("CDP error: " + err));
                    } else {
                        Object result = parsed.get("result");
                        @SuppressWarnings("unchecked")
                        Map<String, Object> resultMap = result instanceof Map ? (Map<String, Object>) result : new LinkedHashMap<>();
                        fut.complete(resultMap);
                    }
                }
                return;
            }

            if ("Page.loadEventFired".equals(parsed.get("method"))) {
                CompletableFuture<Void> waiter = session.loadWaiter;
                if (waiter != null) waiter.complete(null);
            }
        }

        @Override
        public void handleTransportError(@NonNull WebSocketSession wsSession, @NonNull Throwable exception) {
            if ("LAUNCHING".equals(session.status) || "LOADING".equals(session.status)) {
                session.status = "ERROR";
                session.error = rootMessage(exception);
            }
        }

        @Override
        public void afterConnectionClosed(@NonNull WebSocketSession wsSession, @NonNull CloseStatus status) {
            if ("READY".equals(session.status)) {
                session.status = "ERROR";
                session.error = "CDP connection closed: " + status;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Chrome process discovery / bootstrap
    // -------------------------------------------------------------------------

    private static String findChromeExecutable() {
        String override = System.getProperty("browsersimulator.chrome.path", System.getenv("CHROME_PATH"));
        if (override != null && !override.isBlank() && Files.isRegularFile(Path.of(override))) return override;

        List<String> candidates = new ArrayList<>();
        String pf = System.getenv("ProgramFiles");
        String pf86 = System.getenv("ProgramFiles(x86)");
        String localAppData = System.getenv("LocalAppData");
        if (pf != null) {
            candidates.add(pf + "\\Google\\Chrome\\Application\\chrome.exe");
            candidates.add(pf + "\\Microsoft\\Edge\\Application\\msedge.exe");
        }
        if (pf86 != null) {
            candidates.add(pf86 + "\\Google\\Chrome\\Application\\chrome.exe");
            candidates.add(pf86 + "\\Microsoft\\Edge\\Application\\msedge.exe");
        }
        if (localAppData != null) {
            candidates.add(localAppData + "\\Google\\Chrome\\Application\\chrome.exe");
        }
        candidates.add("/usr/bin/google-chrome");
        candidates.add("/usr/bin/google-chrome-stable");
        candidates.add("/usr/bin/chromium-browser");
        candidates.add("/usr/bin/chromium");
        candidates.add("/usr/bin/microsoft-edge");
        candidates.add("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome");
        candidates.add("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge");

        for (String c : candidates) {
            if (c != null && Files.isRegularFile(Path.of(c))) return c;
        }
        throw new IllegalStateException("No Chrome/Chromium/Edge executable found. " +
                "Set the CHROME_PATH environment variable or -Dbrowsersimulator.chrome.path to the browser executable.");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForCdpHttp(int port) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        long deadline = System.currentTimeMillis() + CDP_HTTP_READY_TIMEOUT_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/json/version"))
                        .timeout(Duration.ofSeconds(1)).GET().build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) return;
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(200);
        }
        throw new IOException("Chrome DevTools endpoint did not become ready on port " + port, last);
    }

    private String fetchPageWebSocketUrl(int port) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/json/list"))
                .timeout(Duration.ofSeconds(3)).GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        List<Map<String, Object>> targets = objectMapper.readValue(resp.body(), new TypeReference<>() {});
        for (Map<String, Object> t : targets) {
            if ("page".equals(t.get("type"))) {
                Object ws = t.get("webSocketDebuggerUrl");
                if (ws != null) return ws.toString();
            }
        }
        throw new IOException("No page target found on Chrome DevTools port " + port);
    }

    private void cleanup(SimSession s, String finalStatus) {
        try {
            if (s.cdpSession != null && s.cdpSession.isOpen()) s.cdpSession.close();
        } catch (Exception ignored) {}
        try {
            if (s.chromeProcess != null) s.chromeProcess.destroyForcibly();
        } catch (Exception ignored) {}
        if (s.userDataDir != null) {
            try (var walk = Files.walk(s.userDataDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored2) {}
                });
            } catch (IOException ignored) {}
        }
        s.status = finalStatus;
    }

    private static String rootMessage(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null) r = r.getCause();
        String m = r.getMessage();
        return m != null ? m : r.getClass().getSimpleName();
    }

    // -------------------------------------------------------------------------
    // Per-session state
    // -------------------------------------------------------------------------

    private static class SimSession {
        final String id;
        final String url;
        final long createdAt = System.currentTimeMillis();
        volatile String status = "STARTING";
        volatile String error;
        volatile long lastCapturedAt;
        volatile Process chromeProcess;
        volatile int debugPort;
        volatile WebSocketSession cdpSession;
        volatile Path userDataDir;
        volatile CompletableFuture<Void> loadWaiter;
        volatile Map<String, Object> lastMetrics;
        volatile String lastScreenshotBase64;
        final AtomicLong cmdIdSeq = new AtomicLong();
        final Map<Long, CompletableFuture<Map<String, Object>>> pending = new ConcurrentHashMap<>();

        SimSession(String id, String url) {
            this.id = id;
            this.url = url;
        }
    }
}
