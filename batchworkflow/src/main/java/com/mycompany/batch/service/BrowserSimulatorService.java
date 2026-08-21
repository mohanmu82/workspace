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
 *
 * <p>For the whole life of a session it also records every network call the page makes
 * ({@code Network.*} events) and everything the page logs or throws ({@code Runtime.exceptionThrown},
 * {@code Runtime.consoleAPICalled}, {@code Log.entryAdded}), both kept in capped in-memory ring
 * buffers and exposed through {@link #describeOne}.
 */
@Service
public class BrowserSimulatorService {

    private static final long CDP_HTTP_READY_TIMEOUT_MS = 15_000;
    private static final long CDP_COMMAND_TIMEOUT_SEC = 10;
    private static final long PAGE_LOAD_TIMEOUT_SEC = 30;
    private static final int  DEFAULT_SETTLE_MS = 1500;
    private static final int  MAX_SETTLE_MS = 15_000;
    /** Ring-buffer caps — a busy page can emit thousands of events, and this all lives in heap. */
    private static final int  MAX_NETWORK_ENTRIES = 1000;
    private static final int  MAX_CONSOLE_ENTRIES = 500;

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
            // Runtime/Log/Network must all be enabled *before* Page.navigate — CDP only reports
            // events that occur while its domain is enabled, so anything enabled after navigation
            // silently misses the page's own startup requests and errors.
            sendCommand(s, "Runtime.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            sendCommand(s, "Log.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
            sendCommand(s, "Network.enable", null).get(CDP_COMMAND_TIMEOUT_SEC, TimeUnit.SECONDS);
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

        List<NetCall> calls = snapshot(s.netCalls);
        List<Map<String, Object>> console = snapshot(s.consoleEntries);
        m.put("networkCount", calls.size());
        m.put("networkFailedCount", calls.stream().filter(BrowserSimulatorService::isFailedCall).count());
        m.put("networkPendingCount", calls.stream().filter(c -> "PENDING".equals(c.state)).count());
        m.put("transferredBytes", calls.stream().filter(c -> c.encodedDataLength != null)
                .mapToLong(c -> c.encodedDataLength).sum());
        m.put("errorCount", console.stream().filter(e -> "error".equals(e.get("level"))).count());
        m.put("warningCount", console.stream().filter(e -> "warning".equals(e.get("level"))).count());

        if (includeScreenshot) {
            m.put("screenshotBase64", s.lastScreenshotBase64);
            m.put("network", calls.stream().map(BrowserSimulatorService::netCallToMap).toList());
            m.put("console", console);
        }
        return m;
    }

    /**
     * The network/console log alone — same payload as {@link #describeOne} minus the screenshot,
     * so the UI can tail it on a short interval without re-downloading the PNG every poll.
     */
    public Map<String, Object> logOf(String id) {
        SimSession s = sessions.get(id);
        if (s == null) throw new IllegalStateException("No session " + id);
        Map<String, Object> m = describe(s, true);
        m.remove("screenshotBase64");
        return m;
    }

    /** Drops the accumulated network/console log for a session without touching the browser. */
    public Map<String, Object> clearLog(String id) {
        SimSession s = sessions.get(id);
        if (s == null) throw new IllegalStateException("No session " + id);
        s.netCalls.clear();
        s.netByRequestId.clear();
        s.consoleEntries.clear();
        return describe(s, true);
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

            String method = parsed.get("method") instanceof String str ? str : null;
            if (method == null) return;

            if ("Page.loadEventFired".equals(method)) {
                CompletableFuture<Void> waiter = session.loadWaiter;
                if (waiter != null) waiter.complete(null);
                return;
            }

            try {
                handleEvent(session, method, asMap(parsed.get("params")));
            } catch (Exception ignored) {
                // an unexpected event shape must never kill the CDP read loop
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
    // Network + console/JS-error event capture
    // -------------------------------------------------------------------------

    private void handleEvent(SimSession s, String method, Map<String, Object> p) {
        switch (method) {
            case "Network.requestWillBeSent" -> onRequestWillBeSent(s, p);
            case "Network.responseReceived" -> {
                NetCall c = s.netByRequestId.get(str(p.get("requestId")));
                if (c != null) applyResponse(c, asMap(p.get("response")), str(p.get("type")));
            }
            case "Network.requestServedFromCache" -> {
                NetCall c = s.netByRequestId.get(str(p.get("requestId")));
                if (c != null) c.fromCache = true;
            }
            case "Network.loadingFinished" -> {
                NetCall c = s.netByRequestId.remove(str(p.get("requestId")));
                if (c != null) {
                    c.state = "COMPLETE";
                    c.encodedDataLength = longOrNull(p.get("encodedDataLength"));
                    c.durationMs = elapsedMs(c.startTs, p.get("timestamp"));
                }
            }
            case "Network.loadingFailed" -> {
                NetCall c = s.netByRequestId.remove(str(p.get("requestId")));
                if (c != null) {
                    boolean canceled = Boolean.TRUE.equals(p.get("canceled"));
                    c.state = canceled ? "CANCELED" : "FAILED";
                    c.errorText = str(p.get("errorText"));
                    Object blocked = p.get("blockedReason");
                    if (blocked != null) c.errorText = (c.errorText == null ? "" : c.errorText + " ") + "(blocked: " + blocked + ")";
                    c.durationMs = elapsedMs(c.startTs, p.get("timestamp"));
                }
            }
            case "Runtime.exceptionThrown" -> onExceptionThrown(s, p);
            case "Runtime.consoleAPICalled" -> onConsoleApiCalled(s, p);
            case "Log.entryAdded" -> onLogEntryAdded(s, asMap(p.get("entry")));
            default -> { /* every other CDP event is uninteresting here */ }
        }
    }

    private void onRequestWillBeSent(SimSession s, Map<String, Object> p) {
        String requestId = str(p.get("requestId"));
        // A redirect reuses the same requestId: close out the hop already recorded under it and
        // record the redirect target as its own row, the way DevTools' network panel shows them.
        Map<String, Object> redirect = asMap(p.get("redirectResponse"));
        if (!redirect.isEmpty()) {
            NetCall prev = s.netByRequestId.remove(requestId);
            if (prev != null) {
                applyResponse(prev, redirect, prev.resourceType);
                prev.state = "REDIRECT";
                prev.durationMs = elapsedMs(prev.startTs, p.get("timestamp"));
            }
        }

        Map<String, Object> request = asMap(p.get("request"));
        NetCall c = new NetCall(requestId, s.netSeq.incrementAndGet());
        c.url = str(request.get("url"));
        c.method = str(request.get("method"));
        c.resourceType = str(p.get("type"));
        c.initiator = str(asMap(p.get("initiator")).get("type"));
        c.startTs = dbl(p.get("timestamp"));
        Double wall = dbl(p.get("wallTime"));
        c.startedAtWallMs = wall != null ? (long) (wall * 1000.0) : System.currentTimeMillis();
        s.netByRequestId.put(requestId, c);
        synchronized (s.netCalls) {
            s.netCalls.add(c);
            // Evicting from the display list must also drop the correlation entry, or a page that
            // never finishes its requests would grow netByRequestId without bound.
            while (s.netCalls.size() > MAX_NETWORK_ENTRIES) {
                NetCall evicted = s.netCalls.remove(0);
                s.netByRequestId.remove(evicted.requestId, evicted);
            }
        }
    }

    private static void applyResponse(NetCall c, Map<String, Object> response, String type) {
        if (response.isEmpty()) return;
        Object status = response.get("status");
        c.status = status instanceof Number n ? n.intValue() : null;
        c.statusText = str(response.get("statusText"));
        c.mimeType = str(response.get("mimeType"));
        c.protocol = str(response.get("protocol"));
        String ip = str(response.get("remoteIPAddress"));
        Object port = response.get("remotePort");
        c.remoteAddress = ip == null ? null : (port instanceof Number n ? ip + ":" + n.intValue() : ip);
        if (Boolean.TRUE.equals(response.get("fromDiskCache"))) c.fromCache = true;
        if (c.resourceType == null) c.resourceType = type;
    }

    private void onExceptionThrown(SimSession s, Map<String, Object> p) {
        Map<String, Object> details = asMap(p.get("exceptionDetails"));
        Map<String, Object> exception = asMap(details.get("exception"));
        String text = str(exception.get("description"));
        if (text == null) text = str(details.get("text"));
        if (text == null) text = "Uncaught exception";
        Map<String, Object> e = newConsoleEntry(s, "error", "javascript", text);
        e.put("url", str(details.get("url")));
        e.put("lineNumber", plusOne(intOrNull(details.get("lineNumber"))));
        e.put("columnNumber", plusOne(intOrNull(details.get("columnNumber"))));
        e.put("stack", formatStack(asMap(details.get("stackTrace"))));
        Double ts = dbl(p.get("timestamp"));
        if (ts != null) e.put("timestamp", ts.longValue());
        addCapped(s.consoleEntries, e, MAX_CONSOLE_ENTRIES);
    }

    private void onConsoleApiCalled(SimSession s, Map<String, Object> p) {
        String type = str(p.get("type"));
        StringBuilder text = new StringBuilder();
        Object args = p.get("args");
        if (args instanceof List<?> list) {
            for (Object a : list) {
                if (text.length() > 0) text.append(' ');
                text.append(describeRemoteObject(asMap(a)));
            }
        }
        Map<String, Object> stack = asMap(p.get("stackTrace"));
        Map<String, Object> e = newConsoleEntry(s, normalizeLevel(type), "console-api", text.toString());
        e.put("stack", formatStack(stack));
        Object frames = stack.get("callFrames");
        if (frames instanceof List<?> fl && !fl.isEmpty()) {
            Map<String, Object> top = asMap(fl.get(0));
            e.put("url", str(top.get("url")));
            e.put("lineNumber", plusOne(intOrNull(top.get("lineNumber"))));
            e.put("columnNumber", plusOne(intOrNull(top.get("columnNumber"))));
        }
        Double ts = dbl(p.get("timestamp"));
        if (ts != null) e.put("timestamp", ts.longValue());
        addCapped(s.consoleEntries, e, MAX_CONSOLE_ENTRIES);
    }

    private void onLogEntryAdded(SimSession s, Map<String, Object> entry) {
        if (entry.isEmpty()) return;
        // console.* output already arrives via Runtime.consoleAPICalled — recording it here too
        // would double every line. Everything else (network, security, deprecation, …) is unique.
        String source = str(entry.get("source"));
        if ("console-api".equals(source)) return;
        Map<String, Object> e = newConsoleEntry(s, normalizeLevel(str(entry.get("level"))),
                source != null ? source : "log", str(entry.get("text")));
        e.put("url", str(entry.get("url")));
        e.put("lineNumber", intOrNull(entry.get("lineNumber")));
        e.put("stack", formatStack(asMap(entry.get("stackTrace"))));
        Double ts = dbl(entry.get("timestamp"));
        if (ts != null) e.put("timestamp", ts.longValue());
        addCapped(s.consoleEntries, e, MAX_CONSOLE_ENTRIES);
    }

    private Map<String, Object> newConsoleEntry(SimSession s, String level, String source, String text) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("seq", s.consoleSeq.incrementAndGet());
        e.put("timestamp", System.currentTimeMillis());
        e.put("level", level);
        e.put("source", source);
        e.put("text", text != null ? text : "");
        return e;
    }

    private static Map<String, Object> netCallToMap(NetCall c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", c.seq);
        m.put("requestId", c.requestId);
        m.put("url", c.url);
        m.put("method", c.method);
        m.put("resourceType", c.resourceType);
        m.put("initiator", c.initiator);
        m.put("status", c.status);
        m.put("statusText", c.statusText);
        m.put("mimeType", c.mimeType);
        m.put("protocol", c.protocol);
        m.put("remoteAddress", c.remoteAddress);
        m.put("fromCache", c.fromCache);
        m.put("sizeBytes", c.encodedDataLength);
        m.put("durationMs", c.durationMs);
        m.put("state", c.state);
        m.put("error", c.errorText);
        m.put("startedAt", c.startedAtWallMs);
        return m;
    }

    /** A network call counts as failed if the transfer errored or the response was 4xx/5xx. */
    private static boolean isFailedCall(NetCall c) {
        return "FAILED".equals(c.state) || (c.status != null && c.status >= 400);
    }

    private static String normalizeLevel(String cdpLevel) {
        if (cdpLevel == null) return "log";
        return switch (cdpLevel) {
            case "error", "assert" -> "error";
            case "warning", "warn" -> "warning";
            case "info", "debug", "verbose" -> "info";
            default -> "log";
        };
    }

    private static String describeRemoteObject(Map<String, Object> arg) {
        if (arg.isEmpty()) return "";
        Object value = arg.get("value");
        if (value != null) return String.valueOf(value);
        Object description = arg.get("description");
        if (description != null) return String.valueOf(description);
        if (arg.containsKey("unserializableValue")) return String.valueOf(arg.get("unserializableValue"));
        Object subtype = arg.get("subtype");
        return String.valueOf(subtype != null ? subtype : arg.getOrDefault("type", "?"));
    }

    private static String formatStack(Map<String, Object> stackTrace) {
        Object frames = stackTrace.get("callFrames");
        if (!(frames instanceof List<?> list) || list.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        for (Object f : list) {
            Map<String, Object> frame = asMap(f);
            String fn = str(frame.get("functionName"));
            Integer line = plusOne(intOrNull(frame.get("lineNumber")));
            Integer col = plusOne(intOrNull(frame.get("columnNumber")));
            sb.append("at ").append(fn == null || fn.isEmpty() ? "(anonymous)" : fn)
              .append(" (").append(str(frame.get("url"))).append(':').append(line).append(':').append(col).append(")\n");
        }
        return sb.toString().trim();
    }

    private static <T> void addCapped(List<T> list, T item, int max) {
        synchronized (list) {
            list.add(item);
            while (list.size() > max) list.remove(0);
        }
    }

    private static <T> List<T> snapshot(List<T> list) {
        synchronized (list) {
            return new ArrayList<>(list);
        }
    }

    private static Double elapsedMs(Double startTs, Object endTs) {
        Double end = dbl(endTs);
        if (startTs == null || end == null) return null;
        return (end - startTs) * 1000.0;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }

    private static String str(Object o) {
        return o != null ? String.valueOf(o) : null;
    }

    private static Double dbl(Object o) {
        return o instanceof Number n ? n.doubleValue() : null;
    }

    private static Long longOrNull(Object o) {
        return o instanceof Number n ? n.longValue() : null;
    }

    private static Integer intOrNull(Object o) {
        return o instanceof Number n ? n.intValue() : null;
    }

    /** CDP reports script positions 0-based; DevTools and stack traces show them 1-based. */
    private static Integer plusOne(Integer v) {
        return v == null ? null : v + 1;
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

        /** Every network call the page made, oldest first (capped at {@link #MAX_NETWORK_ENTRIES}). */
        final List<NetCall> netCalls = Collections.synchronizedList(new ArrayList<>());
        /** In-flight calls, so response/finish/fail events can be matched back to their request. */
        final Map<String, NetCall> netByRequestId = new ConcurrentHashMap<>();
        /** JS exceptions, console output and browser log entries (capped at {@link #MAX_CONSOLE_ENTRIES}). */
        final List<Map<String, Object>> consoleEntries = Collections.synchronizedList(new ArrayList<>());
        final AtomicLong netSeq = new AtomicLong();
        final AtomicLong consoleSeq = new AtomicLong();

        SimSession(String id, String url) {
            this.id = id;
            this.url = url;
        }
    }

    /** One network request/response pair, assembled across the four CDP Network lifecycle events. */
    private static class NetCall {
        final String requestId;
        final long seq;
        volatile long startedAtWallMs;
        volatile Double startTs;
        volatile String url;
        volatile String method;
        volatile String resourceType;
        volatile String initiator;
        volatile Integer status;
        volatile String statusText;
        volatile String mimeType;
        volatile String protocol;
        volatile String remoteAddress;
        volatile boolean fromCache;
        volatile Long encodedDataLength;
        volatile Double durationMs;
        volatile String state = "PENDING";
        volatile String errorText;

        NetCall(String requestId, long seq) {
            this.requestId = requestId;
            this.seq = seq;
        }
    }
}
