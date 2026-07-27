package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.service.MulticastService;
import com.mycompany.batch.util.Threads;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic UDP multicast client, streamed over SSE since replies trickle in
 * asynchronously and there's no way to know in advance how many will arrive.
 *
 * Primary use case: discovering all Jolokia agents (or any other UDP-multicast-
 * discoverable process, e.g. SSDP/UPnP-style services) running across a fleet
 * of servers on the same network segment — send one query to the multicast
 * group and collect every server's reply within a time window.
 */
@RestController
@RequestMapping("/multicast")
public class MulticastController {

    private final ObjectMapper objectMapper;
    private final MulticastService multicastService;

    private static final String JOLOKIA_GROUP = "239.192.48.84";
    private static final String JOLOKIA_QUERY = "{\"type\":\"query\"}";

    private static final int MAX_WAIT_MS = 30_000;
    private static final int MAX_TTL     = 64;

    public MulticastController(ObjectMapper objectMapper, MulticastService multicastService) {
        this.objectMapper = objectMapper;
        this.multicastService = multicastService;
    }

    // -------------------------------------------------------------------------
    // GET /multicast/scan — generic multicast request/collect, streamed via SSE
    // -------------------------------------------------------------------------

    @GetMapping(value = "/scan", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter scan(
            @RequestParam String group,
            @RequestParam int port,
            @RequestParam(defaultValue = "") String message,
            @RequestParam(defaultValue = "3000") int waitMs,
            @RequestParam(defaultValue = "1")    int ttl,
            @RequestParam(required = false)      String iface) {

        return run(group, port, message.getBytes(StandardCharsets.UTF_8), waitMs, ttl, iface, false);
    }

    // -------------------------------------------------------------------------
    // GET /multicast/jolokia — preset for discovering Jolokia agents
    // -------------------------------------------------------------------------

    @GetMapping(value = "/jolokia", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter jolokia(
            @RequestParam(defaultValue = JOLOKIA_GROUP) String group,
            @RequestParam(defaultValue = "24884") int port,
            @RequestParam(defaultValue = "3000")  int waitMs,
            @RequestParam(defaultValue = "8")     int ttl,
            @RequestParam(required = false)       String iface) {

        return run(group, port, JOLOKIA_QUERY.getBytes(StandardCharsets.UTF_8), waitMs, ttl, iface, true);
    }

    // -------------------------------------------------------------------------
    // GET /multicast/interfaces — local multicast-capable NICs (multi-homed hosts)
    // -------------------------------------------------------------------------

    @GetMapping("/interfaces")
    public List<Map<String, Object>> interfaces() throws Exception {
        return multicastService.listMulticastInterfaces();
    }

    // -------------------------------------------------------------------------
    // Shared scan runner
    // -------------------------------------------------------------------------

    private SseEmitter run(String group, int port, byte[] message, int waitMs, int ttl,
                            String iface, boolean parseJson) {
        SseEmitter emitter = new SseEmitter(0L);

        if (group == null || group.isBlank()) return error(emitter, "group is required");
        InetAddress groupAddr;
        try {
            groupAddr = InetAddress.getByName(group.trim());
        } catch (UnknownHostException e) {
            return error(emitter, "Cannot resolve multicast address: " + group);
        }
        if (!groupAddr.isMulticastAddress()) return error(emitter, group + " is not a multicast address");
        if (port < 1 || port > 65535) return error(emitter, "port must be 1-65535");

        String  g        = group.trim();
        int     effWait  = Math.max(200, Math.min(waitMs, MAX_WAIT_MS));
        int     effTtl   = Math.max(1,   Math.min(ttl, MAX_TTL));

        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicInteger count     = new AtomicInteger();
        Runnable shutdown = () -> cancelled.set(true);
        emitter.onCompletion(shutdown);
        emitter.onError(e -> shutdown.run());
        emitter.onTimeout(shutdown);

        Threads.startDaemon("multicast-scan", () -> {
            long start = System.currentTimeMillis();
            try {
                multicastService.discover(g, port, message, effWait, effTtl, iface, resp -> {
                    if (cancelled.get()) return;
                    count.incrementAndGet();
                    sendEvent(emitter, "response", toMap(resp, parseJson));
                });
            } catch (Exception e) {
                if (!cancelled.get()) {
                    sendEvent(emitter, "error", Map.of("error", String.valueOf(e.getMessage())));
                }
            }
            if (!cancelled.get()) {
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("responses", count.get());
                summary.put("durationMs", System.currentTimeMillis() - start);
                sendEvent(emitter, "done", summary);
                emitter.complete();
            }
        });

        return emitter;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> toMap(MulticastService.MulticastResponse r, boolean parseJson) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("host", r.host());
        m.put("port", r.port());
        m.put("payload", r.payload());
        m.put("receivedAtMs", r.receivedAtMs());
        if (parseJson) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = objectMapper.readValue(r.payload(), Map.class);
                m.put("parsed", parsed);
            } catch (Exception ignored) {
                // not JSON (or a differently-shaped agent) — raw payload is still shown
            }
        }
        return m;
    }

    private void sendEvent(SseEmitter emitter, String event, Object data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            emitter.send(SseEmitter.event()
                    .name(event != null ? event : "message")
                    .data(json  != null ? json  : "null"));
        } catch (Exception ignored) {}
    }

    private SseEmitter error(SseEmitter emitter, String message) {
        sendEvent(emitter, "error", Map.of("error", message));
        emitter.complete();
        return emitter;
    }
}
