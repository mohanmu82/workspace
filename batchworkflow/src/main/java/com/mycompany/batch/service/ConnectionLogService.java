package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory log of the SSE and MCP traffic this server serves, for the SSE / MCP Connections page:
 * each HTTP exchange on an SSE or MCP endpoint ({@code ConnectionLogFilter}) and each MCP tool call
 * ({@code AppPageMcpTools}). Only the most recent {@value #MAX} of each are kept; nothing survives a restart.
 */
@Service
public class ConnectionLogService {

    static final int MAX = 500;
    public static final int MAX_TEXT = 4_000;

    /** One HTTP exchange. Mutable: a streaming one is logged when it opens and finished when it closes. */
    public static class Connection {
        public long id;
        public String protocol;
        public String method;
        public String path;
        public String sessionId;
        public String rpcMethod;
        public String request;
        public String remoteAddr;
        public String userAgent;
        public long startedAt;
        public volatile Long endedAt;
        public volatile Long durationMs;
        public volatile Integer status;
        public volatile String state = "OPEN";
    }

    public record ToolCall(long id, long startedAt, String protocol, String sessionId, String client, String tool,
                           String arguments, long durationMs, boolean error, String result) {}

    private final AtomicLong ids = new AtomicLong();
    private final Deque<Connection> connections = new ConcurrentLinkedDeque<>();
    private final Deque<ToolCall> toolCalls = new ConcurrentLinkedDeque<>();
    private final ObjectMapper mapper = new ObjectMapper();

    public Connection open(String protocol, String method, String path, String sessionId, String request,
                           String remoteAddr, String userAgent) {
        Connection c = new Connection();
        c.id = ids.incrementAndGet();
        c.protocol = protocol;
        c.method = method;
        c.path = path;
        c.sessionId = sessionId;
        c.request = cut(request);
        c.rpcMethod = rpcMethod(request);
        c.remoteAddr = remoteAddr;
        c.userAgent = userAgent;
        c.startedAt = System.currentTimeMillis();
        add(connections, c);
        return c;
    }

    public void close(Connection c, int status, String state) {
        if (c.endedAt != null) return; // onComplete follows onError/onTimeout; keep the first reason
        c.endedAt = System.currentTimeMillis();
        c.durationMs = c.endedAt - c.startedAt;
        c.status = status;
        c.state = state;
    }

    public void toolCall(String sessionId, String client, String tool, Map<String, Object> arguments,
                         long startedAt, boolean error, String result) {
        String args;
        try { args = arguments == null ? "{}" : mapper.writeValueAsString(arguments); } catch (Exception e) { args = String.valueOf(arguments); }
        add(toolCalls, new ToolCall(ids.incrementAndGet(), startedAt, protocolOf(sessionId), sessionId, client, tool,
                cut(args), System.currentTimeMillis() - startedAt, error, cut(result)));
    }

    /** The transport a session arrived on, from the HTTP exchanges that carried its id. */
    String protocolOf(String sessionId) {
        if (sessionId == null) return null;
        for (Connection c : connections) if (sessionId.equals(c.sessionId)) return c.protocol;
        return null;
    }

    public Map<String, Object> snapshot() {
        return Map.of("connections", List.copyOf(connections), "toolCalls", List.copyOf(toolCalls));
    }

    public void clear() {
        connections.removeIf(c -> !"OPEN".equals(c.state));
        toolCalls.clear();
    }

    /** Newest first, bounded. */
    private static <T> void add(Deque<T> deque, T item) {
        deque.addFirst(item);
        while (deque.size() > MAX) deque.pollLast();
    }

    private String rpcMethod(String body) {
        if (body == null || !body.trim().startsWith("{")) return null;
        try {
            var node = mapper.readTree(body);
            String m = node.path("method").asText(null);
            String tool = node.path("params").path("name").asText(null);
            return m != null && tool != null ? m + " " + tool : m;
        } catch (Exception e) {
            return null;
        }
    }

    private static String cut(String s) {
        return s == null || s.length() <= MAX_TEXT ? s : s.substring(0, MAX_TEXT) + "… (" + s.length() + " chars)";
    }
}
