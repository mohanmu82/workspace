package com.mycompany.batch.service;

import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks remote agent jars currently connected over {@code /agent/ws}, and correlates
 * in-flight exec/http requests (identified by a server-generated requestId) back either to the
 * browser websocket session on {@code /agentconsole/ws} that issued them, or to a pending
 * {@link CompletableFuture} for requests dispatched server-side (see {@code AgentHttpDispatchService}).
 */
@Service
public class AgentRegistryService {

    public static class AgentConnection {
        private final String agentId;
        private final String hostname;
        private final WebSocketSession session;
        private final Instant connectedAt;
        private volatile Instant lastSeen;

        AgentConnection(String agentId, String hostname, WebSocketSession session) {
            this.agentId     = agentId;
            this.hostname    = hostname;
            this.session     = session;
            this.connectedAt = Instant.now();
            this.lastSeen    = this.connectedAt;
        }

        public String           getAgentId()     { return agentId; }
        public String           getHostname()    { return hostname; }
        public WebSocketSession getSession()     { return session; }
        public Instant          getConnectedAt() { return connectedAt; }
        public Instant          getLastSeen()    { return lastSeen; }

        void touch() { lastSeen = Instant.now(); }
    }

    private final Map<String, AgentConnection>  agents              = new ConcurrentHashMap<>();
    private final Map<String, WebSocketSession> pendingRequests     = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Map<String, Object>>> pendingHttpRequests = new ConcurrentHashMap<>();

    public AgentConnection register(String agentId, String hostname, WebSocketSession session) {
        AgentConnection conn = new AgentConnection(agentId, hostname, session);
        agents.put(agentId, conn);
        return conn;
    }

    /** Removes the registry entry only if it still points at this exact session (avoids racing a fresh reconnect). */
    public void unregister(String agentId, WebSocketSession session) {
        agents.computeIfPresent(agentId, (id, existing) -> existing.getSession() == session ? null : existing);
    }

    public void touch(String agentId) {
        AgentConnection c = agents.get(agentId);
        if (c != null) c.touch();
    }

    public AgentConnection get(String agentId) {
        return agents.get(agentId);
    }

    public List<AgentConnection> list() {
        return new ArrayList<>(agents.values());
    }

    /** Registers a new exec request and returns its requestId, used to route the agent's streamed response back. */
    public String trackRequest(WebSocketSession browserSession) {
        String requestId = UUID.randomUUID().toString();
        pendingRequests.put(requestId, browserSession);
        return requestId;
    }

    public WebSocketSession resolveRequest(String requestId, boolean remove) {
        return remove ? pendingRequests.remove(requestId) : pendingRequests.get(requestId);
    }

    public void dropRequestsFor(WebSocketSession browserSession) {
        pendingRequests.values().removeIf(s -> s == browserSession);
    }

    /** Registers a server-initiated dispatch (no browser session involved) and returns its requestId. */
    public String trackHttpRequest(CompletableFuture<Map<String, Object>> future) {
        String requestId = UUID.randomUUID().toString();
        pendingHttpRequests.put(requestId, future);
        return requestId;
    }

    /** Completes a pending server-initiated dispatch. Returns false if requestId isn't one (e.g. it belongs to a browser console session). */
    public boolean completeHttpRequest(String requestId, Map<String, Object> result) {
        CompletableFuture<Map<String, Object>> future = pendingHttpRequests.remove(requestId);
        if (future == null) return false;
        future.complete(result);
        return true;
    }
}
