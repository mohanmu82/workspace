package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.model.HttpBatchRequest;
import com.mycompany.batch.service.AgentRegistryService.AgentConnection;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fans a batch of self-contained HTTP requests out across every agent currently connected on
 * {@code /agent/ws}, round-robin, and blocks until every one has replied. Requests travel over
 * the existing agent control-channel websocket — no new inbound listener is added on the agent
 * side, matching how {@link com.mycompany.batch.web.AgentConsoleWebSocketHandler} already talks
 * to agents for the browser console.
 *
 * <p>{@link #dispatchSingle} is the one-request door onto the same rotation, used by callers that
 * arrive with a single call to place rather than a batch.
 */
@Service
public class AgentHttpDispatchService {

    private static final int DEFAULT_TIMEOUT_MS = 30_000;
    private static final int TIMEOUT_GRACE_MS   = 5_000;

    private final AgentRegistryService registry;
    private final ObjectMapper         objectMapper;

    /**
     * Where the next round-robin hand-out starts. Kept across calls so a caller dispatching one
     * request at a time — the App Catalog running a single instance on an agent — still spreads its
     * work over the fleet instead of every call landing on the first agent.
     */
    private final AtomicInteger roundRobin = new AtomicInteger();

    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "http-dispatch-timeout");
                t.setDaemon(true);
                return t;
            });

    public AgentHttpDispatchService(AgentRegistryService registry, ObjectMapper objectMapper) {
        this.registry     = registry;
        this.objectMapper = objectMapper;
    }

    /** Round-robins {@code requests} across connected agents and blocks until every one has replied or timed out. */
    public List<Map<String, Object>> dispatch(List<HttpBatchRequest.Item> requests, Integer defaultTimeoutMs) {
        return dispatch(requests, defaultTimeoutMs, null);
    }

    /**
     * Dispatches {@code requests}, blocking until every one has replied or timed out. When
     * {@code agentId} is set, every request is sent to that specific agent instead of being
     * round-robined across all connected agents.
     */
    public List<Map<String, Object>> dispatch(List<HttpBatchRequest.Item> requests, Integer defaultTimeoutMs, String agentId) {
        List<AgentConnection> agents = agentId != null && !agentId.isBlank()
                ? List.of(resolveTargetAgent(agentId))
                : registry.list().stream().filter(a -> a.getSession().isOpen()).toList();
        if (agents.isEmpty()) {
            throw new IllegalStateException("No agents connected");
        }

        int fallbackTimeout = defaultTimeoutMs != null && defaultTimeoutMs > 0 ? defaultTimeoutMs : DEFAULT_TIMEOUT_MS;
        int start = roundRobin.getAndAdd(requests.size());
        List<CompletableFuture<Map<String, Object>>> futures = new ArrayList<>(requests.size());
        for (int i = 0; i < requests.size(); i++) {
            AgentConnection agent = agents.get(Math.floorMod(start + i, agents.size()));
            futures.add(dispatchOne(agent, requests.get(i), fallbackTimeout));
        }

        List<Map<String, Object>> results = new ArrayList<>(futures.size());
        for (CompletableFuture<Map<String, Object>> future : futures) {
            try {
                results.add(future.get());
            } catch (Exception e) {
                results.add(Map.of("error", e.getMessage() != null ? e.getMessage() : "dispatch failed"));
            }
        }
        return results;
    }

    /**
     * Sends one request and blocks until the agent replies or it times out. With a blank
     * {@code agentId} the next agent in the rotation takes it, so concurrent single dispatches
     * still spread across the fleet.
     *
     * @return the agent's reply — {@code statusCode}/{@code headers}/{@code body}/{@code agentId} on
     *         success, or a map carrying {@code error} when the call could not be completed
     */
    public Map<String, Object> dispatchSingle(HttpBatchRequest.Item request, Integer defaultTimeoutMs, String agentId) {
        AgentConnection agent = agentId != null && !agentId.isBlank() ? resolveTargetAgent(agentId) : nextAgent();
        int timeoutMs = defaultTimeoutMs != null && defaultTimeoutMs > 0 ? defaultTimeoutMs : DEFAULT_TIMEOUT_MS;
        try {
            return dispatchOne(agent, request, timeoutMs).get();
        } catch (Exception e) {
            return Map.of("agentId", agent.getAgentId(),
                          "error", e.getMessage() != null ? e.getMessage() : "dispatch failed");
        }
    }

    /** The next connected agent in the rotation. */
    private AgentConnection nextAgent() {
        List<AgentConnection> agents = registry.list().stream().filter(a -> a.getSession().isOpen()).toList();
        if (agents.isEmpty()) throw new IllegalStateException("No agents connected");
        return agents.get(Math.floorMod(roundRobin.getAndIncrement(), agents.size()));
    }

    private AgentConnection resolveTargetAgent(String agentId) {
        AgentConnection agent = registry.get(agentId);
        if (agent == null || !agent.getSession().isOpen()) {
            throw new IllegalStateException("Agent not connected: " + agentId);
        }
        return agent;
    }

    /**
     * Sends one non-HTTP control message to a named agent and blocks for its reply — used for the
     * out-of-band things the console asks an agent to do to itself, such as reloading its TLS trust
     * store. Rides the same request-id correlation and timeout guard as a dispatched HTTP call,
     * because from the server's side it is the same shape of conversation.
     *
     * @param message the message body; {@code type} and {@code requestId} are filled in here
     * @return the agent's reply, or a map carrying {@code error} when it never came
     */
    public Map<String, Object> sendControl(String agentId, String type, Map<String, Object> message, int timeoutMs) {
        AgentConnection agent = resolveTargetAgent(agentId);
        Map<String, Object> outbound = new LinkedHashMap<>(message);
        outbound.put("type", type);

        int wait = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        try {
            Map<String, Object> reply = send(agent, outbound, wait).get();
            Map<String, Object> withMeta = new LinkedHashMap<>(reply);
            withMeta.put("agentId", agent.getAgentId());
            return withMeta;
        } catch (Exception e) {
            return Map.of("agentId", agent.getAgentId(), "ok", false,
                          "error", e.getMessage() != null ? e.getMessage() : "control message failed");
        }
    }

    /** Every connected agent, in registry order — the fan-out list for a fleet-wide control message. */
    public List<String> connectedAgentIds() {
        return registry.list().stream().filter(a -> a.getSession().isOpen())
                .map(AgentConnection::getAgentId).toList();
    }

    private CompletableFuture<Map<String, Object>> dispatchOne(AgentConnection agent, HttpBatchRequest.Item spec, int fallbackTimeoutMs) {
        int timeoutMs = spec.getTimeoutMs() != null && spec.getTimeoutMs() > 0 ? spec.getTimeoutMs() : fallbackTimeoutMs;

        Map<String, Object> outbound = new LinkedHashMap<>();
        outbound.put("type", "http");
        outbound.put("url", spec.getUrl());
        outbound.put("method", spec.getMethod() != null ? spec.getMethod().name() : "GET");
        outbound.put("headers", spec.getHeaders() != null ? spec.getHeaders() : Map.of());
        outbound.put("body", spec.getBody() != null ? spec.getBody() : "");
        outbound.put("timeoutMs", timeoutMs);

        String agentId = agent.getAgentId();
        String requestUrl = spec.getUrl();
        return send(agent, outbound, timeoutMs).thenApply(result -> {
            Map<String, Object> withMeta = new LinkedHashMap<>(result);
            withMeta.put("agentId", agentId);
            withMeta.put("url", requestUrl);
            return withMeta;
        });
    }

    /**
     * Registers a reply slot, stamps the message with its request id, sends it, and arms the
     * timeout that completes the future with an error if the agent never answers — without which a
     * caller blocked on {@code get()} would wait on an agent that has gone away.
     */
    private CompletableFuture<Map<String, Object>> send(AgentConnection agent, Map<String, Object> message, int timeoutMs) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
        String requestId = registry.trackHttpRequest(future);
        message.put("requestId", requestId);

        sendJson(agent.getSession(), message);

        ScheduledFuture<?> guard = timeoutScheduler.schedule(
                () -> registry.completeHttpRequest(requestId,
                        Map.of("error", "Timed out waiting for agent " + agent.getAgentId())),
                timeoutMs + TIMEOUT_GRACE_MS, TimeUnit.MILLISECONDS);
        future.whenComplete((r, ex) -> guard.cancel(false));
        return future;
    }

    private void sendJson(WebSocketSession ws, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            synchronized (ws) {
                if (ws.isOpen()) ws.sendMessage(new TextMessage(json));
            }
        } catch (Exception ignored) {}
    }
}
