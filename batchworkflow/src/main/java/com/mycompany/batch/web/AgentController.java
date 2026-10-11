package com.mycompany.batch.web;

import com.mycompany.batch.model.HttpBatchRequest;
import com.mycompany.batch.service.AgentDiscoveryService;
import com.mycompany.batch.service.AgentHttpDispatchService;
import com.mycompany.batch.service.AgentRegistryService;
import com.mycompany.batch.service.AgentRegistryService.AgentConnection;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * REST surface for the remote agent feature:
 * GET  /agents             — agents currently connected over {@code /agent/ws}
 * POST /agents/discover    — multicast probe for agents on the LAN (connected or not)
 * POST /agents/http-batch  — fan a batch of HTTP requests out across connected agents, round-robin
 *                             (or all to one agent via {@code agentId}), and return every result in
 *                             one response (see {@link AgentHttpDispatchService})
 * POST /agents/truststore  — reload the outbound TLS trust of one agent, or of every connected one
 * POST /agents/file-info   — getFileInfo skill: stat an array of paths on one agent, or on every one
 * POST /agents/system-info — getSystemInfo skill: CPU/memory/disk/fd/top-process snapshot per agent
 */
@RestController
public class AgentController {

    private final AgentRegistryService     registry;
    private final AgentDiscoveryService    discoveryService;
    private final AgentHttpDispatchService httpDispatchService;

    public AgentController(AgentRegistryService registry, AgentDiscoveryService discoveryService,
                            AgentHttpDispatchService httpDispatchService) {
        this.registry            = registry;
        this.discoveryService    = discoveryService;
        this.httpDispatchService = httpDispatchService;
    }

    @GetMapping("/agents")
    public ResponseEntity<List<Map<String, Object>>> listAgents() {
        List<Map<String, Object>> agents = registry.list().stream().map(this::describe).toList();
        return ResponseEntity.ok(agents);
    }

    @PostMapping("/agents/discover")
    public ResponseEntity<?> discover(@RequestParam(defaultValue = "1500") int timeoutMs) {
        try {
            return ResponseEntity.ok(discoveryService.discover(timeoutMs));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/agents/http-batch")
    public ResponseEntity<?> httpBatch(@RequestBody HttpBatchRequest request) {
        if (request.getRequests() == null || request.getRequests().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "requests must not be empty"));
        }
        try {
            List<Map<String, Object>> results = httpDispatchService.dispatch(
                    request.getRequests(), request.getDefaultTimeoutMs(), request.getAgentId());
            return ResponseEntity.ok(results);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Pushes a TLS trust store to running agents, so an https endpoint signed by an internal CA
     * becomes reachable from a host whose JVM has never heard of that CA — the usual reason a call
     * that works from this server fails the moment it is routed through an agent.
     *
     * <p>With no {@code agentId} every connected agent gets it: a trust problem is a property of
     * the endpoint, not of one host, so fixing it one agent at a time is nearly always wrong.
     *
     * <p>Always 200 with a per-agent result. One agent failing to load the store says nothing about
     * the others, and the whole point of the reply is to show which of them now trust what.
     */
    @PostMapping(value = "/agents/truststore", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> pushTrustStore(@RequestBody TrustStorePush request) {
        String mode = request.mode() == null || request.mode().isBlank() ? "INLINE" : request.mode().trim().toUpperCase();
        if ("INLINE".equals(mode) && (request.data() == null || request.data().isBlank()))
            return ResponseEntity.badRequest().body(Map.of("error", "data (base64 trust store) is required for mode INLINE"));
        if ("CERT".equals(mode) && (request.data() == null || request.data().isBlank()))
            return ResponseEntity.badRequest().body(Map.of("error", "data (base64 PEM or DER certificate) is required for mode CERT"));
        if ("FILE".equals(mode) && (request.path() == null || request.path().isBlank()))
            return ResponseEntity.badRequest().body(Map.of("error", "path is required for mode FILE"));

        List<String> targets = request.agentId() != null && !request.agentId().isBlank()
                ? List.of(request.agentId())
                : httpDispatchService.connectedAgentIds();
        if (targets.isEmpty()) return ResponseEntity.badRequest().body(Map.of("error", "No agents connected"));

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("mode", mode);
        message.put("path", request.path());
        message.put("password", request.password());
        message.put("storeType", request.storeType());
        message.put("data", request.data());
        message.put("includeDefaults", request.includeDefaults() == null || request.includeDefaults());

        List<Map<String, Object>> results = targets.stream()
                .map(agentId -> {
                    try {
                        Map<String, Object> reply = httpDispatchService.sendControl(agentId, "truststore",
                                new LinkedHashMap<>(message), request.timeoutMs() != null ? request.timeoutMs() : 20_000);
                        rememberTrustStore(agentId, reply);
                        return reply;
                    } catch (IllegalStateException e) {
                        return Map.<String, Object>of("agentId", agentId, "ok", false, "error", e.getMessage());
                    }
                })
                .toList();
        return ResponseEntity.ok(results);
    }

    /**
     * The {@code getFileInfo} skill: stats every path in {@code paths} on the agent's host — exists,
     * name, type, created/modified/accessed, size, permissions, owner — in one reply per agent.
     *
     * <p>With no {@code agentId} the same paths are checked on every connected agent, which is how
     * a fleet is compared ("is this jar the same size and age everywhere?"). Always 200 with a
     * per-agent result, for the same reason as {@link #pushTrustStore}.
     */
    @PostMapping(value = "/agents/file-info", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> fileInfo(@RequestBody FileInfoRequest request) {
        if (request.paths() == null || request.paths().isEmpty())
            return ResponseEntity.badRequest().body(Map.of("error", "paths must not be empty"));

        List<String> targets = targetsFor(request.agentId());
        if (targets.isEmpty()) return ResponseEntity.badRequest().body(Map.of("error", "No agents connected"));

        Map<String, Object> message = Map.of("paths", request.paths());
        int timeoutMs = request.timeoutMs() != null ? request.timeoutMs() : 30_000;
        return ResponseEntity.ok(fanOut(targets, "fileInfo", message, timeoutMs));
    }

    /**
     * The {@code getSystemInfo} skill: a health snapshot of the agent's host — OS and uptime, CPU
     * usage and load, memory and swap, disks, file descriptors, network interfaces, and the top
     * {@code topN} processes by CPU, memory and (Linux) open file descriptors.
     *
     * <p>{@code sampleMs} is the CPU measurement window; with no {@code agentId} every connected
     * agent is sampled concurrently, so the whole fleet costs one window rather than one per host.
     */
    @PostMapping(value = "/agents/system-info", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> systemInfo(@RequestBody(required = false) SystemInfoRequest request) {
        SystemInfoRequest req = request != null ? request : new SystemInfoRequest(null, null, null, null);
        List<String> targets = targetsFor(req.agentId());
        if (targets.isEmpty()) return ResponseEntity.badRequest().body(Map.of("error", "No agents connected"));

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("topN", req.topN() != null ? req.topN() : 10);
        message.put("sampleMs", req.sampleMs() != null ? req.sampleMs() : 1000);
        int timeoutMs = req.timeoutMs() != null ? req.timeoutMs() : 30_000;
        return ResponseEntity.ok(fanOut(targets, "systemInfo", message, timeoutMs));
    }

    /** Body for {@code POST /agents/file-info}; blank {@code agentId} means every connected agent. */
    public record FileInfoRequest(String agentId, List<String> paths, Integer timeoutMs) {}

    /** Body for {@code POST /agents/system-info}; blank {@code agentId} means every connected agent. */
    public record SystemInfoRequest(String agentId, Integer topN, Integer sampleMs, Integer timeoutMs) {}

    private List<String> targetsFor(String agentId) {
        return agentId != null && !agentId.isBlank() ? List.of(agentId) : httpDispatchService.connectedAgentIds();
    }

    /** Sends {@code message} to every target concurrently and collects one reply per agent, in target order. */
    private List<Map<String, Object>> fanOut(List<String> targets, String type, Map<String, Object> message, int timeoutMs) {
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Map<String, Object>>> futures = targets.stream()
                    .map(agentId -> pool.submit(() -> {
                        try {
                            return httpDispatchService.sendControl(agentId, type, new LinkedHashMap<>(message), timeoutMs);
                        } catch (IllegalStateException e) {
                            return Map.<String, Object>of("agentId", agentId, "error", e.getMessage());
                        }
                    }))
                    .toList();
            List<Map<String, Object>> results = new ArrayList<>(futures.size());
            for (int i = 0; i < futures.size(); i++) {
                try {
                    results.add(futures.get(i).get());
                } catch (Exception e) {
                    results.add(Map.of("agentId", targets.get(i), "error", String.valueOf(e.getMessage())));
                }
            }
            return results;
        }
    }

    /**
     * Body for {@code POST /agents/truststore}.
     *
     * @param agentId        one agent, or blank for every connected agent
     * @param mode           {@code CERT} (one or more bare certificates in {@code data}),
     *                       {@code INLINE} (a keystore in {@code data}), {@code FILE} (a path the
     *                       agent's own host can read, either shape), {@code DEFAULT} (back to the
     *                       JVM's trust) or {@code INSECURE} (accept anything — for telling a trust
     *                       failure apart from a connectivity one)
     * @param data           base64 of the file: a .jks/.p12 keystore for {@code INLINE}, or a
     *                       PEM/DER certificate — a chain in one PEM is fine — for {@code CERT}
     * @param includeDefaults keep the JVM's own trust anchors alongside the supplied ones; default
     *                       true, because an internal CA is meant to be trusted in addition to the
     *                       public roots rather than instead of them
     */
    public record TrustStorePush(String agentId, String mode, String path, String password,
                                 String storeType, String data, Boolean includeDefaults, Integer timeoutMs) {}

    /**
     * Keeps {@code GET /agents} honest after a successful reload. Only on success — a rejected store
     * changes nothing on the agent, so recording it would have the console showing trust the agent
     * never took on.
     */
    private void rememberTrustStore(String agentId, Map<String, Object> reply) {
        if (!Boolean.TRUE.equals(reply.get("ok"))) return;
        AgentConnection agent = registry.get(agentId);
        if (agent == null) return;

        Map<String, Object> status = new LinkedHashMap<>();
        for (String field : List.of("source", "certificates", "loadedAt")) {
            if (reply.get(field) != null) status.put(field, reply.get(field));
        }
        agent.setTrustStore(status);
    }

    private Map<String, Object> describe(AgentConnection a) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("agentId", a.getAgentId());
        m.put("hostname", a.getHostname());
        m.put("connectedAt", a.getConnectedAt().toString());
        m.put("lastSeen", a.getLastSeen().toString());
        m.put("trustStore", a.getTrustStore());
        return m;
    }
}
