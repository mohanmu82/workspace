package com.mycompany.batch.web;

import com.mycompany.batch.service.AgentDiscoveryService;
import com.mycompany.batch.service.AgentRegistryService;
import com.mycompany.batch.service.AgentRegistryService.AgentConnection;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST surface for the remote agent feature:
 * GET  /agents           — agents currently connected over {@code /agent/ws}
 * POST /agents/discover  — multicast probe for agents on the LAN (connected or not)
 */
@RestController
public class AgentController {

    private final AgentRegistryService  registry;
    private final AgentDiscoveryService discoveryService;

    public AgentController(AgentRegistryService registry, AgentDiscoveryService discoveryService) {
        this.registry         = registry;
        this.discoveryService = discoveryService;
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

    private Map<String, Object> describe(AgentConnection a) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("agentId", a.getAgentId());
        m.put("hostname", a.getHostname());
        m.put("connectedAt", a.getConnectedAt().toString());
        m.put("lastSeen", a.getLastSeen().toString());
        return m;
    }
}
