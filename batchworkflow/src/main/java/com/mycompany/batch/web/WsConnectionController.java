package com.mycompany.batch.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** The outbound WebSocket connections the page relay holds — listed, paused, resumed or disconnected. */
@RestController
@RequestMapping("/wsconnections")
public class WsConnectionController {

    private final AppWebSocketRelayHandler relay;

    public WsConnectionController(AppWebSocketRelayHandler relay) {
        this.relay = relay;
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return relay.connections();
    }

    /** {@code op} is {@code pause}, {@code resume} or {@code disconnect}. */
    @PostMapping("/{id}/{op}")
    public ResponseEntity<?> control(@PathVariable String id, @PathVariable String op) {
        try {
            relay.control(id, op);
            return ResponseEntity.ok(Map.of("status", "ok"));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
