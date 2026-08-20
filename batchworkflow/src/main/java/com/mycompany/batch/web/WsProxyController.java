package com.mycompany.batch.web;

import com.mycompany.batch.model.WsProxyLogEntry;
import com.mycompany.batch.model.WsProxyMode;
import com.mycompany.batch.service.WsProxyService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST control plane for {@link WsProxyService}. Each proxy instance itself is a raw socket
 * server bound to the requested port (see {@link WsProxyService} for why) — these endpoints
 * only start/stop instances, drive the manual "send" test button, and serve the request log.
 */
@RestController
@RequestMapping("/wsproxy")
public class WsProxyController {

    private final WsProxyService service;

    public WsProxyController(WsProxyService service) {
        this.service = service;
    }

    @PostMapping("/start")
    public ResponseEntity<?> start(@RequestBody Map<String, Object> body) {
        try {
            int port = ((Number) body.get("port")).intValue();
            String targetsRaw = String.valueOf(body.getOrDefault("targets", ""));
            List<String> targets = new ArrayList<>();
            for (String t : targetsRaw.split(",")) {
                String trimmed = t.trim();
                if (!trimmed.isEmpty()) targets.add(trimmed);
            }
            WsProxyMode mode = WsProxyMode.valueOf(String.valueOf(body.getOrDefault("mode", "PROXY")).toUpperCase());
            if (port < 1 || port > 65535) return error("port must be 1-65535");
            if (mode != WsProxyMode.REFLECT && targets.isEmpty()) return error("At least one target is required for " + mode + " mode");
            return ResponseEntity.ok(service.start(port, targets, mode));
        } catch (IllegalStateException | IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stop(@RequestBody Map<String, Object> body) {
        int port = ((Number) body.get("port")).intValue();
        service.stop(port);
        return ResponseEntity.ok(Map.of("stopped", port));
    }

    @GetMapping("/status")
    public List<Map<String, Object>> status() {
        return service.status();
    }

    @PostMapping("/send")
    public ResponseEntity<?> send(@RequestBody Map<String, Object> body) {
        try {
            int port = ((Number) body.get("port")).intValue();
            String payload = String.valueOf(body.getOrDefault("payload", ""));
            return ResponseEntity.ok(service.sendManual(port, payload));
        } catch (IllegalStateException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/log")
    public List<WsProxyLogEntry> log(@RequestParam(required = false) Integer port,
                                      @RequestParam(required = false) Integer limit,
                                      @RequestParam(required = false) Integer sinceMinutes) {
        return service.queryLog(port, limit, sinceMinutes);
    }

    private ResponseEntity<?> error(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message != null ? message : "Bad request"));
    }
}
