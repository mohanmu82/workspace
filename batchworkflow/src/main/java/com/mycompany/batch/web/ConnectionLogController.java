package com.mycompany.batch.web;

import com.mycompany.batch.service.ConnectionLogService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/** The SSE / MCP connections and MCP tool calls this server has served — see {@link ConnectionLogService}. */
@RestController
@RequestMapping("/connectionlog")
public class ConnectionLogController {

    private final ConnectionLogService log;

    public ConnectionLogController(ConnectionLogService log) {
        this.log = log;
    }

    @GetMapping
    public Map<String, Object> list() {
        return log.snapshot();
    }

    /** Drops finished connections and every tool call; open connections stay. */
    @DeleteMapping
    public Map<String, Object> clear() {
        log.clear();
        return Map.of("status", "cleared");
    }
}
