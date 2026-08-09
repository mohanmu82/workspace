package com.mycompany.batch.web;

import com.mycompany.batch.service.ObservePollingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read access to the most recent Observe OPAL poll — the actual polling runs on a schedule in
 * {@link ObservePollingService}, this just exposes its cached result and lets the UI force an
 * off-cycle refresh.
 */
@RestController
@RequestMapping("/observe")
public class ObserveController {

    private final ObservePollingService pollingService;

    public ObserveController(ObservePollingService pollingService) {
        this.pollingService = pollingService;
    }

    @GetMapping("/latest")
    public ResponseEntity<Map<String, Object>> latest() {
        Map<String, Object> body = new LinkedHashMap<>(pollingService.status());
        body.put("rows", pollingService.latestRows());
        return ResponseEntity.ok(body);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(pollingService.status());
    }

    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> refresh() {
        return ResponseEntity.ok(pollingService.pollNow());
    }
}
