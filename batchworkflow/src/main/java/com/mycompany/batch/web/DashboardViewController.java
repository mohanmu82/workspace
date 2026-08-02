package com.mycompany.batch.web;

import com.mycompany.batch.dashboardview.DashboardView;
import com.mycompany.batch.dashboardview.DashboardViewService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Manages saved Dashboard Views (group-by drill-down levels + swimlane orientation + active
 * filter) for the Service Dashboard's Services grid — see {@link DashboardView}.
 */
@RestController
@RequestMapping("/dashboardview")
public class DashboardViewController {

    private final DashboardViewService service;

    public DashboardViewController(DashboardViewService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<DashboardView>> list(@RequestParam(defaultValue = "serviceDashboard") String page) {
        return ResponseEntity.ok(service.list(page));
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> save(@RequestBody DashboardView view) {
        try {
            DashboardView saved = service.save(view);
            return ResponseEntity.ok(saved);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to save view: " + e.getMessage()));
        }
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> delete(@PathVariable String name,
                                     @RequestParam(defaultValue = "serviceDashboard") String page) {
        try {
            service.delete(page, name);
            return ResponseEntity.ok(Map.of("status", "deleted", "name", name));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
