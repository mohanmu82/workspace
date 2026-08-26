package com.mycompany.batch.web;

import com.mycompany.batch.sequencediagram.SequenceDiagram;
import com.mycompany.batch.sequencediagram.ServerHit;
import com.mycompany.batch.sequencediagram.SequenceDiagramService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * REST surface behind {@code diagrams.html} — CRUD over the stored diagrams.
 *
 * <p>A diagram is saved whole: the editor sends back the nodes, links and steps it has on the
 * canvas and the server replaces what it held. That keeps the picture and the JSON the single
 * source of each other, which is what lets the page hand you the raw JSON to edit directly.
 */
@RestController
@RequestMapping("/sequencediagrams")
public class SequenceDiagramController {

    private final SequenceDiagramService service;

    public SequenceDiagramController(SequenceDiagramService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<List<SequenceDiagram>> list() {
        return ResponseEntity.ok(service.list());
    }

    /**
     * Which applications run on these hosts — {@code ?servers=host1,host2}, or one {@code servers}
     * parameter per name. Newlines and semicolons split too, so a list pasted out of a change
     * ticket needs no reformatting first.
     *
     * <p>Sits above {@code /{diagramId}} so the literal path wins over the variable one.
     */
    @GetMapping("/servers")
    public ResponseEntity<List<ServerHit>> byServers(@RequestParam(name = "servers", required = false) List<String> servers) {
        List<String> names = servers == null ? List.of()
                : servers.stream().flatMap(s -> Arrays.stream(s.split("[,;\r\n]"))).toList();
        return ResponseEntity.ok(service.findByServers(names));
    }

    @GetMapping("/{diagramId}")
    public ResponseEntity<?> get(@PathVariable String diagramId) {
        SequenceDiagram diagram = service.get(diagramId);
        return diagram == null ? notFound(diagramId) : ResponseEntity.ok(diagram);
    }

    /** Mints the unique id; whatever {@code diagramId} the body carries is ignored. */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> create(@RequestBody SequenceDiagram diagram) {
        return save(() -> service.create(diagram));
    }

    @PutMapping(value = "/{diagramId}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> update(@PathVariable String diagramId, @RequestBody SequenceDiagram diagram) {
        if (service.get(diagramId) == null) return notFound(diagramId);
        return save(() -> service.update(diagramId, diagram));
    }

    @PostMapping("/{diagramId}/clone")
    public ResponseEntity<?> clone(@PathVariable String diagramId, @RequestParam(required = false) String title) {
        if (service.get(diagramId) == null) return notFound(diagramId);
        return save(() -> service.copyOf(diagramId, title));
    }

    @DeleteMapping("/{diagramId}")
    public ResponseEntity<?> delete(@PathVariable String diagramId) {
        if (service.get(diagramId) == null) return notFound(diagramId);
        return save(() -> {
            service.delete(diagramId);
            return Map.of("status", "deleted", "diagramId", diagramId);
        });
    }

    @FunctionalInterface
    private interface DiagramAction {
        Object run() throws Exception;
    }

    /** Turns the service's validation failures into the {@code {"error": "..."}} the page shows. */
    private ResponseEntity<?> save(DiagramAction action) {
        try {
            return ResponseEntity.ok(action.run());
        } catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        } catch (Exception e) {
            return badRequest("Failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    private ResponseEntity<Map<String, Object>> notFound(String diagramId) {
        return ResponseEntity.status(404).body(Map.of("error", "Diagram not found: " + diagramId));
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        return ResponseEntity.badRequest().body(Map.of("error", message));
    }
}
