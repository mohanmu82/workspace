package com.mycompany.batch.web;

import com.mycompany.batch.staticdataset.StaticDatasetDef;
import com.mycompany.batch.staticdataset.StaticDatasetService;
import com.mycompany.batch.vault.VaultService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Posts rows into the Obsidian vault named by {@code obsidian.vault-dir}: any grid or run report
 * the browser sends, and static datasets by name.
 */
@RestController
@RequestMapping("/vault")
public class VaultController {

    public record NoteRequest(String path, Map<String, Object> properties,
                              List<String> columns, List<Map<String, Object>> rows) {}

    private final VaultService vault;
    private final StaticDatasetService datasets;

    public VaultController(VaultService vault, StaticDatasetService datasets) {
        this.vault = vault;
        this.datasets = datasets;
    }

    @PostMapping("/note")
    public ResponseEntity<?> note(@RequestBody NoteRequest req) {
        return write(req.path(), req.properties(), req.columns(), req.rows());
    }

    /** The dataset's loaded rows as {@code Datasets/<name>.md}, overwritten on every post. */
    @PostMapping("/dataset/{name}")
    public ResponseEntity<?> dataset(@PathVariable String name) {
        StaticDatasetDef def = datasets.get(name);
        if (def == null) return ResponseEntity.status(404).body(Map.of("error", "Static dataset not found: " + name));
        StaticDatasetService.DatasetState s = datasets.getState(name);
        List<Map<String, Object>> rows = s != null ? s.rows() : List.of();

        Map<String, Object> props = new LinkedHashMap<>();
        props.put("source", "static-dataset");
        props.put("dataset", name);
        props.put("rows", rows.size());
        props.put("loadedTime", s != null ? s.loadedTime() : null);
        props.put("error", s != null ? s.error() : null);
        return write("Datasets/" + name, props, s != null ? s.attributes() : def.getAttributes(), rows);
    }

    private ResponseEntity<?> write(String path, Map<String, Object> props,
                                    List<String> columns, List<Map<String, Object>> rows) {
        try {
            Path file = vault.writeNote(path, props, columns, rows);
            return ResponseEntity.ok(Map.of("file", file.toString()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", "Failed to write note: " + e.getMessage()));
        }
    }
}
