package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.dto.FieldConfig;
import com.mycompany.taskmanagement.service.FieldConfigService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class DropdownConfigController {

    private final FieldConfigService fieldConfigService;

    @GetMapping("/api/field-configs")
    public Map<String, FieldConfig> getAll() {
        return fieldConfigService.getAll();
    }

    @PutMapping("/api/field-configs/{fieldId}")
    public FieldConfig save(@PathVariable String fieldId,
                            @RequestBody FieldConfig config) {
        return fieldConfigService.save(fieldId, config);
    }

    @DeleteMapping("/api/field-configs/{fieldId}")
    public ResponseEntity<Void> delete(@PathVariable String fieldId) {
        fieldConfigService.delete(fieldId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/api/field-options/{fieldId}")
    public ResponseEntity<?> getOptions(@PathVariable String fieldId) {
        try {
            return ResponseEntity.ok(fieldConfigService.getOptions(fieldId));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/api/field-options/test")
    public ResponseEntity<?> testOptions(@RequestBody FieldConfig.DropdownSource source) {
        try {
            return ResponseEntity.ok(fieldConfigService.testHttpOptions(source));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
