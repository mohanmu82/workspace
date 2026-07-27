package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.model.TaskTemplate;
import com.mycompany.taskmanagement.service.TaskTemplateService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/task-templates")
@RequiredArgsConstructor
public class TaskTemplateController {

    private final TaskTemplateService templateService;

    @GetMapping
    public List<TaskTemplate> list() {
        return templateService.list();
    }

    @GetMapping("/{id}")
    public TaskTemplate getById(@PathVariable Long id) {
        return templateService.getById(id);
    }

    @PostMapping
    public TaskTemplate create(@Valid @RequestBody TaskTemplate template) {
        return templateService.create(template);
    }

    @PutMapping("/{id}")
    public TaskTemplate update(@PathVariable Long id, @Valid @RequestBody TaskTemplate template) {
        return templateService.update(id, template);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        templateService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
