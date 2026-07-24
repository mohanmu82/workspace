package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.model.Milestone;
import com.mycompany.taskmanagement.model.Project;
import com.mycompany.taskmanagement.service.ProjectService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/projects")
@RequiredArgsConstructor
public class ProjectController {

    private final ProjectService projectService;

    @GetMapping
    public List<Project> list() {
        return projectService.list();
    }

    @GetMapping("/all-milestones")
    public List<Milestone> allMilestones() {
        return projectService.getAllMilestones();
    }

    @GetMapping("/{id}")
    public Project getById(@PathVariable Long id) {
        return projectService.getById(id);
    }

    @PostMapping
    public Project create(@Valid @RequestBody Project project) {
        return projectService.create(project);
    }

    @PutMapping("/{id}")
    public Project update(@PathVariable Long id, @Valid @RequestBody Project project) {
        return projectService.update(id, project);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        projectService.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{id}/milestones")
    public List<Milestone> milestones(@PathVariable Long id) {
        return projectService.getMilestones(id);
    }

    @PostMapping("/{id}/milestones")
    public Milestone addMilestone(@PathVariable Long id, @Valid @RequestBody Milestone milestone) {
        return projectService.addMilestone(id, milestone);
    }

    @PutMapping("/{id}/milestones/{milestoneId}")
    public Milestone updateMilestone(@PathVariable Long id, @PathVariable Long milestoneId,
                                     @Valid @RequestBody Milestone milestone) {
        return projectService.updateMilestone(id, milestoneId, milestone);
    }

    @DeleteMapping("/{id}/milestones/{milestoneId}")
    public ResponseEntity<Void> deleteMilestone(@PathVariable Long id, @PathVariable Long milestoneId) {
        projectService.deleteMilestone(id, milestoneId);
        return ResponseEntity.noContent().build();
    }
}
