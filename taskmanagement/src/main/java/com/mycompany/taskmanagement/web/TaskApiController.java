package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.dto.TaskListResponse;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskHistory;
import com.mycompany.taskmanagement.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskApiController {

    private final TaskService taskService;

    @GetMapping
    public TaskListResponse search(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<String> status,
            @RequestParam(required = false) List<String> priority,
            @RequestParam(required = false) String assignee,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueFrom,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueTo,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String programme,
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String workingGroup,
            @RequestParam(required = false) String assetClass,
            @RequestParam(required = false) String stakeholder,
            @RequestParam(required = false) String jira,
            @RequestParam(required = false) List<String> tags,
            @RequestParam(required = false) String groupBy,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "200") int size,
            @RequestParam(defaultValue = "createdAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {

        TaskSearchRequest req = new TaskSearchRequest();
        req.setQ(q);
        req.setStatus(status);
        req.setPriority(priority);
        req.setAssignee(assignee);
        req.setDueFrom(dueFrom);
        req.setDueTo(dueTo);
        req.setCategory(category);
        req.setProgramme(programme);
        req.setProject(project);
        req.setWorkingGroup(workingGroup);
        req.setAssetClass(assetClass);
        req.setStakeholder(stakeholder);
        req.setJira(jira);
        req.setTags(tags);
        req.setGroupBy(groupBy);
        req.setPage(page);
        req.setSize(size);
        req.setSortBy(sortBy);
        req.setSortDir(sortDir);

        return taskService.search(req);
    }

    @GetMapping("/{id}")
    public Task getById(@PathVariable Long id) {
        return taskService.getById(id);
    }

    @PostMapping
    public Task create(@Valid @RequestBody Task task) {
        return taskService.create(task);
    }

    @PutMapping("/{id}")
    public Task update(@PathVariable Long id,
                       @Valid @RequestBody Task task,
                       @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        return taskService.update(id, task, changedBy);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        taskService.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{id}/comments")
    public List<TaskComment> getComments(@PathVariable Long id) {
        return taskService.getComments(id);
    }

    @PostMapping("/{id}/comments")
    public TaskComment addComment(@PathVariable Long id,
                                  @RequestBody Map<String, String> body) {
        return taskService.addComment(id, body.get("content"), body.get("author"));
    }

    @GetMapping("/{id}/history")
    public List<TaskHistory> getHistory(@PathVariable Long id) {
        return taskService.getHistory(id);
    }

    @GetMapping("/filter-options")
    public Map<String, List<String>> getFilterOptions() {
        return taskService.getFilterOptions();
    }
}
