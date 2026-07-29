package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.dto.TaskBulkCreateRequest;
import com.mycompany.taskmanagement.dto.TaskDependencyGraphResponse;
import com.mycompany.taskmanagement.dto.TaskListResponse;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskDependency;
import com.mycompany.taskmanagement.model.TaskHistory;
import com.mycompany.taskmanagement.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    @PatchMapping("/{id}/status")
    public Task updateStatus(@PathVariable Long id,
                             @RequestBody Map<String, String> body,
                             @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        return taskService.updateStatus(id, body.get("status"), changedBy);
    }

    @PatchMapping("/{id}/board")
    public Task updateBoardPosition(@PathVariable Long id,
                                    @RequestBody Map<String, String> body,
                                    @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        return taskService.updateBoardPosition(id, body.get("status"), body.get("project"), body.get("programme"), body.get("assignee"), changedBy);
    }

    @PatchMapping("/{id}/due-date")
    public Task updateDueDate(@PathVariable Long id,
                              @RequestBody Map<String, String> body,
                              @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        String dueDate = body.get("dueDate");
        LocalDateTime parsed = (dueDate == null || dueDate.isBlank()) ? null : LocalDateTime.parse(dueDate);
        return taskService.updateDueDate(id, parsed, changedBy);
    }

    @PatchMapping("/{id}/phase")
    public Task updatePhase(@PathVariable Long id,
                            @RequestBody Map<String, String> body,
                            @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        return taskService.updatePhase(id, body.get("phase"), changedBy);
    }

    @PatchMapping("/{id}/custom-field/{fieldNumber}")
    public Task updateCustomField(@PathVariable Long id,
                                  @PathVariable int fieldNumber,
                                  @RequestBody Map<String, String> body,
                                  @RequestHeader(value = "X-Changed-By", defaultValue = "system") String changedBy) {
        return taskService.updateCustomField(id, fieldNumber, body.get("value"), changedBy);
    }

    @PostMapping("/bulk")
    public List<Task> bulkCreate(@RequestBody TaskBulkCreateRequest req) {
        return taskService.bulkCreate(req);
    }

    /**
     * Bulk-creates tasks from an uploaded CSV file. Expected header row (case-insensitive,
     * any subset, any order): title,description,status,priority,assignee,dueDate,category,
     * programme,project,assetClass,workingGroup,stakeholder,jira,tags,estimatedHours.
     * Only "title" is required; rows without one are skipped. Tags are semicolon-separated.
     */
    @PostMapping("/import")
    public Map<String, Object> importCsv(@RequestParam("file") MultipartFile file) throws IOException {
        List<Task> parsed = new ArrayList<>();
        int skipped = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return Map.of("created", 0, "skipped", 0);
            }
            List<String> headers = parseCsvLine(headerLine).stream()
                    .map(h -> h.trim().toLowerCase())
                    .collect(Collectors.toList());
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> cells = parseCsvLine(line);
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < headers.size() && i < cells.size(); i++) {
                    row.put(headers.get(i), cells.get(i).trim());
                }
                Task task = taskFromCsvRow(row);
                if (task == null) {
                    skipped++;
                    continue;
                }
                parsed.add(task);
            }
        }
        List<Task> created = taskService.bulkImport(parsed);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("created", created.size());
        result.put("skipped", skipped);
        return result;
    }

    private Task taskFromCsvRow(Map<String, String> row) {
        String title = blankToNull(row.get("title"));
        if (title == null) {
            return null;
        }
        Task task = new Task();
        task.setTitle(title);
        task.setDescription(blankToNull(row.get("description")));
        if (blankToNull(row.get("status")) != null) task.setStatus(row.get("status").trim());
        if (blankToNull(row.get("priority")) != null) task.setPriority(row.get("priority").trim());
        task.setAssignee(blankToNull(row.get("assignee")));
        task.setCategory(blankToNull(row.get("category")));
        task.setProgramme(blankToNull(row.get("programme")));
        task.setProject(blankToNull(row.get("project")));
        task.setAssetClass(blankToNull(row.get("assetclass")));
        task.setWorkingGroup(blankToNull(row.get("workinggroup")));
        task.setStakeholder(blankToNull(row.get("stakeholder")));
        task.setJira(blankToNull(row.get("jira")));

        String due = blankToNull(row.get("duedate"));
        if (due != null) {
            try {
                task.setDueDate(LocalDateTime.parse(due));
            } catch (Exception e) {
                try {
                    task.setDueDate(LocalDate.parse(due).atStartOfDay());
                } catch (Exception ignored) {
                    // leave dueDate unset if it can't be parsed
                }
            }
        }

        String tags = blankToNull(row.get("tags"));
        if (tags != null) {
            task.setTags(Arrays.stream(tags.split(";"))
                    .map(String::trim)
                    .filter(t -> !t.isEmpty())
                    .collect(Collectors.toList()));
        }

        String estimatedHours = blankToNull(row.get("estimatedhours"));
        if (estimatedHours != null) {
            try {
                task.setEstimatedHours(new BigDecimal(estimatedHours));
            } catch (NumberFormatException ignored) {
                // leave estimatedHours unset if it isn't a valid number
            }
        }
        return task;
    }

    private String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                result.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        result.add(cur.toString());
        return result;
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

    @GetMapping("/dependencies/graph")
    public TaskDependencyGraphResponse getDependencyGraph() {
        return taskService.getDependencyGraph();
    }

    @GetMapping("/{id}/dependencies")
    public List<Task> getDependencies(@PathVariable Long id) {
        return taskService.getDependencies(id);
    }

    @GetMapping("/{id}/dependents")
    public List<Task> getDependents(@PathVariable Long id) {
        return taskService.getDependents(id);
    }

    @PostMapping("/{id}/dependencies")
    public TaskDependency addDependency(@PathVariable Long id, @RequestBody Map<String, Long> body) {
        return taskService.addDependency(id, body.get("dependsOnTaskId"));
    }

    @DeleteMapping("/{id}/dependencies/{dependsOnTaskId}")
    public ResponseEntity<Void> removeDependency(@PathVariable Long id, @PathVariable Long dependsOnTaskId) {
        taskService.removeDependency(id, dependsOnTaskId);
        return ResponseEntity.noContent().build();
    }
}
