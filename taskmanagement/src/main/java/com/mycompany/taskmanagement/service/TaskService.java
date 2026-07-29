package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.dto.TaskBulkCreateRequest;
import com.mycompany.taskmanagement.dto.TaskDependencyGraphResponse;
import com.mycompany.taskmanagement.dto.TaskListResponse;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.event.TaskChangedEvent;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskDependency;
import com.mycompany.taskmanagement.model.TaskHistory;
import com.mycompany.taskmanagement.store.TaskDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class TaskService {

    private static final int MAX_PAGE_SIZE = 500;

    private static final Set<String> SORTABLE_FIELDS = Set.of(
            "id", "title", "status", "priority", "assignee", "createdBy", "dueDate",
            "createdAt", "updatedAt", "category", "programme", "project", "assetClass",
            "workingGroup", "stakeholder", "jira", "estimatedHours", "actualHours");

    private final TaskDataStore dataStore;
    private final ApplicationEventPublisher eventPublisher;

    public TaskListResponse search(TaskSearchRequest req) {
        if (req.getPage() < 0) {
            req.setPage(0);
        }
        if (req.getSize() < 1) {
            req.setSize(1);
        } else if (req.getSize() > MAX_PAGE_SIZE) {
            req.setSize(MAX_PAGE_SIZE);
        }
        if (req.getSortBy() == null || !SORTABLE_FIELDS.contains(req.getSortBy())) {
            req.setSortBy("dueDate");
        }
        if (req.getSortDir() == null
                || !(req.getSortDir().equalsIgnoreCase("asc") || req.getSortDir().equalsIgnoreCase("desc"))) {
            req.setSortDir("asc");
        }
        Page<Task> page = dataStore.search(req);
        return new TaskListResponse(page.getContent(), page.getTotalElements(),
                req.getPage(), req.getSize());
    }

    public Task getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found: " + id));
    }

    @Transactional
    public Task create(Task task) {
        Task saved = dataStore.save(task);
        eventPublisher.publishEvent(TaskChangedEvent.created(saved));
        return saved;
    }

    @Transactional
    public Task update(Long id, Task updated, String changedBy) {
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();

        record(histories, id, "title", existing.getTitle(), updated.getTitle(), changedBy);
        record(histories, id, "description", existing.getDescription(), updated.getDescription(), changedBy);
        record(histories, id, "status", existing.getStatus(), updated.getStatus(), changedBy);
        record(histories, id, "priority", existing.getPriority(), updated.getPriority(), changedBy);
        record(histories, id, "assignee", existing.getAssignee(), updated.getAssignee(), changedBy);
        record(histories, id, "createdBy", existing.getCreatedBy(), updated.getCreatedBy(), changedBy);
        record(histories, id, "dueDate", existing.getDueDate(), updated.getDueDate(), changedBy);
        record(histories, id, "category", existing.getCategory(), updated.getCategory(), changedBy);
        record(histories, id, "tags", existing.getTags(), updated.getTags(), changedBy);
        record(histories, id, "estimatedHours", existing.getEstimatedHours(), updated.getEstimatedHours(), changedBy);
        record(histories, id, "actualHours", existing.getActualHours(), updated.getActualHours(), changedBy);
        record(histories, id, "programme", existing.getProgramme(), updated.getProgramme(), changedBy);
        record(histories, id, "project", existing.getProject(), updated.getProject(), changedBy);
        record(histories, id, "assetClass", existing.getAssetClass(), updated.getAssetClass(), changedBy);
        record(histories, id, "workingGroup", existing.getWorkingGroup(), updated.getWorkingGroup(), changedBy);
        record(histories, id, "stakeholder", existing.getStakeholder(), updated.getStakeholder(), changedBy);
        record(histories, id, "jira", existing.getJira(), updated.getJira(), changedBy);
        record(histories, id, "links", existing.getLinks(), updated.getLinks(), changedBy);
        record(histories, id, "parentTaskId", existing.getParentTaskId(), updated.getParentTaskId(), changedBy);
        record(histories, id, "customField1", existing.getCustomField1(), updated.getCustomField1(), changedBy);
        record(histories, id, "customField2", existing.getCustomField2(), updated.getCustomField2(), changedBy);
        record(histories, id, "customField3", existing.getCustomField3(), updated.getCustomField3(), changedBy);
        record(histories, id, "customField4", existing.getCustomField4(), updated.getCustomField4(), changedBy);
        record(histories, id, "customField5", existing.getCustomField5(), updated.getCustomField5(), changedBy);
        record(histories, id, "customField6", existing.getCustomField6(), updated.getCustomField6(), changedBy);
        record(histories, id, "customField7", existing.getCustomField7(), updated.getCustomField7(), changedBy);
        record(histories, id, "customField8", existing.getCustomField8(), updated.getCustomField8(), changedBy);
        record(histories, id, "customField9", existing.getCustomField9(), updated.getCustomField9(), changedBy);
        record(histories, id, "customField10", existing.getCustomField10(), updated.getCustomField10(), changedBy);

        existing.setTitle(updated.getTitle());
        existing.setDescription(updated.getDescription());
        existing.setStatus(updated.getStatus());
        existing.setPriority(updated.getPriority());
        existing.setAssignee(updated.getAssignee());
        existing.setCreatedBy(updated.getCreatedBy());
        existing.setDueDate(updated.getDueDate());
        existing.setCategory(updated.getCategory());
        existing.setTags(updated.getTags());
        existing.setEstimatedHours(updated.getEstimatedHours());
        existing.setActualHours(updated.getActualHours());
        existing.setProgramme(updated.getProgramme());
        existing.setProject(updated.getProject());
        existing.setAssetClass(updated.getAssetClass());
        existing.setWorkingGroup(updated.getWorkingGroup());
        existing.setStakeholder(updated.getStakeholder());
        existing.setJira(updated.getJira());
        existing.setLinks(updated.getLinks());
        existing.setParentTaskId(updated.getParentTaskId());
        existing.setCustomField1(updated.getCustomField1());
        existing.setCustomField2(updated.getCustomField2());
        existing.setCustomField3(updated.getCustomField3());
        existing.setCustomField4(updated.getCustomField4());
        existing.setCustomField5(updated.getCustomField5());
        existing.setCustomField6(updated.getCustomField6());
        existing.setCustomField7(updated.getCustomField7());
        existing.setCustomField8(updated.getCustomField8());
        existing.setCustomField9(updated.getCustomField9());
        existing.setCustomField10(updated.getCustomField10());

        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    @Transactional
    public Task updateStatus(Long id, String status, String changedBy) {
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("status must not be blank");
        }
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();
        record(histories, id, "status", existing.getStatus(), status, changedBy);
        existing.setStatus(status);
        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    @Transactional
    public Task updateBoardPosition(Long id, String status, String project, String programme, String assignee, String changedBy) {
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();
        if (status != null) {
            record(histories, id, "status", existing.getStatus(), status, changedBy);
            existing.setStatus(status);
        }
        if (project != null) {
            String newProject = project.isBlank() ? null : project;
            record(histories, id, "project", existing.getProject(), newProject, changedBy);
            existing.setProject(newProject);
        }
        if (programme != null) {
            String newProgramme = programme.isBlank() ? null : programme;
            record(histories, id, "programme", existing.getProgramme(), newProgramme, changedBy);
            existing.setProgramme(newProgramme);
        }
        if (assignee != null) {
            String newAssignee = assignee.isBlank() ? null : assignee;
            record(histories, id, "assignee", existing.getAssignee(), newAssignee, changedBy);
            existing.setAssignee(newAssignee);
        }
        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    @Transactional
    public Task updateDueDate(Long id, LocalDateTime dueDate, String changedBy) {
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();
        record(histories, id, "dueDate", existing.getDueDate(), dueDate, changedBy);
        existing.setDueDate(dueDate);
        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    @Transactional
    public Task updatePhase(Long id, String phase, String changedBy) {
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();
        String newPhase = (phase == null || phase.isBlank()) ? null : phase;
        record(histories, id, "customField1", existing.getCustomField1(), newPhase, changedBy);
        existing.setCustomField1(newPhase);
        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    @Transactional
    public Task updateCustomField(Long id, int fieldNumber, String value, String changedBy) {
        if (fieldNumber < 1 || fieldNumber > 10) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "fieldNumber must be between 1 and 10");
        }
        Task existing = getById(id);
        List<TaskHistory> histories = new ArrayList<>();
        String fieldName = "customField" + fieldNumber;
        String newValue = (value == null || value.isBlank()) ? null : value;
        String oldValue = getCustomField(existing, fieldNumber);
        record(histories, id, fieldName, oldValue, newValue, changedBy);
        setCustomField(existing, fieldNumber, newValue);
        if (!histories.isEmpty()) {
            dataStore.saveAllHistory(histories);
        }
        Task saved = dataStore.save(existing);
        eventPublisher.publishEvent(TaskChangedEvent.updated(saved));
        return saved;
    }

    private String getCustomField(Task task, int fieldNumber) {
        return switch (fieldNumber) {
            case 1 -> task.getCustomField1();
            case 2 -> task.getCustomField2();
            case 3 -> task.getCustomField3();
            case 4 -> task.getCustomField4();
            case 5 -> task.getCustomField5();
            case 6 -> task.getCustomField6();
            case 7 -> task.getCustomField7();
            case 8 -> task.getCustomField8();
            case 9 -> task.getCustomField9();
            case 10 -> task.getCustomField10();
            default -> throw new IllegalArgumentException("Invalid fieldNumber: " + fieldNumber);
        };
    }

    private void setCustomField(Task task, int fieldNumber, String value) {
        switch (fieldNumber) {
            case 1 -> task.setCustomField1(value);
            case 2 -> task.setCustomField2(value);
            case 3 -> task.setCustomField3(value);
            case 4 -> task.setCustomField4(value);
            case 5 -> task.setCustomField5(value);
            case 6 -> task.setCustomField6(value);
            case 7 -> task.setCustomField7(value);
            case 8 -> task.setCustomField8(value);
            case 9 -> task.setCustomField9(value);
            case 10 -> task.setCustomField10(value);
            default -> throw new IllegalArgumentException("Invalid fieldNumber: " + fieldNumber);
        }
    }

    @Transactional
    public List<Task> bulkCreate(TaskBulkCreateRequest req) {
        List<Task> created = new ArrayList<>();
        if (req.getTitles() == null) {
            return created;
        }
        for (String rawTitle : req.getTitles()) {
            if (rawTitle == null || rawTitle.isBlank()) {
                continue;
            }
            Task task = new Task();
            task.setTitle(rawTitle.trim());
            task.setProject(req.getProject());
            task.setProgramme(req.getProgramme());
            Task saved = dataStore.save(task);
            created.add(saved);
            eventPublisher.publishEvent(TaskChangedEvent.created(saved));
        }
        return created;
    }

    @Transactional
    public List<Task> bulkImport(List<Task> tasks) {
        List<Task> created = new ArrayList<>();
        for (Task task : tasks) {
            if (task.getTitle() == null || task.getTitle().isBlank()) {
                continue;
            }
            Task saved = dataStore.save(task);
            created.add(saved);
            eventPublisher.publishEvent(TaskChangedEvent.created(saved));
        }
        return created;
    }

    @Transactional
    public void delete(Long id) {
        getById(id); // throws 404 if the task doesn't exist
        dataStore.deleteCommentsByTaskId(id);
        dataStore.deleteHistoryByTaskId(id);
        dataStore.deleteDependenciesForTask(id);
        dataStore.deleteById(id);
        eventPublisher.publishEvent(TaskChangedEvent.deleted(id));
    }

    public List<TaskComment> getComments(Long taskId) {
        return dataStore.findCommentsByTaskId(taskId);
    }

    @Transactional
    public TaskComment addComment(Long taskId, String content, String author) {
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("content must not be blank");
        }
        getById(taskId); // throws 404 if the task doesn't exist
        TaskComment comment = new TaskComment();
        comment.setTaskId(taskId);
        comment.setContent(content);
        comment.setAuthor(author);
        return dataStore.saveComment(comment);
    }

    public List<TaskHistory> getHistory(Long taskId) {
        return dataStore.findHistoryByTaskId(taskId);
    }

    public TaskDependencyGraphResponse getDependencyGraph() {
        TaskSearchRequest req = new TaskSearchRequest();
        req.setSortBy("id");
        req.setSortDir("asc");
        req.setSize(10000);
        List<Task> tasks = search(req).getTasks();
        return new TaskDependencyGraphResponse(tasks, dataStore.findAllDependencies());
    }

    public List<Task> getDependencies(Long taskId) {
        getById(taskId); // throws 404 if the task doesn't exist
        return dataStore.findDependenciesByTaskId(taskId).stream()
                .map(d -> dataStore.findById(d.getDependsOnTaskId()).orElse(null))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<Task> getDependents(Long taskId) {
        getById(taskId); // throws 404 if the task doesn't exist
        return dataStore.findDependentsByTaskId(taskId).stream()
                .map(d -> dataStore.findById(d.getTaskId()).orElse(null))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Transactional
    public TaskDependency addDependency(Long taskId, Long dependsOnTaskId) {
        if (dependsOnTaskId == null) {
            throw new IllegalArgumentException("dependsOnTaskId must not be null");
        }
        if (taskId.equals(dependsOnTaskId)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A task cannot depend on itself");
        }
        getById(taskId); // throws 404 if either task doesn't exist
        getById(dependsOnTaskId);

        boolean alreadyExists = dataStore.findDependenciesByTaskId(taskId).stream()
                .anyMatch(d -> d.getDependsOnTaskId().equals(dependsOnTaskId));
        if (alreadyExists) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Dependency already exists");
        }
        if (createsCycle(taskId, dependsOnTaskId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Adding this dependency would create a circular dependency");
        }

        TaskDependency dependency = new TaskDependency();
        dependency.setTaskId(taskId);
        dependency.setDependsOnTaskId(dependsOnTaskId);
        return dataStore.saveDependency(dependency);
    }

    @Transactional
    public void removeDependency(Long taskId, Long dependsOnTaskId) {
        dataStore.deleteDependency(taskId, dependsOnTaskId);
    }

    // Would linking taskId -> dependsOnTaskId create a cycle? True if dependsOnTaskId
    // can already reach taskId by following existing "depends on" edges.
    private boolean createsCycle(Long taskId, Long dependsOnTaskId) {
        Deque<Long> queue = new ArrayDeque<>();
        Set<Long> visited = new HashSet<>();
        queue.add(dependsOnTaskId);
        visited.add(dependsOnTaskId);
        while (!queue.isEmpty()) {
            Long current = queue.poll();
            if (current.equals(taskId)) {
                return true;
            }
            for (TaskDependency d : dataStore.findDependenciesByTaskId(current)) {
                if (visited.add(d.getDependsOnTaskId())) {
                    queue.add(d.getDependsOnTaskId());
                }
            }
        }
        return false;
    }

    public Map<String, List<String>> getFilterOptions() {
        return Map.of(
                "categories", dataStore.findDistinctCategories(),
                "programmes", dataStore.findDistinctProgrammes(),
                "projects", dataStore.findDistinctProjects(),
                "assignees", dataStore.findDistinctAssignees(),
                "workingGroups", dataStore.findDistinctWorkingGroups(),
                "assetClasses", dataStore.findDistinctAssetClasses()
        );
    }

    private void record(List<TaskHistory> histories, Long taskId, String field,
                        Object oldVal, Object newVal, String changedBy) {
        String oldStr = oldVal != null ? oldVal.toString() : null;
        String newStr = newVal != null ? newVal.toString() : null;
        if (!Objects.equals(oldStr, newStr)) {
            histories.add(TaskHistory.builder()
                    .taskId(taskId)
                    .fieldName(field)
                    .oldValue(oldStr)
                    .newValue(newStr)
                    .changedBy(changedBy)
                    .changedAt(LocalDateTime.now())
                    .build());
        }
    }
}
