package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.dto.TaskListResponse;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskHistory;
import com.mycompany.taskmanagement.repository.TaskCommentRepository;
import com.mycompany.taskmanagement.repository.TaskHistoryRepository;
import com.mycompany.taskmanagement.repository.TaskRepository;
import com.mycompany.taskmanagement.repository.TaskSpecification;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;
    private final TaskCommentRepository commentRepository;
    private final TaskHistoryRepository historyRepository;

    public TaskListResponse search(TaskSearchRequest req) {
        Sort sort = req.getSortDir().equalsIgnoreCase("desc")
                ? Sort.by(req.getSortBy()).descending()
                : Sort.by(req.getSortBy()).ascending();

        PageRequest pageReq = PageRequest.of(req.getPage(), req.getSize(), sort);
        Page<Task> page = taskRepository.findAll(TaskSpecification.withSearch(req), pageReq);
        return new TaskListResponse(page.getContent(), page.getTotalElements(),
                req.getPage(), req.getSize());
    }

    public Task getById(Long id) {
        return taskRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Task not found: " + id));
    }

    @Transactional
    public Task create(Task task) {
        return taskRepository.save(task);
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
            historyRepository.saveAll(histories);
        }
        return taskRepository.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        commentRepository.deleteByTaskId(id);
        historyRepository.deleteByTaskId(id);
        taskRepository.deleteById(id);
    }

    public List<TaskComment> getComments(Long taskId) {
        return commentRepository.findByTaskIdOrderByCreatedAtAsc(taskId);
    }

    @Transactional
    public TaskComment addComment(Long taskId, String content, String author) {
        TaskComment comment = new TaskComment();
        comment.setTaskId(taskId);
        comment.setContent(content);
        comment.setAuthor(author);
        return commentRepository.save(comment);
    }

    public List<TaskHistory> getHistory(Long taskId) {
        return historyRepository.findByTaskIdOrderByChangedAtDesc(taskId);
    }

    public Map<String, List<String>> getFilterOptions() {
        return Map.of(
                "categories", taskRepository.findDistinctCategories(),
                "programmes", taskRepository.findDistinctProgrammes(),
                "projects", taskRepository.findDistinctProjects(),
                "assignees", taskRepository.findDistinctAssignees(),
                "workingGroups", taskRepository.findDistinctWorkingGroups(),
                "assetClasses", taskRepository.findDistinctAssetClasses()
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
