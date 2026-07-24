package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskHistory;
import com.mycompany.taskmanagement.repository.TaskCommentRepository;
import com.mycompany.taskmanagement.repository.TaskHistoryRepository;
import com.mycompany.taskmanagement.repository.TaskRepository;
import com.mycompany.taskmanagement.repository.TaskSpecification;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaTaskDataStore implements TaskDataStore {

    private final TaskRepository taskRepository;
    private final TaskCommentRepository commentRepository;
    private final TaskHistoryRepository historyRepository;

    @Override
    public Page<Task> search(TaskSearchRequest req) {
        Sort sort = req.getSortDir().equalsIgnoreCase("desc")
                ? Sort.by(req.getSortBy()).descending()
                : Sort.by(req.getSortBy()).ascending();
        return taskRepository.findAll(TaskSpecification.withSearch(req),
                PageRequest.of(req.getPage(), req.getSize(), sort));
    }

    @Override
    public Optional<Task> findById(Long id) {
        return taskRepository.findById(id);
    }

    @Override
    public Task save(Task task) {
        return taskRepository.save(task);
    }

    @Override
    public void deleteById(Long id) {
        taskRepository.deleteById(id);
    }

    @Override
    public List<TaskComment> findCommentsByTaskId(Long taskId) {
        return commentRepository.findByTaskIdOrderByCreatedAtAsc(taskId);
    }

    @Override
    public TaskComment saveComment(TaskComment comment) {
        return commentRepository.save(comment);
    }

    @Override
    public void deleteCommentsByTaskId(Long taskId) {
        commentRepository.deleteByTaskId(taskId);
    }

    @Override
    public List<TaskHistory> findHistoryByTaskId(Long taskId) {
        return historyRepository.findByTaskIdOrderByChangedAtDesc(taskId);
    }

    @Override
    public void saveAllHistory(List<TaskHistory> histories) {
        historyRepository.saveAll(histories);
    }

    @Override
    public void deleteHistoryByTaskId(Long taskId) {
        historyRepository.deleteByTaskId(taskId);
    }

    @Override
    public List<String> findDistinctCategories() { return taskRepository.findDistinctCategories(); }
    @Override
    public List<String> findDistinctProgrammes() { return taskRepository.findDistinctProgrammes(); }
    @Override
    public List<String> findDistinctProjects() { return taskRepository.findDistinctProjects(); }
    @Override
    public List<String> findDistinctAssignees() { return taskRepository.findDistinctAssignees(); }
    @Override
    public List<String> findDistinctWorkingGroups() { return taskRepository.findDistinctWorkingGroups(); }
    @Override
    public List<String> findDistinctAssetClasses() { return taskRepository.findDistinctAssetClasses(); }
}
