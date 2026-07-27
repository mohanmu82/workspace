package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskDependency;
import com.mycompany.taskmanagement.model.TaskHistory;
import org.springframework.data.domain.Page;

import java.util.List;
import java.util.Optional;

public interface TaskDataStore {
    Page<Task> search(TaskSearchRequest req);
    Optional<Task> findById(Long id);
    Task save(Task task);
    void deleteById(Long id);

    List<TaskComment> findCommentsByTaskId(Long taskId);
    TaskComment saveComment(TaskComment comment);
    void deleteCommentsByTaskId(Long taskId);

    List<TaskHistory> findHistoryByTaskId(Long taskId);
    void saveAllHistory(List<TaskHistory> histories);
    void deleteHistoryByTaskId(Long taskId);

    List<String> findDistinctCategories();
    List<String> findDistinctProgrammes();
    List<String> findDistinctProjects();
    List<String> findDistinctAssignees();
    List<String> findDistinctWorkingGroups();
    List<String> findDistinctAssetClasses();

    List<TaskDependency> findAllDependencies();
    List<TaskDependency> findDependenciesByTaskId(Long taskId);
    List<TaskDependency> findDependentsByTaskId(Long taskId);
    TaskDependency saveDependency(TaskDependency dependency);
    void deleteDependency(Long taskId, Long dependsOnTaskId);
    void deleteDependenciesForTask(Long taskId);
}
