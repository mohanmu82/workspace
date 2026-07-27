package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.TaskDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TaskDependencyRepository extends JpaRepository<TaskDependency, Long> {
    List<TaskDependency> findByTaskId(Long taskId);
    List<TaskDependency> findByDependsOnTaskId(Long dependsOnTaskId);
    void deleteByTaskIdAndDependsOnTaskId(Long taskId, Long dependsOnTaskId);
    void deleteByTaskId(Long taskId);
    void deleteByDependsOnTaskId(Long dependsOnTaskId);
}
