package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.TaskHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TaskHistoryRepository extends JpaRepository<TaskHistory, Long> {
    List<TaskHistory> findByTaskIdOrderByChangedAtDesc(Long taskId);
    List<TaskHistory> findAllByOrderByChangedAtDesc(Pageable pageable);
    void deleteByTaskId(Long taskId);
}
