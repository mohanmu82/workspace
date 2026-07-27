package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.TaskTemplate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TaskTemplateRepository extends JpaRepository<TaskTemplate, Long> {
}
