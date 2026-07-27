package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.TaskTemplate;

import java.util.List;
import java.util.Optional;

public interface TaskTemplateDataStore {
    List<TaskTemplate> findAll();
    Optional<TaskTemplate> findById(Long id);
    TaskTemplate save(TaskTemplate template);
    void deleteById(Long id);
}
