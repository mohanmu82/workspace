package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.TaskTemplate;
import com.mycompany.taskmanagement.store.TaskTemplateDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TaskTemplateService {

    private final TaskTemplateDataStore dataStore;

    public List<TaskTemplate> list() {
        return dataStore.findAll();
    }

    public TaskTemplate getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task template not found: " + id));
    }

    @Transactional
    public TaskTemplate create(TaskTemplate template) {
        template.setId(null);
        return dataStore.save(template);
    }

    @Transactional
    public TaskTemplate update(Long id, TaskTemplate updated) {
        TaskTemplate existing = getById(id);
        existing.setName(updated.getName());
        existing.setTitle(updated.getTitle());
        existing.setDescription(updated.getDescription());
        existing.setPriority(updated.getPriority());
        existing.setCategory(updated.getCategory());
        existing.setTags(updated.getTags());
        existing.setEstimatedHours(updated.getEstimatedHours());
        existing.setProgramme(updated.getProgramme());
        existing.setProject(updated.getProject());
        existing.setAssetClass(updated.getAssetClass());
        existing.setWorkingGroup(updated.getWorkingGroup());
        return dataStore.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        getById(id); // throws 404 if the template doesn't exist
        dataStore.deleteById(id);
    }
}
