package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.Milestone;
import com.mycompany.taskmanagement.model.Project;
import com.mycompany.taskmanagement.store.ProjectDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProjectService {

    private final ProjectDataStore dataStore;

    public List<Project> list() {
        return dataStore.findAll();
    }

    public Project getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Project not found: " + id));
    }

    @Transactional
    public Project create(Project project) {
        return dataStore.save(project);
    }

    @Transactional
    public Project update(Long id, Project updated) {
        Project existing = getById(id);
        existing.setName(updated.getName());
        existing.setDescription(updated.getDescription());
        existing.setOwner(updated.getOwner());
        existing.setStatus(updated.getStatus());
        existing.setStartDate(updated.getStartDate());
        existing.setEndDate(updated.getEndDate());
        return dataStore.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        dataStore.deleteMilestonesByProjectId(id);
        dataStore.deleteById(id);
    }

    public List<Milestone> getAllMilestones() {
        return dataStore.findAllMilestones();
    }

    public List<Milestone> getMilestones(Long projectId) {
        return dataStore.findMilestonesByProjectId(projectId);
    }

    @Transactional
    public Milestone addMilestone(Long projectId, Milestone milestone) {
        getById(projectId);
        milestone.setId(null);
        milestone.setProjectId(projectId);
        return dataStore.saveMilestone(milestone);
    }

    @Transactional
    public Milestone updateMilestone(Long projectId, Long milestoneId, Milestone updated) {
        getById(projectId);
        Milestone existing = dataStore.findMilestonesByProjectId(projectId).stream()
                .filter(m -> m.getId().equals(milestoneId))
                .findFirst()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Milestone not found: " + milestoneId));
        existing.setName(updated.getName());
        existing.setDescription(updated.getDescription());
        existing.setTargetDate(updated.getTargetDate());
        existing.setStatus(updated.getStatus());
        return dataStore.saveMilestone(existing);
    }

    @Transactional
    public void deleteMilestone(Long projectId, Long milestoneId) {
        dataStore.deleteMilestone(milestoneId);
    }
}
