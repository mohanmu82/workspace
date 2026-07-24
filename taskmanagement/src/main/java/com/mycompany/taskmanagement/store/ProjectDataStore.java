package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Milestone;
import com.mycompany.taskmanagement.model.Project;

import java.util.List;
import java.util.Optional;

public interface ProjectDataStore {
    List<Project> findAll();
    Optional<Project> findById(Long id);
    Optional<Project> findByName(String name);
    Project save(Project project);
    void deleteById(Long id);

    List<Milestone> findAllMilestones();
    List<Milestone> findMilestonesByProjectId(Long projectId);
    Milestone saveMilestone(Milestone milestone);
    void deleteMilestone(Long milestoneId);
    void deleteMilestonesByProjectId(Long projectId);
}
