package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Milestone;
import com.mycompany.taskmanagement.model.Project;
import com.mycompany.taskmanagement.repository.MilestoneRepository;
import com.mycompany.taskmanagement.repository.ProjectRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaProjectDataStore implements ProjectDataStore {

    private final ProjectRepository projectRepository;
    private final MilestoneRepository milestoneRepository;

    @Override
    public List<Project> findAll() {
        return projectRepository.findAllByOrderByNameAsc();
    }

    @Override
    public Optional<Project> findById(Long id) {
        return projectRepository.findById(id);
    }

    @Override
    public Optional<Project> findByName(String name) {
        return projectRepository.findByNameIgnoreCase(name);
    }

    @Override
    public Project save(Project project) {
        return projectRepository.save(project);
    }

    @Override
    public void deleteById(Long id) {
        projectRepository.deleteById(id);
    }

    @Override
    public List<Milestone> findAllMilestones() {
        return milestoneRepository.findAll();
    }

    @Override
    public List<Milestone> findMilestonesByProjectId(Long projectId) {
        return milestoneRepository.findByProjectIdOrderByTargetDateAsc(projectId);
    }

    @Override
    public Milestone saveMilestone(Milestone milestone) {
        return milestoneRepository.save(milestone);
    }

    @Override
    public void deleteMilestone(Long milestoneId) {
        milestoneRepository.deleteById(milestoneId);
    }

    @Override
    public void deleteMilestonesByProjectId(Long projectId) {
        milestoneRepository.deleteByProjectId(projectId);
    }
}
