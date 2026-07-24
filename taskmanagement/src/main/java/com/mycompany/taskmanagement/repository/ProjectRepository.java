package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.Project;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ProjectRepository extends JpaRepository<Project, Long> {
    Optional<Project> findByNameIgnoreCase(String name);
    List<Project> findAllByOrderByNameAsc();
}
