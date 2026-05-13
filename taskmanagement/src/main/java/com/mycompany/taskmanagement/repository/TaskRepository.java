package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long>, JpaSpecificationExecutor<Task> {

    @Query("SELECT DISTINCT t.category FROM Task t WHERE t.category IS NOT NULL ORDER BY t.category")
    List<String> findDistinctCategories();

    @Query("SELECT DISTINCT t.programme FROM Task t WHERE t.programme IS NOT NULL ORDER BY t.programme")
    List<String> findDistinctProgrammes();

    @Query("SELECT DISTINCT t.project FROM Task t WHERE t.project IS NOT NULL ORDER BY t.project")
    List<String> findDistinctProjects();

    @Query("SELECT DISTINCT t.assignee FROM Task t WHERE t.assignee IS NOT NULL ORDER BY t.assignee")
    List<String> findDistinctAssignees();

    @Query("SELECT DISTINCT t.workingGroup FROM Task t WHERE t.workingGroup IS NOT NULL ORDER BY t.workingGroup")
    List<String> findDistinctWorkingGroups();

    @Query("SELECT DISTINCT t.assetClass FROM Task t WHERE t.assetClass IS NOT NULL ORDER BY t.assetClass")
    List<String> findDistinctAssetClasses();
}
