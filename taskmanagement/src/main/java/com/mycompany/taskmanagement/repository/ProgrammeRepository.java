package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.Programme;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ProgrammeRepository extends JpaRepository<Programme, Long> {
    Optional<Programme> findByNameIgnoreCase(String name);
    List<Programme> findAllByOrderByNameAsc();
}
