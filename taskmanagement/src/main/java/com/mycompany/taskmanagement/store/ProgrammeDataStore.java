package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Programme;

import java.util.List;
import java.util.Optional;

public interface ProgrammeDataStore {
    List<Programme> findAll();
    Optional<Programme> findById(Long id);
    Optional<Programme> findByName(String name);
    Programme save(Programme programme);
    void deleteById(Long id);
}
