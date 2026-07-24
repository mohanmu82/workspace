package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.Programme;
import com.mycompany.taskmanagement.store.ProgrammeDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProgrammeService {

    private final ProgrammeDataStore dataStore;

    public List<Programme> list() {
        return dataStore.findAll();
    }

    public Programme getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new RuntimeException("Programme not found: " + id));
    }

    @Transactional
    public Programme create(Programme programme) {
        return dataStore.save(programme);
    }

    @Transactional
    public Programme update(Long id, Programme updated) {
        Programme existing = getById(id);
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
        dataStore.deleteById(id);
    }
}
