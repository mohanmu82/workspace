package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Programme;
import com.mycompany.taskmanagement.repository.ProgrammeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaProgrammeDataStore implements ProgrammeDataStore {

    private final ProgrammeRepository programmeRepository;

    @Override
    public List<Programme> findAll() {
        return programmeRepository.findAllByOrderByNameAsc();
    }

    @Override
    public Optional<Programme> findById(Long id) {
        return programmeRepository.findById(id);
    }

    @Override
    public Optional<Programme> findByName(String name) {
        return programmeRepository.findByNameIgnoreCase(name);
    }

    @Override
    public Programme save(Programme programme) {
        return programmeRepository.save(programme);
    }

    @Override
    public void deleteById(Long id) {
        programmeRepository.deleteById(id);
    }
}
