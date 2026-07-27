package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.TaskTemplate;
import com.mycompany.taskmanagement.repository.TaskTemplateRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaTaskTemplateDataStore implements TaskTemplateDataStore {

    private final TaskTemplateRepository repository;

    @Override
    public List<TaskTemplate> findAll() {
        return repository.findAll().stream()
                .sorted(Comparator.comparing(TaskTemplate::getName, Comparator.nullsLast(String::compareTo)))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<TaskTemplate> findById(Long id) {
        return repository.findById(id);
    }

    @Override
    public TaskTemplate save(TaskTemplate template) {
        return repository.save(template);
    }

    @Override
    public void deleteById(Long id) {
        repository.deleteById(id);
    }
}
