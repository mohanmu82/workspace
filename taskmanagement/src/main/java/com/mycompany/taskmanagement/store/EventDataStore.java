package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Event;

import java.util.List;
import java.util.Optional;

public interface EventDataStore {
    List<Event> findAll();
    Optional<Event> findById(Long id);
    Event save(Event event);
    void deleteById(Long id);
}
