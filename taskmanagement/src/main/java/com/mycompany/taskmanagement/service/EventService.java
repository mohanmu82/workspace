package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.Event;
import com.mycompany.taskmanagement.store.EventDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EventService {

    private final EventDataStore dataStore;

    public List<Event> list() {
        return dataStore.findAll();
    }

    public Event getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Event not found: " + id));
    }

    @Transactional
    public Event create(Event event) {
        event.setId(null);
        return dataStore.save(event);
    }

    @Transactional
    public Event update(Long id, Event updated) {
        Event existing = getById(id);
        existing.setTitle(updated.getTitle());
        existing.setType(updated.getType());
        existing.setDescription(updated.getDescription());
        existing.setStartDate(updated.getStartDate());
        existing.setEndDate(updated.getEndDate());
        return dataStore.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        dataStore.deleteById(id);
    }
}
