package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Event;
import com.mycompany.taskmanagement.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaEventDataStore implements EventDataStore {

    private final EventRepository eventRepository;

    @Override
    public List<Event> findAll() {
        return eventRepository.findAllByOrderByStartDateAsc();
    }

    @Override
    public Optional<Event> findById(Long id) {
        return eventRepository.findById(id);
    }

    @Override
    public Event save(Event event) {
        return eventRepository.save(event);
    }

    @Override
    public void deleteById(Long id) {
        eventRepository.deleteById(id);
    }
}
