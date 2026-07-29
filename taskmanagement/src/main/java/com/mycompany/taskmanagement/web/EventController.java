package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.model.Event;
import com.mycompany.taskmanagement.service.EventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;

    @GetMapping
    public List<Event> list() {
        return eventService.list();
    }

    @GetMapping("/{id}")
    public Event getById(@PathVariable Long id) {
        return eventService.getById(id);
    }

    @PostMapping
    public Event create(@Valid @RequestBody Event event) {
        return eventService.create(event);
    }

    @PutMapping("/{id}")
    public Event update(@PathVariable Long id, @Valid @RequestBody Event event) {
        return eventService.update(id, event);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        eventService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
