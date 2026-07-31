package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.model.Note;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.service.NoteService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/notes")
@RequiredArgsConstructor
public class NoteController {

    private final NoteService noteService;

    @GetMapping
    public List<Note> list() {
        return noteService.list();
    }

    @GetMapping("/{id}")
    public Note getById(@PathVariable Long id) {
        return noteService.getById(id);
    }

    @PostMapping
    public Note create(@Valid @RequestBody Note note) {
        return noteService.create(note);
    }

    @PutMapping("/{id}")
    public Note update(@PathVariable Long id, @Valid @RequestBody Note note) {
        return noteService.update(id, note);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        noteService.delete(id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/create-task")
    public Task createTask(@PathVariable Long id, @RequestBody Map<String, Object> body) {
        Long dependsOnTaskId = body.get("dependsOnTaskId") != null
                ? Long.valueOf(body.get("dependsOnTaskId").toString())
                : null;
        String title = body.get("title") != null ? body.get("title").toString() : null;
        return noteService.createTaskFromNote(id, dependsOnTaskId, title);
    }
}
