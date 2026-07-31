package com.mycompany.taskmanagement.service;

import com.mycompany.taskmanagement.model.Note;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.store.NoteDataStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class NoteService {

    private static final String DEPENDENCY_PREFIX = "Dependency: ";
    private static final int TITLE_SNIPPET_LENGTH = 80;

    private final NoteDataStore dataStore;
    private final TaskService taskService;

    public List<Note> list() {
        return dataStore.findAll();
    }

    public Note getById(Long id) {
        return dataStore.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Note not found: " + id));
    }

    @Transactional
    public Note create(Note note) {
        note.setId(null);
        return dataStore.save(note);
    }

    @Transactional
    public Note update(Long id, Note updated) {
        Note existing = getById(id);
        existing.setDescription(updated.getDescription());
        existing.setAssignee(updated.getAssignee());
        existing.setCustomField1(updated.getCustomField1());
        existing.setCustomField2(updated.getCustomField2());
        existing.setCustomField3(updated.getCustomField3());
        existing.setCustomField4(updated.getCustomField4());
        existing.setCustomField5(updated.getCustomField5());
        return dataStore.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        dataStore.deleteById(id);
    }

    /**
     * Spins a new prerequisite Task out of a Note. If dependsOnTaskId is given, the existing
     * task is made to depend on the new task (i.e. the note becomes a blocking prerequisite).
     */
    @Transactional
    public Task createTaskFromNote(Long noteId, Long dependsOnTaskId, String title) {
        Note note = getById(noteId);

        String resolvedTitle = (title != null && !title.isBlank()) ? title.trim() : snippet(note.getDescription());
        Task task = new Task();
        task.setTitle(DEPENDENCY_PREFIX + resolvedTitle);
        task.setDescription(note.getDescription());
        task.setAssignee(note.getAssignee());
        Task created = taskService.create(task);

        if (dependsOnTaskId != null) {
            taskService.addDependency(dependsOnTaskId, created.getId());
        }
        return created;
    }

    private String snippet(String description) {
        if (description == null || description.isBlank()) {
            return "Note";
        }
        String trimmed = description.trim();
        return trimmed.length() > TITLE_SNIPPET_LENGTH
                ? trimmed.substring(0, TITLE_SNIPPET_LENGTH) + "..."
                : trimmed;
    }
}
