package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Note;

import java.util.List;
import java.util.Optional;

public interface NoteDataStore {
    List<Note> findAll();
    Optional<Note> findById(Long id);
    Note save(Note note);
    void deleteById(Long id);
}
