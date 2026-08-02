package com.mycompany.taskmanagement.store;

import com.mycompany.taskmanagement.model.Note;
import com.mycompany.taskmanagement.repository.NoteRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@Profile("!json")
@RequiredArgsConstructor
public class JpaNoteDataStore implements NoteDataStore {

    private final NoteRepository noteRepository;

    @Override
    public List<Note> findAll() {
        return noteRepository.findAllByOrderByUpdatedAtDesc();
    }

    @Override
    public Optional<Note> findById(Long id) {
        return noteRepository.findById(id);
    }

    @Override
    public Note save(Note note) {
        return noteRepository.save(note);
    }

    @Override
    public void deleteById(Long id) {
        noteRepository.deleteById(id);
    }
}
