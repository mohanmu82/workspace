package com.mycompany.taskmanagement.repository;

import com.mycompany.taskmanagement.model.Note;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface NoteRepository extends JpaRepository<Note, Long> {
    List<Note> findAllByOrderByUpdatedAtDesc();
}
