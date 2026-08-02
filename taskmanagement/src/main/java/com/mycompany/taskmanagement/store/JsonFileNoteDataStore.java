package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.Note;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile("json")
public class JsonFileNoteDataStore implements NoteDataStore {

    private final ObjectMapper objectMapper;
    private final Path notesDir;
    private final AtomicLong noteSeq = new AtomicLong(0);

    public JsonFileNoteDataStore(ObjectMapper objectMapper,
                                 @Value("${app.notes.directory:./data/notes}") String notesDirectory) {
        this.objectMapper = objectMapper;
        this.notesDir = Paths.get(notesDirectory);
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(notesDir);
        noteSeq.set(maxIdInDir(notesDir));
    }

    // ---- Notes ----
    // Stored one file per note: {notesDir}/{id}.json

    @Override
    public List<Note> findAll() {
        List<Note> all = loadAllNotes();
        all.sort(java.util.Comparator.comparing(Note::getUpdatedAt, java.util.Comparator.nullsLast(java.util.Comparator.reverseOrder())));
        return all;
    }

    @Override
    public Optional<Note> findById(Long id) {
        Path file = noteFile(id);
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), Note.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized Note save(Note note) {
        if (note.getId() == null) {
            note.setId(noteSeq.incrementAndGet());
            note.setCreatedAt(LocalDateTime.now());
        }
        note.setUpdatedAt(LocalDateTime.now());
        writeJson(noteFile(note.getId()), note);
        return note;
    }

    @Override
    public synchronized void deleteById(Long id) {
        silentDelete(noteFile(id));
    }

    // ---- I/O helpers ----

    private Path noteFile(Long id) {
        return notesDir.resolve(id + ".json");
    }

    private List<Note> loadAllNotes() {
        try (Stream<Path> files = Files.list(notesDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), Note.class); }
                        catch (IOException e) { throw new UncheckedIOException(e); }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeJson(Path path, Object value) {
        try {
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void silentDelete(Path path) {
        try { Files.deleteIfExists(path); } catch (IOException ignored) {}
    }

    private long maxIdInDir(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString().replace(".json", ""))
                    .filter(s -> s.matches("\\d+"))
                    .mapToLong(Long::parseLong)
                    .max().orElse(0L);
        }
    }
}
