package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.Programme;
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
public class JsonFileProgrammeDataStore implements ProgrammeDataStore {

    private final ObjectMapper objectMapper;
    private final Path programmesDir;
    private final AtomicLong programmeSeq = new AtomicLong(0);

    public JsonFileProgrammeDataStore(ObjectMapper objectMapper,
                                      @Value("${app.programmes.directory:./data/programmes}") String programmesDirectory) {
        this.objectMapper = objectMapper;
        this.programmesDir = Paths.get(programmesDirectory);
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(programmesDir);
        programmeSeq.set(maxIdInDir(programmesDir));
    }

    // ---- Programmes ----
    // Stored one file per programme: {programmesDir}/{id}.json

    @Override
    public List<Programme> findAll() {
        List<Programme> all = loadAllProgrammes();
        all.sort(java.util.Comparator.comparing(Programme::getName, java.util.Comparator.nullsLast(String::compareTo)));
        return all;
    }

    @Override
    public Optional<Programme> findById(Long id) {
        Path file = programmeFile(id);
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), Programme.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<Programme> findByName(String name) {
        return loadAllProgrammes().stream().filter(p -> p.getName().equalsIgnoreCase(name)).findFirst();
    }

    @Override
    public synchronized Programme save(Programme programme) {
        if (programme.getId() == null) {
            programme.setId(programmeSeq.incrementAndGet());
            programme.setCreatedAt(LocalDateTime.now());
        }
        programme.setUpdatedAt(LocalDateTime.now());
        writeJson(programmeFile(programme.getId()), programme);
        return programme;
    }

    @Override
    public synchronized void deleteById(Long id) {
        silentDelete(programmeFile(id));
    }

    // ---- I/O helpers ----

    private Path programmeFile(Long id) {
        return programmesDir.resolve(id + ".json");
    }

    private List<Programme> loadAllProgrammes() {
        try (Stream<Path> files = Files.list(programmesDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), Programme.class); }
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
