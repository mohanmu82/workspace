package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.Event;
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
public class JsonFileEventDataStore implements EventDataStore {

    private final ObjectMapper objectMapper;
    private final Path eventsDir;
    private final AtomicLong eventSeq = new AtomicLong(0);

    public JsonFileEventDataStore(ObjectMapper objectMapper,
                                  @Value("${app.events.directory:./data/events}") String eventsDirectory) {
        this.objectMapper = objectMapper;
        this.eventsDir = Paths.get(eventsDirectory);
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(eventsDir);
        eventSeq.set(maxIdInDir(eventsDir));
    }

    // ---- Events ----
    // Stored one file per event: {eventsDir}/{id}.json

    @Override
    public List<Event> findAll() {
        List<Event> all = loadAllEvents();
        all.sort(java.util.Comparator.comparing(Event::getStartDate, java.util.Comparator.nullsLast(java.util.Comparator.naturalOrder())));
        return all;
    }

    @Override
    public Optional<Event> findById(Long id) {
        Path file = eventFile(id);
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), Event.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized Event save(Event event) {
        if (event.getId() == null) {
            event.setId(eventSeq.incrementAndGet());
            event.setCreatedAt(LocalDateTime.now());
        }
        event.setUpdatedAt(LocalDateTime.now());
        writeJson(eventFile(event.getId()), event);
        return event;
    }

    @Override
    public synchronized void deleteById(Long id) {
        silentDelete(eventFile(id));
    }

    // ---- I/O helpers ----

    private Path eventFile(Long id) {
        return eventsDir.resolve(id + ".json");
    }

    private List<Event> loadAllEvents() {
        try (Stream<Path> files = Files.list(eventsDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), Event.class); }
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
