package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.TaskTemplate;
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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile("json")
public class JsonFileTaskTemplateDataStore implements TaskTemplateDataStore {

    private final ObjectMapper objectMapper;
    private final Path templatesDir;
    private final AtomicLong templateSeq = new AtomicLong(0);

    public JsonFileTaskTemplateDataStore(ObjectMapper objectMapper,
                                         @Value("${app.task-templates.directory:./data/task-templates}") String directory) {
        this.objectMapper = objectMapper;
        this.templatesDir = Paths.get(directory);
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(templatesDir);
        templateSeq.set(maxIdInDir(templatesDir));
    }

    @Override
    public List<TaskTemplate> findAll() {
        List<TaskTemplate> all = loadAll();
        all.sort(Comparator.comparing(TaskTemplate::getName, Comparator.nullsLast(String::compareTo)));
        return all;
    }

    @Override
    public Optional<TaskTemplate> findById(Long id) {
        Path file = templateFile(id);
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), TaskTemplate.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized TaskTemplate save(TaskTemplate template) {
        if (template.getId() == null) {
            template.setId(templateSeq.incrementAndGet());
            template.setCreatedAt(LocalDateTime.now());
        }
        template.setUpdatedAt(LocalDateTime.now());
        writeJson(templateFile(template.getId()), template);
        return template;
    }

    @Override
    public synchronized void deleteById(Long id) {
        try {
            Files.deleteIfExists(templateFile(id));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path templateFile(Long id) {
        return templatesDir.resolve(id + ".json");
    }

    private List<TaskTemplate> loadAll() {
        try (Stream<Path> files = Files.list(templatesDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), TaskTemplate.class); }
                        catch (IOException e) { throw new UncheckedIOException(e); }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeJson(Path path, Object value) {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
