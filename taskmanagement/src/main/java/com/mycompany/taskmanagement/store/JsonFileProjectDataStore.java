package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.Milestone;
import com.mycompany.taskmanagement.model.Project;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile("json")
public class JsonFileProjectDataStore implements ProjectDataStore {

    private final ObjectMapper objectMapper;
    private final Path projectsDir;
    private final Path milestonesFile;
    private final AtomicLong projectSeq = new AtomicLong(0);
    private final AtomicLong milestoneSeq = new AtomicLong(0);

    public JsonFileProjectDataStore(ObjectMapper objectMapper,
                                    @Value("${app.projects.directory:./data/projects}") String projectsDirectory,
                                    @Value("${app.milestones.file:./data/milestones.json}") String milestonesFilePath) {
        this.objectMapper = objectMapper;
        this.projectsDir = Paths.get(projectsDirectory);
        this.milestonesFile = Paths.get(milestonesFilePath);
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(projectsDir);
        if (milestonesFile.getParent() != null) Files.createDirectories(milestonesFile.getParent());
        projectSeq.set(maxIdInDir(projectsDir));
        milestoneSeq.set(readAllMilestones().stream().mapToLong(m -> m.getId() == null ? 0 : m.getId()).max().orElse(0L));
    }

    // ---- Projects ----
    // Stored one file per project: {projectsDir}/{id}.json

    @Override
    public List<Project> findAll() {
        List<Project> all = loadAllProjects();
        all.sort(java.util.Comparator.comparing(Project::getName, java.util.Comparator.nullsLast(String::compareTo)));
        return all;
    }

    @Override
    public Optional<Project> findById(Long id) {
        Path file = projectFile(id);
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), Project.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<Project> findByName(String name) {
        return loadAllProjects().stream().filter(p -> p.getName().equalsIgnoreCase(name)).findFirst();
    }

    @Override
    public synchronized Project save(Project project) {
        if (project.getId() == null) {
            project.setId(projectSeq.incrementAndGet());
            project.setCreatedAt(LocalDateTime.now());
        }
        project.setUpdatedAt(LocalDateTime.now());
        writeJson(projectFile(project.getId()), project);
        return project;
    }

    @Override
    public synchronized void deleteById(Long id) {
        silentDelete(projectFile(id));
    }

    // ---- Milestones ----
    // Kept as a single array file: {milestonesFile}

    @Override
    public List<Milestone> findAllMilestones() {
        return readAllMilestones();
    }

    @Override
    public List<Milestone> findMilestonesByProjectId(Long projectId) {
        return readAllMilestones().stream()
                .filter(m -> m.getProjectId().equals(projectId))
                .sorted(java.util.Comparator.comparing(Milestone::getTargetDate,
                        java.util.Comparator.nullsLast(java.util.Comparator.naturalOrder())))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized Milestone saveMilestone(Milestone milestone) {
        List<Milestone> all = readAllMilestones();
        if (milestone.getId() == null) {
            milestone.setId(milestoneSeq.incrementAndGet());
            milestone.setCreatedAt(LocalDateTime.now());
            all.add(milestone);
        } else {
            all.replaceAll(m -> m.getId().equals(milestone.getId()) ? milestone : m);
        }
        milestone.setUpdatedAt(LocalDateTime.now());
        writeJson(milestonesFile, all);
        return milestone;
    }

    @Override
    public synchronized void deleteMilestone(Long milestoneId) {
        List<Milestone> all = readAllMilestones();
        all.removeIf(m -> m.getId().equals(milestoneId));
        writeJson(milestonesFile, all);
    }

    @Override
    public synchronized void deleteMilestonesByProjectId(Long projectId) {
        List<Milestone> all = readAllMilestones();
        all.removeIf(m -> m.getProjectId().equals(projectId));
        writeJson(milestonesFile, all);
    }

    // ---- I/O helpers ----

    private Path projectFile(Long id) {
        return projectsDir.resolve(id + ".json");
    }

    private List<Project> loadAllProjects() {
        try (Stream<Path> files = Files.list(projectsDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), Project.class); }
                        catch (IOException e) { throw new UncheckedIOException(e); }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Milestone> readAllMilestones() {
        if (!Files.exists(milestonesFile)) return new ArrayList<>();
        try {
            return objectMapper.readValue(milestonesFile.toFile(), new TypeReference<ArrayList<Milestone>>() {});
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
