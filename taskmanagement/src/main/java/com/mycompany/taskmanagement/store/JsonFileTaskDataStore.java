package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskComment;
import com.mycompany.taskmanagement.model.TaskDependency;
import com.mycompany.taskmanagement.model.TaskHistory;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile("json")
public class JsonFileTaskDataStore implements TaskDataStore {

    private final ObjectMapper objectMapper;
    private final Path tasksDir;
    private final Path commentsDir;
    private final Path historyDir;
    private final Path dependenciesFile;
    private final AtomicLong taskSeq = new AtomicLong(0);
    private final AtomicLong commentSeq = new AtomicLong(0);
    private final AtomicLong historySeq = new AtomicLong(0);
    private final AtomicLong dependencySeq = new AtomicLong(0);

    public JsonFileTaskDataStore(ObjectMapper objectMapper,
                                 @Value("${app.json.datasource.directory:./data}") String directory) {
        this.objectMapper = objectMapper;
        Path base = Paths.get(directory);
        this.tasksDir = base.resolve("tasks");
        this.commentsDir = base.resolve("comments");
        this.historyDir = base.resolve("history");
        this.dependenciesFile = base.resolve("dependencies.json");
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(tasksDir);
        Files.createDirectories(commentsDir);
        Files.createDirectories(historyDir);
        taskSeq.set(maxIdInDir(tasksDir));
        commentSeq.set(scanMaxIdInGroupedFiles(commentsDir, new TypeReference<List<TaskComment>>() {}, c -> c.getId()));
        historySeq.set(scanMaxIdInGroupedFiles(historyDir, new TypeReference<List<TaskHistory>>() {}, h -> h.getId()));
        if (!Files.exists(dependenciesFile)) {
            writeJson(dependenciesFile, new ArrayList<TaskDependency>());
        }
        long maxDepId = 0;
        for (TaskDependency d : readDependencies()) {
            if (d.getId() != null && d.getId() > maxDepId) maxDepId = d.getId();
        }
        dependencySeq.set(maxDepId);
    }

    // ---- Tasks ----

    @Override
    public Page<Task> search(TaskSearchRequest req) {
        List<Task> filtered = loadAllTasks().stream()
                .filter(t -> matches(t, req))
                .sorted(comparatorFor(req.getSortBy(), req.getSortDir()))
                .collect(Collectors.toList());
        int from = req.getPage() * req.getSize();
        int to = Math.min(from + req.getSize(), filtered.size());
        List<Task> pageContent = from >= filtered.size() ? List.of() : filtered.subList(from, to);
        return new PageImpl<>(pageContent, PageRequest.of(req.getPage(), req.getSize()), filtered.size());
    }

    @Override
    public Optional<Task> findById(Long id) {
        Path file = tasksDir.resolve(id + ".json");
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(file.toFile(), Task.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Task save(Task task) {
        if (task.getId() == null) {
            task.setId(taskSeq.incrementAndGet());
            task.setCreatedAt(LocalDateTime.now());
        }
        task.setUpdatedAt(LocalDateTime.now());
        writeJson(tasksDir.resolve(task.getId() + ".json"), task);
        return task;
    }

    @Override
    public void deleteById(Long id) {
        silentDelete(tasksDir.resolve(id + ".json"));
    }

    // ---- Comments ----
    // Stored as an array per task: comments/task-{taskId}.json

    @Override
    public List<TaskComment> findCommentsByTaskId(Long taskId) {
        return readComments(taskId).stream()
                .sorted(Comparator.comparing(c -> c.getCreatedAt() != null ? c.getCreatedAt() : LocalDateTime.MIN))
                .collect(Collectors.toList());
    }

    @Override
    public TaskComment saveComment(TaskComment comment) {
        List<TaskComment> list = readComments(comment.getTaskId());
        if (comment.getId() == null) {
            comment.setId(commentSeq.incrementAndGet());
            comment.setCreatedAt(LocalDateTime.now());
            list.add(comment);
        } else {
            list.replaceAll(c -> c.getId().equals(comment.getId()) ? comment : c);
        }
        writeJson(commentsDir.resolve("task-" + comment.getTaskId() + ".json"), list);
        return comment;
    }

    @Override
    public void deleteCommentsByTaskId(Long taskId) {
        silentDelete(commentsDir.resolve("task-" + taskId + ".json"));
    }

    // ---- History ----
    // Stored as an array per task: history/task-{taskId}.json

    @Override
    public List<TaskHistory> findHistoryByTaskId(Long taskId) {
        return readHistory(taskId).stream()
                .sorted(Comparator.comparing(
                        (TaskHistory h) -> h.getChangedAt() != null ? h.getChangedAt() : LocalDateTime.MIN).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public void saveAllHistory(List<TaskHistory> histories) {
        histories.stream()
                .collect(Collectors.groupingBy(TaskHistory::getTaskId))
                .forEach((taskId, entries) -> {
                    List<TaskHistory> existing = readHistory(taskId);
                    for (TaskHistory h : entries) {
                        if (h.getId() == null) h.setId(historySeq.incrementAndGet());
                        existing.add(h);
                    }
                    writeJson(historyDir.resolve("task-" + taskId + ".json"), existing);
                });
    }

    @Override
    public void deleteHistoryByTaskId(Long taskId) {
        silentDelete(historyDir.resolve("task-" + taskId + ".json"));
    }

    // ---- Filter options ----

    @Override
    public List<String> findDistinctCategories() { return distinctValues(Task::getCategory); }
    @Override
    public List<String> findDistinctProgrammes() { return distinctValues(Task::getProgramme); }
    @Override
    public List<String> findDistinctProjects() { return distinctValues(Task::getProject); }
    @Override
    public List<String> findDistinctAssignees() { return distinctValues(Task::getAssignee); }
    @Override
    public List<String> findDistinctWorkingGroups() { return distinctValues(Task::getWorkingGroup); }
    @Override
    public List<String> findDistinctAssetClasses() { return distinctValues(Task::getAssetClass); }

    // ---- Dependencies ----
    // Stored as a single array file: dependencies.json

    @Override
    public List<TaskDependency> findAllDependencies() {
        return readDependencies();
    }

    @Override
    public List<TaskDependency> findDependenciesByTaskId(Long taskId) {
        return readDependencies().stream()
                .filter(d -> d.getTaskId().equals(taskId))
                .collect(Collectors.toList());
    }

    @Override
    public List<TaskDependency> findDependentsByTaskId(Long taskId) {
        return readDependencies().stream()
                .filter(d -> d.getDependsOnTaskId().equals(taskId))
                .collect(Collectors.toList());
    }

    @Override
    public TaskDependency saveDependency(TaskDependency dependency) {
        List<TaskDependency> all = readDependencies();
        if (dependency.getId() == null) {
            dependency.setId(dependencySeq.incrementAndGet());
            dependency.setCreatedAt(LocalDateTime.now());
            all.add(dependency);
        } else {
            all.replaceAll(d -> d.getId().equals(dependency.getId()) ? dependency : d);
        }
        writeJson(dependenciesFile, all);
        return dependency;
    }

    @Override
    public void deleteDependency(Long taskId, Long dependsOnTaskId) {
        List<TaskDependency> all = readDependencies();
        boolean removed = all.removeIf(d -> d.getTaskId().equals(taskId) && d.getDependsOnTaskId().equals(dependsOnTaskId));
        if (removed) {
            writeJson(dependenciesFile, all);
        }
    }

    @Override
    public void deleteDependenciesForTask(Long taskId) {
        List<TaskDependency> all = readDependencies();
        boolean removed = all.removeIf(d -> d.getTaskId().equals(taskId) || d.getDependsOnTaskId().equals(taskId));
        if (removed) {
            writeJson(dependenciesFile, all);
        }
    }

    private List<TaskDependency> readDependencies() {
        if (!Files.exists(dependenciesFile)) return new ArrayList<>();
        try { return objectMapper.readValue(dependenciesFile.toFile(), new TypeReference<>() {}); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    // ---- I/O helpers ----

    private List<Task> loadAllTasks() {
        try (Stream<Path> files = Files.list(tasksDir)) {
            return files.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> {
                        try { return objectMapper.readValue(p.toFile(), Task.class); }
                        catch (IOException e) { throw new UncheckedIOException(e); }
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<TaskComment> readComments(Long taskId) {
        Path file = commentsDir.resolve("task-" + taskId + ".json");
        if (!Files.exists(file)) return new ArrayList<>();
        try { return objectMapper.readValue(file.toFile(), new TypeReference<>() {}); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private List<TaskHistory> readHistory(Long taskId) {
        Path file = historyDir.resolve("task-" + taskId + ".json");
        if (!Files.exists(file)) return new ArrayList<>();
        try { return objectMapper.readValue(file.toFile(), new TypeReference<>() {}); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private void writeJson(Path path, Object value) {
        try { objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value); }
        catch (IOException e) { throw new UncheckedIOException(e); }
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

    @FunctionalInterface
    private interface IdExtractor<T> { Long getId(T item); }

    private <T> long scanMaxIdInGroupedFiles(Path dir, TypeReference<List<T>> typeRef, IdExtractor<T> extractor)
            throws IOException {
        long max = 0;
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.collect(Collectors.toList())) {
                List<T> items = objectMapper.readValue(f.toFile(), typeRef);
                for (T item : items) {
                    Long id = extractor.getId(item);
                    if (id != null && id > max) max = id;
                }
            }
        }
        return max;
    }

    private List<String> distinctValues(java.util.function.Function<Task, String> extractor) {
        return loadAllTasks().stream()
                .map(extractor)
                .filter(v -> v != null && !v.isBlank())
                .distinct().sorted()
                .collect(Collectors.toList());
    }

    // ---- In-memory filtering ----

    private boolean matches(Task t, TaskSearchRequest req) {
        if (hasText(req.getQ())) {
            String q = req.getQ().toLowerCase();
            boolean hit = contains(t.getTitle(), q) || contains(t.getDescription(), q)
                    || contains(t.getAssignee(), q) || contains(t.getCategory(), q)
                    || contains(t.getProgramme(), q) || contains(t.getProject(), q)
                    || contains(t.getStakeholder(), q)
                    || (t.getTags() != null && t.getTags().stream().anyMatch(tag -> contains(tag, q)));
            if (!hit) return false;
        }
        if (req.getStatus() != null && !req.getStatus().isEmpty()
                && !req.getStatus().contains(t.getStatus())) return false;
        if (req.getPriority() != null && !req.getPriority().isEmpty()
                && !req.getPriority().contains(t.getPriority())) return false;
        if (hasText(req.getAssignee()) && !contains(t.getAssignee(), req.getAssignee().toLowerCase())) return false;
        if (req.getDueFrom() != null
                && (t.getDueDate() == null || t.getDueDate().isBefore(req.getDueFrom().atStartOfDay()))) return false;
        if (req.getDueTo() != null
                && (t.getDueDate() == null || t.getDueDate().isAfter(req.getDueTo().atTime(23, 59, 59)))) return false;
        if (hasText(req.getCategory()) && !contains(t.getCategory(), req.getCategory().toLowerCase())) return false;
        if (hasText(req.getProgramme()) && !contains(t.getProgramme(), req.getProgramme().toLowerCase())) return false;
        if (hasText(req.getProject()) && !contains(t.getProject(), req.getProject().toLowerCase())) return false;
        if (hasText(req.getWorkingGroup()) && !contains(t.getWorkingGroup(), req.getWorkingGroup().toLowerCase())) return false;
        if (hasText(req.getAssetClass()) && !contains(t.getAssetClass(), req.getAssetClass().toLowerCase())) return false;
        if (hasText(req.getStakeholder()) && !contains(t.getStakeholder(), req.getStakeholder().toLowerCase())) return false;
        if (hasText(req.getJira()) && !contains(t.getJira(), req.getJira().toLowerCase())) return false;
        if (req.getTags() != null && !req.getTags().isEmpty()) {
            if (t.getTags() == null) return false;
            boolean anyTag = req.getTags().stream()
                    .anyMatch(tag -> t.getTags().stream().anyMatch(tt -> tt.toLowerCase().contains(tag.toLowerCase())));
            if (!anyTag) return false;
        }
        return true;
    }

    private boolean contains(String field, String search) {
        return field != null && field.toLowerCase().contains(search);
    }

    private boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    // ---- Sorting ----

    private Comparator<Task> comparatorFor(String field, String dir) {
        Comparator<Task> cmp = switch (field) {
            case "title"          -> Comparator.comparing(Task::getTitle,          nullsLast());
            case "status"         -> Comparator.comparing(Task::getStatus,         nullsLast());
            case "priority"       -> Comparator.comparing(Task::getPriority,       nullsLast());
            case "assignee"       -> Comparator.comparing(Task::getAssignee,       nullsLast());
            case "category"       -> Comparator.comparing(Task::getCategory,       nullsLast());
            case "programme"      -> Comparator.comparing(Task::getProgramme,      nullsLast());
            case "project"        -> Comparator.comparing(Task::getProject,        nullsLast());
            case "createdAt"      -> Comparator.comparing(Task::getCreatedAt,      nullsLast());
            case "updatedAt"      -> Comparator.comparing(Task::getUpdatedAt,      nullsLast());
            case "estimatedHours" -> Comparator.comparing(Task::getEstimatedHours, nullsLast());
            case "actualHours"    -> Comparator.comparing(Task::getActualHours,    nullsLast());
            default               -> Comparator.comparing(Task::getDueDate,        nullsLast());
        };
        return "desc".equalsIgnoreCase(dir) ? cmp.reversed() : cmp;
    }

    private static <T extends Comparable<T>> Comparator<T> nullsLast() {
        return Comparator.nullsLast(Comparator.naturalOrder());
    }
}
