package com.mycompany.taskmanagement.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.UserAccess;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@Profile("json")
public class JsonFileUserAccessDataStore implements UserAccessDataStore {

    private final ObjectMapper objectMapper;
    private final Path file;

    public JsonFileUserAccessDataStore(ObjectMapper objectMapper,
                                        @Value("${app.user-access.file:./data/user-access.json}") String filePath) {
        this.objectMapper = objectMapper;
        this.file = Paths.get(filePath);
    }

    // Stored as a single JSON file: { "<userId>": UserAccess, ... }

    @Override
    public synchronized List<UserAccess> findAll() {
        return List.copyOf(loadAll().values());
    }

    @Override
    public synchronized Optional<UserAccess> findByUserId(String userId) {
        return Optional.ofNullable(loadAll().get(userId));
    }

    @Override
    public synchronized UserAccess save(UserAccess access) {
        Map<String, UserAccess> all = loadAll();
        access.setUpdatedAt(LocalDateTime.now());
        all.put(access.getUserId(), access);
        writeAll(all);
        return access;
    }

    @Override
    public synchronized void deleteByUserId(String userId) {
        Map<String, UserAccess> all = loadAll();
        all.remove(userId);
        writeAll(all);
    }

    private Map<String, UserAccess> loadAll() {
        if (!Files.exists(file)) return new LinkedHashMap<>();
        try {
            return objectMapper.readValue(file.toFile(), new TypeReference<LinkedHashMap<String, UserAccess>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeAll(Map<String, UserAccess> all) {
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), all);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
