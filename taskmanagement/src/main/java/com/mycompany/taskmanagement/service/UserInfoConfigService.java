package com.mycompany.taskmanagement.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.UserInfoAttributeConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Admin-configurable list of extra user-info attributes (firstName, nickname, orgUnit, ...) and
 * the JSONPath used to pull each one out of the auth.user-detail-url response. Always stored as a
 * plain JSON file - this is app configuration, not per-tenant business data, so it doesn't need
 * the JPA/JSON dual-store split the rest of the app uses.
 */
@Service
public class UserInfoConfigService {

    private static final List<UserInfoAttributeConfig> DEFAULTS = List.of(
            attr("firstName", "First Name", "$.firstName"),
            attr("lastName", "Last Name", "$.lastName"),
            attr("nickname", "Nickname", "$.nickname"),
            attr("orgUnit", "Org Unit", "$.orgUnit"),
            attr("team", "Team", "$.team"));

    private final ObjectMapper objectMapper;
    private final Path file;

    public UserInfoConfigService(ObjectMapper objectMapper,
                                  @Value("${app.user-info-config.file:./data/user-info-config.json}") String filePath) {
        this.objectMapper = objectMapper;
        this.file = Paths.get(filePath);
    }

    public synchronized List<UserInfoAttributeConfig> list() {
        if (!Files.exists(file)) {
            save(DEFAULTS);
            return DEFAULTS;
        }
        try {
            return objectMapper.readValue(file.toFile(), new TypeReference<List<UserInfoAttributeConfig>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized List<UserInfoAttributeConfig> save(List<UserInfoAttributeConfig> configs) {
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), configs);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return configs;
    }

    private static UserInfoAttributeConfig attr(String key, String label, String jsonPath) {
        UserInfoAttributeConfig config = new UserInfoAttributeConfig();
        config.setKey(key);
        config.setLabel(label);
        config.setJsonPath(jsonPath);
        config.setDefaultValue("");
        return config;
    }
}
