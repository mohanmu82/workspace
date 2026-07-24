package com.mycompany.taskmanagement.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.dto.FieldConfig;
import com.mycompany.taskmanagement.model.Programme;
import com.mycompany.taskmanagement.model.Project;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Service
@RequiredArgsConstructor
public class FieldConfigService {

    private final ObjectMapper objectMapper;
    private final ProjectService projectService;
    private final ProgrammeService programmeService;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Value("${app.field-configs.file:./data/field-configs.json}")
    private String configFilePath;

    public Map<String, FieldConfig> getAll() {
        Path path = Paths.get(configFilePath);
        if (!Files.exists(path)) return new LinkedHashMap<>();
        try {
            return objectMapper.readValue(path.toFile(),
                    new TypeReference<LinkedHashMap<String, FieldConfig>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized FieldConfig save(String fieldId, FieldConfig config) {
        Map<String, FieldConfig> all = getAll();
        config.setFieldId(fieldId);
        all.put(fieldId, config);
        writeConfig(all);
        return config;
    }

    public synchronized void delete(String fieldId) {
        Map<String, FieldConfig> all = getAll();
        all.remove(fieldId);
        writeConfig(all);
    }

    public List<Map<String, String>> getOptions(String fieldId) {
        FieldConfig config = getAll().get(fieldId);
        if (config == null || !"select".equals(config.getFieldType()) || config.getSource() == null) {
            return List.of();
        }
        return resolveOptions(config.getSource());
    }

    public List<Map<String, String>> testHttpOptions(FieldConfig.DropdownSource source) {
        if (source == null) {
            return List.of();
        }
        return resolveOptions(source);
    }

    private List<Map<String, String>> resolveOptions(FieldConfig.DropdownSource source) {
        if ("static".equals(source.getType())) {
            return staticToOptions(source.getStaticOptions());
        } else if ("http".equals(source.getType())) {
            return fetchHttpOptions(source);
        } else if ("file".equals(source.getType())) {
            return fetchFileOptions(source);
        } else if ("entity".equals(source.getType())) {
            return fetchEntityOptions(source);
        }
        return List.of();
    }

    private List<Map<String, String>> fetchEntityOptions(FieldConfig.DropdownSource source) {
        String entityType = source.getEntityType();
        if (entityType == null || entityType.isBlank()) {
            return List.of();
        }
        List<String> names;
        if ("programme".equalsIgnoreCase(entityType)) {
            names = programmeService.list().stream().map(Programme::getName).toList();
        } else if ("project".equalsIgnoreCase(entityType)) {
            names = projectService.list().stream().map(Project::getName).toList();
        } else {
            throw new IllegalArgumentException("Unknown entity type: " + entityType);
        }
        List<Map<String, String>> options = new ArrayList<>();
        for (String name : names) {
            Map<String, String> opt = new LinkedHashMap<>();
            opt.put("name", name);
            opt.put("value", name);
            options.add(opt);
        }
        return options;
    }

    private List<Map<String, String>> fetchFileOptions(FieldConfig.DropdownSource source) {
        String fileName = source.getFileName();
        if (fileName == null || fileName.isBlank()) {
            return List.of();
        }
        ClassPathResource resource = new ClassPathResource(fileName);
        if (!resource.exists()) {
            throw new RuntimeException("Classpath file not found: " + fileName);
        }
        List<Map<String, String>> options = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\|", 2);
                if (parts.length < 2) continue;
                Map<String, String> opt = new LinkedHashMap<>();
                opt.put("name", parts[0].trim());
                opt.put("value", parts[1].trim());
                options.add(opt);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return options;
    }

    private List<Map<String, String>> staticToOptions(List<String> options) {
        if (options == null) return List.of();
        List<Map<String, String>> result = new ArrayList<>();
        for (String opt : options) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("name", opt);
            m.put("value", opt);
            result.add(m);
        }
        return result;
    }

    private List<Map<String, String>> fetchHttpOptions(FieldConfig.DropdownSource source) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(source.getUrl()))
                    .GET()
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode root = objectMapper.readTree(response.body());

            JsonNode arrayNode = (source.getArrayName() != null && !source.getArrayName().isBlank())
                    ? root.path(source.getArrayName())
                    : root;

            List<Map<String, String>> options = new ArrayList<>();
            for (JsonNode item : arrayNode) {
                String nameAttr = source.getNameAttribute() != null ? source.getNameAttribute() : "";
                String name = item.path(nameAttr).asText();
                String value = (source.getValueAttribute() != null && !source.getValueAttribute().isBlank())
                        ? item.path(source.getValueAttribute()).asText()
                        : name;
                Map<String, String> opt = new LinkedHashMap<>();
                opt.put("name", name);
                opt.put("value", value);
                options.add(opt);
            }
            return options;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch options from " + source.getUrl() + ": " + e.getMessage(), e);
        }
    }

    private void writeConfig(Map<String, FieldConfig> config) {
        try {
            Path path = Paths.get(configFilePath);
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
