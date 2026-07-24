package com.mycompany.taskmanagement.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.dto.DropdownSourceConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@RequiredArgsConstructor
public class DropdownConfigService {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Value("${app.dropdown-sources.file:./data/dropdown-sources.json}")
    private String configFilePath;

    public Map<String, DropdownSourceConfig> getAll() {
        Path path = Paths.get(configFilePath);
        if (!Files.exists(path)) return new LinkedHashMap<>();
        try {
            return objectMapper.readValue(path.toFile(),
                    new TypeReference<LinkedHashMap<String, DropdownSourceConfig>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public DropdownSourceConfig save(String fieldName, DropdownSourceConfig config) {
        Map<String, DropdownSourceConfig> all = getAll();
        config.setFieldName(fieldName);
        all.put(fieldName, config);
        writeConfig(all);
        return config;
    }

    public void delete(String fieldName) {
        Map<String, DropdownSourceConfig> all = getAll();
        all.remove(fieldName);
        writeConfig(all);
    }

    public List<Map<String, String>> fetchOptions(String fieldName) {
        DropdownSourceConfig config = getAll().get(fieldName);
        if (config == null || config.getUrl() == null || config.getUrl().isBlank()) {
            return List.of();
        }
        return fetchFromUrl(config);
    }

    public List<Map<String, String>> fetchOptionsFromConfig(DropdownSourceConfig config) {
        if (config == null || config.getUrl() == null || config.getUrl().isBlank()) {
            return List.of();
        }
        return fetchFromUrl(config);
    }

    private List<Map<String, String>> fetchFromUrl(DropdownSourceConfig config) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(config.getUrl()))
                    .GET()
                    .header("Accept", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode root = objectMapper.readTree(response.body());

            JsonNode arrayNode = (config.getArrayName() != null && !config.getArrayName().isBlank())
                    ? root.path(config.getArrayName())
                    : root;

            List<Map<String, String>> options = new ArrayList<>();
            for (JsonNode item : arrayNode) {
                String name = item.path(config.getNameAttribute() != null ? config.getNameAttribute() : "").asText();
                String value = (config.getValueAttribute() != null && !config.getValueAttribute().isBlank())
                        ? item.path(config.getValueAttribute()).asText()
                        : name;
                Map<String, String> opt = new LinkedHashMap<>();
                opt.put("name", name);
                opt.put("value", value);
                options.add(opt);
            }
            return options;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch options from " + config.getUrl() + ": " + e.getMessage(), e);
        }
    }

    private void writeConfig(Map<String, DropdownSourceConfig> config) {
        try {
            Path path = Paths.get(configFilePath);
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
