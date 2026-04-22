package com.mycompany.batch.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ServerPropertiesLoader {

    private final ObjectMapper objectMapper;
    private final Map<String, String> properties = new LinkedHashMap<>();

    public ServerPropertiesLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void load() throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream("server.json");
        if (is == null)
            throw new IllegalStateException("server.json not found on classpath — it is required at startup");
        try (is) {
            Map<String, Object> raw = objectMapper.readValue(is, new TypeReference<>() {});
            raw.forEach((k, v) -> properties.put(k, v != null ? v.toString() : ""));
        }
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
}
