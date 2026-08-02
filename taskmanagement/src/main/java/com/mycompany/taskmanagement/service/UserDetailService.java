package com.mycompany.taskmanagement.service;

import com.jayway.jsonpath.JsonPath;
import com.mycompany.taskmanagement.config.AuthProperties;
import com.mycompany.taskmanagement.model.UserInfoAttributeConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Looks up a user's attributes (first/last name, nickname, orgUnit, ...) from the
 * auth.user-detail-url endpoint given their BRID. Called once by an admin when a new user is
 * added in the Admin > Access tab - the extracted values are then stored on the UserAccess
 * record and can be edited by hand afterward, so this is never called on the login path.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class UserDetailService {

    private final AuthProperties authProperties;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public Map<String, String> fetchAttributes(String brid, List<UserInfoAttributeConfig> configs) {
        String urlTemplate = authProperties.getUserDetailUrl();
        Map<String, String> result = new LinkedHashMap<>();
        if (urlTemplate == null || urlTemplate.isBlank() || brid == null) {
            configs.forEach(c -> result.put(c.getKey(), c.getDefaultValue()));
            return result;
        }

        String url = urlTemplate.replace("{brid}", URLEncoder.encode(brid, StandardCharsets.UTF_8));
        String body = null;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                body = response.body();
            } else {
                log.warn("User detail lookup for {} failed: HTTP {}", brid, response.statusCode());
            }
        } catch (IOException e) {
            log.warn("User detail lookup for {} failed", brid, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (UserInfoAttributeConfig config : configs) {
            result.put(config.getKey(), extract(body, config));
        }
        return result;
    }

    private String extract(String body, UserInfoAttributeConfig config) {
        if (body != null && config.getJsonPath() != null && !config.getJsonPath().isBlank()) {
            try {
                Object value = JsonPath.read(body, config.getJsonPath());
                if (value != null) {
                    String text = String.valueOf(value);
                    if (!text.isBlank()) return text;
                }
            } catch (RuntimeException e) {
                log.debug("JSONPath {} did not resolve for attribute {}", config.getJsonPath(), config.getKey());
            }
        }
        return config.getDefaultValue();
    }
}
