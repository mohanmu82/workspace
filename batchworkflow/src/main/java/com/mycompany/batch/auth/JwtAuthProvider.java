package com.mycompany.batch.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class JwtAuthProvider implements HttpAuthProvider {

    private final String applicationName;
    private final String username;
    private final String password;
    private final String jwtUrl;
    private final ObjectMapper objectMapper;
    private final AtomicReference<String> cachedToken = new AtomicReference<>();

    public JwtAuthProvider(String applicationName, String username, String password,
                           String jwtUrl, ObjectMapper objectMapper) {
        this.applicationName = applicationName;
        this.username = username;
        this.password = password;
        this.jwtUrl = jwtUrl;
        this.objectMapper = objectMapper;
    }

    @Override
    public String getAuthorizationHeader() throws Exception {
        String token = cachedToken.get();
        if (token == null) {
            token = fetchToken();
            cachedToken.set(token);
        }
        return "Bearer " + token;
    }

    /** Invalidates the cached token, forcing a fresh fetch on the next call. */
    public void invalidate() {
        cachedToken.set(null);
    }

    private String fetchToken() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of(
                "applicationName", applicationName,
                "username", username,
                "password", password
        ));

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(jwtUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("JWT token request failed: HTTP " + response.statusCode()
                    + " — " + response.body());
        }

        JsonNode json = objectMapper.readTree(response.body());
        for (String field : new String[]{"token", "access_token", "accessToken"}) {
            if (json.has(field)) {
                return json.get(field).asText();
            }
        }
        throw new RuntimeException("JWT response contained no recognised token field "
                + "(expected: token, access_token, accessToken). Body: " + response.body());
    }
}
