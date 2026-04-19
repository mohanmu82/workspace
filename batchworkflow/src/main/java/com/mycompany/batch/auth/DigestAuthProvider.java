package com.mycompany.batch.auth;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fetches a base64-encoded token from a URL by POSTing username + password.
 * The response body is used directly as the Bearer token. Token is cached after the first call.
 */
public class DigestAuthProvider implements HttpAuthProvider {

    private final String username;
    private final String password;
    private final String url;
    private final ObjectMapper objectMapper;
    private final AtomicReference<String> cachedToken = new AtomicReference<>();

    public DigestAuthProvider(String username, String password, String url, ObjectMapper objectMapper) {
        this.username = username;
        this.password = password;
        this.url = url;
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

    public void invalidate() {
        cachedToken.set(null);
    }

    private String fetchToken() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of(
                "username", username,
                "password", password
        ));

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("Digest token request failed: HTTP " + response.statusCode()
                    + " — " + response.body());
        }

        String token = response.body().trim();
        if (token.isEmpty()) {
            throw new RuntimeException("Digest token request returned an empty response body");
        }
        return token;
    }
}
