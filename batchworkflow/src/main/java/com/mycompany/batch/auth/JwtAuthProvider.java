package com.mycompany.batch.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class JwtAuthProvider implements HttpAuthProvider {

    /** How much of an unexpected response body a failure message carries — enough to recognise it. */
    private static final int BODY_SNIPPET = 400;

    private final String applicationName;
    private final String username;
    private final String password;
    private final String jwtUrl;
    private final String jwtMethod;
    private final ObjectMapper objectMapper;
    private final AtomicReference<String> cachedToken = new AtomicReference<>();

    /** Calls the token endpoint with POST — the long-standing behaviour, kept for existing callers. */
    public JwtAuthProvider(String applicationName, String username, String password,
                           String jwtUrl, ObjectMapper objectMapper) {
        this(applicationName, username, password, jwtUrl, "POST", objectMapper);
    }

    /**
     * @param jwtMethod GET or POST. POST sends the credentials as a JSON body; GET sends no body at
     *                  all and, when a username is configured, presents it as HTTP Basic — plenty of
     *                  token endpoints are a plain GET, and silently dropping the credentials there
     *                  would be harder to diagnose than sending them the conventional way.
     */
    public JwtAuthProvider(String applicationName, String username, String password,
                           String jwtUrl, String jwtMethod, ObjectMapper objectMapper) {
        this.applicationName = applicationName;
        this.username = username;
        this.password = password;
        this.jwtUrl = jwtUrl;
        this.jwtMethod = jwtMethod == null || jwtMethod.isBlank() ? "POST" : jwtMethod.trim().toUpperCase();
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

    /**
     * Calls the token endpoint the way {@code jwtMethod} says, and returns the token from its JSON.
     *
     * <p>On POST the request carries whichever credentials are actually configured; a field that was
     * never set is left out rather than sent as null, because plenty of token endpoints authenticate
     * by client certificate or network position alone and need no username at all. (This used
     * {@code Map.of}, which rejects null values, so a credential-free environment failed with a bare
     * NullPointerException before the request was ever made.)
     */
    private String fetchToken() throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(jwtUrl));

        if ("GET".equals(jwtMethod)) {
            // A GET token endpoint has nowhere to put a JSON credential body, so any configured
            // username travels the conventional way instead of being quietly dropped.
            if (username != null && !username.isBlank()) {
                builder.header("Authorization",
                        new BasicAuthProvider(username, password == null ? "" : password).getAuthorizationHeader());
            }
            builder.GET();
        } else {
            Map<String, String> payload = new LinkedHashMap<>();
            if (applicationName != null) payload.put("applicationName", applicationName);
            if (username != null)        payload.put("username", username);
            if (password != null)        payload.put("password", password);
            builder.header("Content-Type", "application/json")
                   .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)));
        }

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        String contentType = response.headers().firstValue("content-type").orElse("(none)");

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new RuntimeException("JWT token request failed: " + describe(response, contentType));
        }

        // Jackson's own "Unexpected character ('<')" says nothing about what actually arrived. A
        // 2xx that is not JSON almost always means the request never reached the token service —
        // an SSO or proxy login page, or an endpoint that wanted a different method or path — so
        // the failure reports the status, the content type and the start of the body instead.
        JsonNode json;
        try {
            json = objectMapper.readTree(response.body());
        } catch (Exception e) {
            throw new RuntimeException("JWT endpoint did not return JSON: " + describe(response, contentType)
                    + " — check that the JWT URL points at the token service itself and not at a login "
                    + "or error page.", e);
        }

        for (String field : new String[]{"token", "access_token", "accessToken"}) {
            if (json.has(field)) {
                return json.get(field).asText();
            }
        }
        throw new RuntimeException("JWT response contained no recognised token field "
                + "(expected: token, access_token, accessToken). " + describe(response, contentType));
    }

    /** What was sent and what came back — enough to tell a wrong method from a wrong URL. */
    private String describe(HttpResponse<String> response, String contentType) {
        String body = response.body() == null ? "" : response.body().strip();
        String snippet = body.length() > BODY_SNIPPET ? body.substring(0, BODY_SNIPPET) + "… (" + body.length() + " chars)" : body;
        return jwtMethod + " " + jwtUrl + " returned HTTP " + response.statusCode() + ", Content-Type " + contentType
                + ", body: " + (snippet.isEmpty() ? "(empty)" : snippet);
    }
}
