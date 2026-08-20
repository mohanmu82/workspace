package com.mycompany.agent;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Runs a single self-contained HTTP request (method, url, headers, body, timeout) per call and
 * reports the status code, response headers and body back via {@code onDone} in one shot.
 * Unlike {@link CommandExecutor} there's no line-by-line streaming — the whole response is only
 * meaningful once the call completes.
 */
public class HttpRequestExecutor {

    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public void execute(HttpExecRequest request, Consumer<Map<String, Object>> onDone) {
        // Virtual threads so one agent can hold many concurrent in-flight HTTP calls (this is
        // I/O-bound work, blocked mostly on the remote server) without platform-thread overhead.
        Thread.ofVirtual().name("http-exec").start(() -> runRequest(request, onDone));
    }

    private void runRequest(HttpExecRequest request, Consumer<Map<String, Object>> onDone) {
        long start = System.currentTimeMillis();
        try {
            int timeoutMs = request.timeoutMs() > 0 ? request.timeoutMs() : DEFAULT_TIMEOUT_MS;
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(request.url()))
                    .timeout(Duration.ofMillis(timeoutMs));

            if (request.headers() != null) {
                request.headers().forEach((k, v) -> {
                    if (k != null && !k.isBlank() && v != null) builder.header(k, v);
                });
            }

            String body = request.body() != null ? request.body() : "";
            HttpRequest.BodyPublisher publisher = body.isEmpty()
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body);
            String method = request.method() == null || request.method().isBlank()
                    ? "GET" : request.method().toUpperCase();
            builder.method(method, publisher);

            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());

            Map<String, String> responseHeaders = new LinkedHashMap<>();
            response.headers().map().forEach((k, v) -> responseHeaders.put(k, String.join(", ", v)));

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("statusCode", response.statusCode());
            result.put("headers", responseHeaders);
            result.put("body", response.body());
            result.put("durationMs", System.currentTimeMillis() - start);
            onDone.accept(result);
        } catch (Exception e) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            result.put("durationMs", System.currentTimeMillis() - start);
            onDone.accept(result);
        }
    }

    /** Self-contained description of the HTTP call — everything needed travels with the request, nothing is read from agent-local config. */
    public record HttpExecRequest(String url, String method, Map<String, String> headers, String body, int timeoutMs) {}
}
