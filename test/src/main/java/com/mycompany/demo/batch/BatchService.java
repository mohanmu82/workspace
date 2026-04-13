package com.mycompany.demo.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class BatchService {

    @Value("${batch.http.url}")
    private String httpUrl;

    /**
     * HTTP method to use: GET or POST (case-insensitive).
     * For GET, put {id} in the URL (e.g. http://host/api?id={id}).
     * For POST, put {id} in the body template.
     */
    @Value("${batch.http.method:POST}")
    private String httpMethod;

    @Value("${batch.http.content-type:text/plain}")
    private String contentType;

    /** POST only — request body template; {id} is replaced with the identifier */
    @Value("${batch.http.body-template:{id}}")
    private String bodyTemplate;

    @PostConstruct
    void validateConfig() {
        String method = httpMethod.toUpperCase();
        if (!method.equals("GET") && !method.equals("POST")) {
            throw new IllegalStateException(
                    "batch.http.method must be GET or POST, got: " + httpMethod);
        }
        if (method.equals("GET") && !httpUrl.contains("{id}")) {
            throw new IllegalStateException(
                    "batch.http.method=GET requires {id} in batch.http.url, e.g. http://host/api?id={id}");
        }
    }

    @Value("${batch.http.thread-count:5}")
    private int httpThreadCount;

    @Value("${batch.xpath.thread-count:4}")
    private int xpathThreadCount;

    /** classpath:xpaths.json or an absolute file path */
    @Value("${batch.xpath.config:classpath:xpaths.json}")
    private String xpathsConfig;

    private final ObjectMapper objectMapper;  // used for loadXPaths
    private final XPathExtractor xpathExtractor;

    public BatchService(ObjectMapper objectMapper, XPathExtractor xpathExtractor) {
        this.objectMapper = objectMapper;
        this.xpathExtractor = xpathExtractor;
    }

    public record BatchResult(int processed, int succeeded, int failed, List<Map<String, Object>> results) {}
    public record PsvResult(int processed, int succeeded, int failed, String outputFile) {}

    public PsvResult runToPsv(String filePath) throws Exception {
        BatchResult batch = run(filePath);

        // Collect ordered column names from the first successful row (skip error rows)
        SequencedSet<String> columns = new LinkedHashSet<>();
        columns.add("identifier");
        for (Map<String, Object> row : batch.results()) {
            if (!row.containsKey("error")) {
                row.keySet().stream()
                        .filter(k -> !k.equals("identifier"))
                        .forEach(columns::add);
                break;
            }
        }

        // Derive output path: same as input file with .psv extension
        Path inputPath = Path.of(filePath);
        String baseName = inputPath.getFileName().toString().replaceFirst("\\.[^.]+$", "");
        Path outputPath = inputPath.resolveSibling(baseName + ".psv");

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            // Header
            writer.write(String.join("|", columns));
            writer.newLine();

            // One line per identifier
            for (Map<String, Object> row : batch.results()) {
                List<String> line = new ArrayList<>();
                for (String col : columns) {
                    Object val = row.get(col);
                    if (val == null && row.containsKey("error")) {
                        val = col.equals("identifier") ? row.get("identifier") : "ERROR: " + row.get("message");
                    }
                    line.add(val != null ? val.toString() : "");
                }
                writer.write(String.join("|", line));
                writer.newLine();
            }
        }

        return new PsvResult(batch.processed(), batch.succeeded(), batch.failed(),
                outputPath.toAbsolutePath().toString());
    }

    public BatchResult run(String filePath) throws Exception {
        List<String> identifiers = Files.readAllLines(Path.of(filePath)).stream()
                .skip(1)  // first line is a header
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        Map<String, String> xpaths = loadXPaths();

        ExecutorService httpPool = Executors.newFixedThreadPool(httpThreadCount);
        ExecutorService xpathPool = Executors.newFixedThreadPool(xpathThreadCount);
        HttpClient httpClient = HttpClient.newBuilder().build();

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();

        List<CompletableFuture<Void>> futures = identifiers.stream()
                .map(id -> processOne(id, httpClient, httpPool, xpaths, xpathPool, succeeded, failed, results))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        httpPool.shutdown();
        xpathPool.shutdown();

        return new BatchResult(identifiers.size(), succeeded.get(), failed.get(), results);
    }

    private CompletableFuture<Void> processOne(
            String id, HttpClient httpClient, ExecutorService httpPool,
            Map<String, String> xpaths, ExecutorService xpathPool,
            AtomicInteger succeeded, AtomicInteger failed, List<Map<String, Object>> results) {

        HttpRequest request = buildRequest(id);

        // HTTP call runs on httpPool (controls max simultaneous connections)
        return CompletableFuture
                .supplyAsync(() -> {
                    try {
                        HttpResponse<String> response = httpClient.send(
                                request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            throw new RuntimeException("HTTP " + response.statusCode()
                                    + " for identifier '" + id + "'");
                        }
                        return response.body();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while calling HTTP endpoint for '" + id + "'", e);
                    } catch (Exception e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }, httpPool)
                // XPath extraction runs on xpathPool
                .thenCompose(xml -> xpathExtractor.extractAsync(xml, xpaths, xpathPool))
                .thenAccept(attributes -> {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("identifier", id);
                    entry.putAll(attributes);
                    results.add(entry);
                    succeeded.incrementAndGet();
                })
                .exceptionally(ex -> {
                    failed.incrementAndGet();
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("identifier", id);
                    entry.put("error", true);
                    entry.put("message", message);
                    results.add(entry);
                    return null;
                });
    }

    private HttpRequest buildRequest(String id) {
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        if (httpMethod.equalsIgnoreCase("GET")) {
            builder.uri(URI.create(httpUrl.replace("{id}", id)))
                   .GET();
        } else {
            builder.uri(URI.create(httpUrl.replace("{id}", id)))
                   .header("Content-Type", contentType)
                   .POST(HttpRequest.BodyPublishers.ofString(bodyTemplate.replace("{id}", id)));
        }
        return builder.build();
    }

    private Map<String, String> loadXPaths() throws Exception {
        InputStream is;
        if (xpathsConfig.startsWith("classpath:")) {
            String resource = xpathsConfig.substring("classpath:".length());
            is = getClass().getClassLoader().getResourceAsStream(resource);
            if (is == null) {
                throw new FileNotFoundException("XPath config not found on classpath: " + resource);
            }
        } else {
            is = new FileInputStream(xpathsConfig);
        }
        try (is) {
            return objectMapper.readValue(is, new TypeReference<>() {});
        }
    }

}
