package com.mycompany.batch.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.BatchProperties;
import com.mycompany.batch.config.ServerPropertiesLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@RestController
@RequestMapping("/batch/swagger")
public class SwaggerController {

    private final ObjectMapper          objectMapper;
    private final ServerPropertiesLoader serverPropertiesLoader;
    private final BatchProperties       batchProperties;

    public SwaggerController(ObjectMapper objectMapper,
                             ServerPropertiesLoader serverPropertiesLoader,
                             BatchProperties batchProperties) {
        this.objectMapper           = objectMapper;
        this.serverPropertiesLoader = serverPropertiesLoader;
        this.batchProperties        = batchProperties;
    }

    // -------------------------------------------------------------------------
    // GET /batch/swagger/parse?url=...
    // Fetches a Swagger 2.0 / OpenAPI 3.x JSON and returns the operation list.
    // -------------------------------------------------------------------------

    @GetMapping("/parse")
    public ResponseEntity<?> parse(@RequestParam String url) {
        try {
            String json = fetchUrl(url);
            JsonNode root = objectMapper.readTree(json);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("baseUrl",    extractBaseUrl(root));
            result.put("operations", parseOperations(root));
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return badRequest(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    // -------------------------------------------------------------------------
    // GET /batch/swagger/requestTemplates
    // Lists request template files in ${DATADIR}/requestTemplate/.
    // -------------------------------------------------------------------------

    @GetMapping("/requestTemplates")
    public ResponseEntity<?> listTemplates() {
        List<String> names = new ArrayList<>();
        Path dir = requestTemplateDir();
        if (Files.isDirectory(dir)) {
            try (Stream<Path> stream = Files.list(dir)) {
                stream.filter(p -> p.toString().endsWith(".json"))
                      .map(p -> p.getFileName().toString())
                      .sorted()
                      .forEach(names::add);
            } catch (Exception ignored) {}
        }
        return ResponseEntity.ok(Map.of("data", names));
    }

    // -------------------------------------------------------------------------
    // POST /batch/swagger/create
    // Body: { prefix, baseUrl, threadCount, timeoutMs, operations:[{operationId, method, path, summary, hasBody, schema}] }
    // Creates one operation JSON file per entry + a body-template file when hasBody=true.
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @PostMapping("/create")
    public ResponseEntity<?> create(@RequestBody Map<String, Object> body) {
        String prefix      = body.getOrDefault("prefix",      "").toString().trim();
        String baseUrl     = body.getOrDefault("baseUrl",     "").toString().trim();
        int    threadCount = toInt(body.getOrDefault("threadCount", 5),  5);
        int    timeoutMs   = toInt(body.getOrDefault("timeoutMs",  3000), 3000);

        List<Map<String, Object>> ops;
        try {
            ops = (List<Map<String, Object>>) body.get("operations");
        } catch (ClassCastException e) {
            return badRequest("'operations' must be an array");
        }
        if (ops == null || ops.isEmpty()) return badRequest("No operations provided");

        Path opsDir = operationsDir();
        Path tplDir = requestTemplateDir();
        try {
            Files.createDirectories(opsDir);
            Files.createDirectories(tplDir);
        } catch (Exception e) {
            return badRequest("Cannot create output directories: " + e.getMessage());
        }

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map<String, Object> op : ops) {
            try {
                results.add(createOneOperation(op, prefix, baseUrl, threadCount, timeoutMs, opsDir, tplDir));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("name",   prefix + op.getOrDefault("operationId", "?"));
                err.put("status", "error: " + e.getMessage());
                results.add(err);
            }
        }

        // Reload operations so they are immediately available
        try { batchProperties.loadFromJson(); } catch (Exception ignored) {}

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("created",    results.stream().filter(r -> "created".equals(r.get("status"))).count());
        response.put("operations", results);
        return ResponseEntity.ok(response);
    }

    // -------------------------------------------------------------------------
    // Helpers — swagger parsing
    // -------------------------------------------------------------------------

    private List<Map<String, Object>> parseOperations(JsonNode root) {
        List<Map<String, Object>> ops = new ArrayList<>();
        JsonNode paths = root.get("paths");
        if (paths == null) return ops;

        for (String method : List.of("get","post","put","delete","patch","options","head")) {
            paths.properties().forEach(pathEntry -> {
                JsonNode item = pathEntry.getValue();
                JsonNode opNode = item.get(method);
                if (opNode == null) return;

                String operationId = opNode.path("operationId").asText("").trim();
                if (operationId.isBlank()) {
                    // generate from method + path
                    operationId = method + pathEntry.getKey().replaceAll("[^a-zA-Z0-9]", "_")
                                                             .replaceAll("_+", "_")
                                                             .replaceAll("^_|_$", "");
                }
                String summary = opNode.path("summary").asText("").trim();
                if (summary.isBlank()) summary = opNode.path("description").asText("").trim();

                // Detect request body
                boolean hasBody = opNode.has("requestBody");  // OpenAPI 3.x
                Map<String, Object> schema = null;
                if (opNode.has("requestBody")) {
                    JsonNode rb = opNode.get("requestBody");
                    JsonNode content = rb.path("content");
                    JsonNode jsonContent = content.get("application/json");
                    if (jsonContent == null) {
                        var it = content.properties().iterator();
                        jsonContent = it.hasNext() ? it.next().getValue() : null;
                    }
                    if (jsonContent != null) {
                        schema = extractSchemaTemplate(jsonContent.path("schema"), root);
                    }
                }
                // Swagger 2.0 — body parameter
                if (!hasBody && opNode.has("parameters")) {
                    for (JsonNode p : opNode.get("parameters")) {
                        if ("body".equals(p.path("in").asText(""))) {
                            hasBody = true;
                            schema  = extractSchemaTemplate(p.path("schema"), root);
                            break;
                        }
                    }
                }
                if (schema == null && hasBody) schema = new LinkedHashMap<>();

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("operationId", operationId);
                entry.put("method",      method.toUpperCase());
                entry.put("path",        pathEntry.getKey());
                entry.put("summary",     summary);
                entry.put("hasBody",     hasBody);
                if (schema != null) entry.put("schema", schema);
                ops.add(entry);
            });
        }
        return ops;
    }

    private String extractBaseUrl(JsonNode root) {
        // OpenAPI 3.x
        JsonNode servers = root.get("servers");
        if (servers != null && servers.isArray() && !servers.isEmpty()) {
            String url = servers.get(0).path("url").asText("").trim();
            if (!url.isBlank()) return url;
        }
        // Swagger 2.0
        String host     = root.path("host").asText("").trim();
        String basePath = root.path("basePath").asText("/").trim();
        if (!host.isBlank()) {
            String scheme = "https";
            JsonNode schemes = root.get("schemes");
            if (schemes != null && schemes.isArray() && !schemes.isEmpty())
                scheme = schemes.get(0).asText("https");
            return scheme + "://" + host + (basePath.startsWith("/") ? basePath : "/" + basePath);
        }
        return "";
    }

    private Map<String, Object> extractSchemaTemplate(JsonNode schema, JsonNode root) {
        Map<String, Object> tpl = new LinkedHashMap<>();
        if (schema == null || schema.isMissingNode() || schema.isNull()) return tpl;

        // Resolve $ref
        if (schema.has("$ref")) {
            String ref = schema.get("$ref").asText("");
            if (ref.startsWith("#/")) {
                String[] parts = ref.substring(2).split("/");
                JsonNode resolved = root;
                for (String part : parts) {
                    resolved = resolved.path(part.replace("~1", "/").replace("~0", "~"));
                    if (resolved.isMissingNode()) return tpl;
                }
                return extractSchemaTemplate(resolved, root);
            }
            return tpl;
        }

        // allOf / anyOf / oneOf — merge first match
        for (String combiner : List.of("allOf","anyOf","oneOf")) {
            if (schema.has(combiner)) {
                JsonNode arr = schema.get(combiner);
                if (arr.isArray()) for (JsonNode sub : arr) tpl.putAll(extractSchemaTemplate(sub, root));
                return tpl;
            }
        }

        JsonNode props = schema.get("properties");
        if (props == null) return tpl;
        props.properties().forEach(e -> {
            String name    = e.getKey();
            JsonNode pNode = e.getValue();
            if (pNode.has("$ref") || "object".equals(pNode.path("type").asText(""))) {
                tpl.put(name, extractSchemaTemplate(pNode, root));
            } else {
                String type = pNode.path("type").asText("string");
                tpl.put(name, switch (type) {
                    case "integer", "number" -> 0;
                    case "boolean"           -> false;
                    case "array"             -> List.of();
                    default                  -> "${" + name + "}";
                });
            }
        });
        return tpl;
    }

    // -------------------------------------------------------------------------
    // Helpers — file creation
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> createOneOperation(Map<String, Object> op,
                                                   String prefix, String baseUrl,
                                                   int threadCount, int timeoutMs,
                                                   Path opsDir, Path tplDir) throws Exception {
        String operationId = op.getOrDefault("operationId", "").toString().trim();
        String method      = op.getOrDefault("method",      "GET").toString().toUpperCase();
        String path        = op.getOrDefault("path",        "").toString();
        String summary     = op.getOrDefault("summary",     "").toString();
        boolean hasBody    = Boolean.TRUE.equals(op.get("hasBody"));
        String opBaseUrl   = baseUrl.isBlank() ? op.getOrDefault("baseUrl","").toString() : baseUrl;
        Object schemaObj   = op.get("schema");

        String name = (prefix + operationId).replaceAll("[^a-zA-Z0-9_\\-]", "_");
        String url  = opBaseUrl + path;

        // --- body template file ---
        String bodyTemplatePath = null;
        if (hasBody) {
            Path tplFile = tplDir.resolve(name + ".json");
            if (!Files.exists(tplFile)) {
                Map<String, Object> tplContent = (schemaObj instanceof Map<?,?> m)
                        ? (Map<String, Object>) m : new LinkedHashMap<>();
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(tplFile.toFile(), tplContent);
            }
            bodyTemplatePath = tplFile.toAbsolutePath().toString().replace('\\', '/');
        }

        // --- operation JSON ---
        Map<String, Object> http = new LinkedHashMap<>();
        http.put("url",         url);
        http.put("method",      method);
        http.put("threadCount", threadCount);
        http.put("timeoutMs",   timeoutMs);
        if (bodyTemplatePath != null) http.put("bodyTemplate", bodyTemplatePath);

        Map<String, Object> activity = new LinkedHashMap<>();
        activity.put("name", "http_" + name);
        activity.put("type", "HTTP");
        activity.put("http", http);

        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("operationName", name);
        if (!summary.isBlank()) attrs.put("operationDescription", summary);

        Map<String, Object> opJson = new LinkedHashMap<>();
        opJson.put("name",       name);
        opJson.put("properties", Map.of("attributes", attrs));
        opJson.put("activity",   List.of(activity));

        Path opFile = opsDir.resolve(name + ".json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(opFile.toFile(), opJson);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name",   name);
        result.put("file",   opFile.toAbsolutePath().toString().replace('\\', '/'));
        if (bodyTemplatePath != null) result.put("template", bodyTemplatePath);
        result.put("status", "created");
        return result;
    }

    // -------------------------------------------------------------------------
    // Helpers — misc
    // -------------------------------------------------------------------------

    private String fetchUrl(String url) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();
        HttpResponse<String> response = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            throw new IllegalStateException("HTTP " + response.statusCode() + " fetching swagger URL");
        return response.body();
    }

    private Path operationsDir() {
        return dataDirPath().resolve("operations");
    }

    private Path requestTemplateDir() {
        return dataDirPath().resolve("requestTemplate");
    }

    private Path dataDirPath() {
        String dataDir = serverPropertiesLoader.getProperties().getOrDefault("DATADIR", ".");
        return Path.of(dataDir);
    }

    private int toInt(Object val, int def) {
        try { return Integer.parseInt(val.toString()); } catch (Exception e) { return def; }
    }

    private ResponseEntity<Map<String, Object>> badRequest(String msg) {
        return ResponseEntity.badRequest().body(Map.of("error", msg));
    }
}
