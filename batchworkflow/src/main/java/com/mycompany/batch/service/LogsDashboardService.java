package com.mycompany.batch.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Walks a top-level unix log directory, finds files modified within a recent window, and greps
 * each one for a configurable issue pattern (default CRITICAL/ERROR/exception markers) — backs
 * the /logsdashboard/scan endpoint.
 */
@Service
public class LogsDashboardService {

    /** Extensions we never treat as text logs, even if they sit under the scanned tree. Applied
     *  only when the caller didn't supply an explicit file name filter. */
    private static final List<String> SKIP_EXTENSIONS = List.of(
            ".gz", ".zip", ".tar", ".bz2", ".xz", ".7z", ".jar", ".war", ".class",
            ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".so", ".dll", ".exe", ".bin");

    private static final long MAX_FILE_BYTES = 25L * 1024 * 1024; // 25MB cap per file read
    private static final int MAX_MATCHES_PER_FILE = 200;
    private static final int MAX_FILES_SCANNED = 2000;

    public static final String DEFAULT_SEARCH_PATTERN = "CRITICAL|ERROR|EXCEPTION";

    /** Attribute names across any MBean that plausibly hold a log file/dir path. Deliberately
     *  narrow (not "path" or "directory") to avoid false hits like ClassPath. */
    private static final Pattern FILE_ATTR_NAME_HINT =
            Pattern.compile("file|log[_-]?dir|log[_-]?path", Pattern.CASE_INSENSITIVE);

    /** JVM system-property keys (always readable via java.lang:type=Runtime/SystemProperties)
     *  whose value is itself a usable log file/dir path, not just an app base dir. */
    private static final List<String> DIRECT_PATH_PROPERTY_KEYS = List.of(
            "LOG_PATH", "LOG_FILE", "logging.path", "logging.file.path", "logging.file.name");

    /** System-property keys that name an app base/install dir; logs conventionally live under
     *  "<value>/logs", but this is a guess, not a config-backed fact. */
    private static final List<String> BASE_DIR_PROPERTY_KEYS = List.of(
            "catalina.base", "catalina.home", "user.dir");

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final ObjectMapper objectMapper;

    public LogsDashboardService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Best-effort: queries a Jolokia (JMX-over-HTTP) endpoint for MBean attributes that look like
     * a log file/dir path, plus JVM system properties that commonly point at one. Returns ranked
     * candidates rather than a single answer — most JVMs don't expose their log path via JMX at
     * all (e.g. plain Logback setups), so an empty list is an expected outcome, not a bug.
     */
    public Map<String, Object> guessLogPath(String jolokiaUrl, long timeoutMs) {
        String base = jolokiaUrl.trim().replaceAll("/+$", "");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jolokiaUrl", base);
        List<Map<String, Object>> candidates = new ArrayList<>();
        result.put("candidates", candidates);

        JsonNode listValue;
        try {
            listValue = jolokiaCall(base, List.of(Map.of("type", "list")), timeoutMs).get(0).path("value");
        } catch (Exception e) {
            result.put("error", "Could not reach Jolokia endpoint: "
                    + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
            return result;
        }

        List<Map<String, String>> hits = new ArrayList<>();
        listValue.properties().forEach(domainEntry -> {
            String domain = domainEntry.getKey();
            domainEntry.getValue().properties().forEach(mbeanEntry -> {
                String mbean = domain + ":" + mbeanEntry.getKey();
                mbeanEntry.getValue().path("attr").properties().forEach(attrEntry -> {
                    String attrName = attrEntry.getKey();
                    if (FILE_ATTR_NAME_HINT.matcher(attrName).find()) {
                        Map<String, String> hit = new LinkedHashMap<>();
                        hit.put("mbean", mbean);
                        hit.put("attribute", attrName);
                        hits.add(hit);
                    }
                });
            });
        });

        if (!hits.isEmpty()) {
            List<Map<String, Object>> readReqs = new ArrayList<>();
            for (Map<String, String> hit : hits) {
                readReqs.add(Map.of("type", "read", "mbean", hit.get("mbean"), "attribute", hit.get("attribute")));
            }
            try {
                for (JsonNode entry : jolokiaCall(base, readReqs, timeoutMs)) {
                    if (entry.path("status").asInt(0) != 200) continue;
                    JsonNode value = entry.path("value");
                    if (!value.isTextual()) continue;
                    String v = value.asText();
                    if (!looksLikePath(v)) continue;
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("source", "mbean-attribute");
                    c.put("mbean", entry.path("request").path("mbean").asText(""));
                    c.put("attribute", entry.path("request").path("attribute").asText(""));
                    c.put("value", v);
                    c.put("confidence", "high");
                    candidates.add(c);
                }
            } catch (Exception ignored) {
                // best-effort; fall through to the system-properties heuristic below
            }
        }

        try {
            JsonNode sysProps = jolokiaCall(base, List.of(Map.of(
                    "type", "read", "mbean", "java.lang:type=Runtime", "attribute", "SystemProperties"
            )), timeoutMs).get(0).path("value");

            for (String key : DIRECT_PATH_PROPERTY_KEYS) {
                String v = sysProps.path(key).asText(null);
                if (v == null || v.isBlank() || !looksLikePath(v)) continue;
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("source", "system-property");
                c.put("property", key);
                c.put("value", v);
                c.put("confidence", "medium");
                candidates.add(c);
            }
            for (String key : BASE_DIR_PROPERTY_KEYS) {
                String v = sysProps.path(key).asText(null);
                if (v == null || v.isBlank() || !looksLikePath(v)) continue;
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("source", "system-property");
                c.put("property", key);
                c.put("value", v);
                c.put("derivedPath", v.replaceAll("[/\\\\]+$", "") + "/logs");
                c.put("confidence", "low");
                candidates.add(c);
            }
        } catch (Exception ignored) {
            // best-effort; candidates collected so far (if any) are still returned
        }

        return result;
    }

    private List<JsonNode> jolokiaCall(String base, List<Map<String, Object>> requests, long timeoutMs) throws Exception {
        String body = objectMapper.writeValueAsString(requests);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(base))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IOException("Jolokia HTTP " + resp.statusCode());
        }
        JsonNode root = objectMapper.readTree(resp.body());
        List<JsonNode> out = new ArrayList<>();
        if (root.isArray()) root.forEach(out::add); else out.add(root);
        return out;
    }

    /** Rejects classpath-style multi-entry strings and anything that isn't an absolute path, so a
     *  MBean attribute like "ClassPath" doesn't masquerade as a log path just for matching "file". */
    private boolean looksLikePath(String v) {
        String s = v.trim();
        if (s.isEmpty() || s.length() > 400) return false;
        boolean windowsAbsolute = s.matches("^[A-Za-z]:[\\\\/].*");
        if (!s.startsWith("/") && !windowsAbsolute) return false;
        String rest = windowsAbsolute ? s.substring(2) : s;
        return rest.indexOf(';') < 0 && rest.indexOf(':') < 0;
    }

    public Map<String, Object> scan(String rootPath, int minutesBack, String fileNameGlob, String searchPattern) {
        Path root = Path.of(rootPath);
        if (!Files.exists(root)) {
            throw new IllegalArgumentException("Path does not exist: " + rootPath);
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Path is not a directory: " + rootPath);
        }

        Pattern fileNamePattern = (fileNameGlob == null || fileNameGlob.isBlank())
                ? null : globToRegex(fileNameGlob.trim());

        String searchText = (searchPattern == null || searchPattern.isBlank())
                ? DEFAULT_SEARCH_PATTERN : searchPattern.trim();
        Pattern issuePattern;
        try {
            issuePattern = Pattern.compile(searchText, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid search pattern: " + e.getMessage());
        }

        Instant cutoff = Instant.now().minusSeconds(minutesBack * 60L);

        List<Path> candidates = new ArrayList<>();
        int[] totalFilesHolder = new int[1];
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path p, BasicFileAttributes attrs) {
                    if (!attrs.isRegularFile()) {
                        return FileVisitResult.CONTINUE;
                    }
                    totalFilesHolder[0]++;
                    if (!isSkippable(p, fileNamePattern) && !attrs.lastModifiedTime().toInstant().isBefore(cutoff)) {
                        candidates.add(p);
                    }
                    return candidates.size() >= MAX_FILES_SCANNED
                            ? FileVisitResult.TERMINATE
                            : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path p, IOException exc) {
                    // Skip files/dirs we can't access (e.g. AccessDeniedException) and keep walking.
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to walk " + rootPath + ": " + e.getMessage(), e);
        }
        int totalFiles = totalFilesHolder[0];

        List<Map<String, Object>> files = new ArrayList<>();
        int totalIssues = 0;
        for (Path p : candidates) {
            Map<String, Object> fileResult = scanFile(p, issuePattern);
            files.add(fileResult);
            totalIssues += (int) fileResult.get("issueCount");
        }
        files.sort((a, b) -> Long.compare((long) b.get("lastModifiedEpochMs"), (long) a.get("lastModifiedEpochMs")));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("rootPath", root.toString());
        result.put("minutesBack", minutesBack);
        result.put("fileNamePattern", fileNameGlob == null ? "" : fileNameGlob.trim());
        result.put("searchPattern", searchText);
        result.put("scannedAt", Instant.now().toString());
        result.put("totalFilesUnderPath", totalFiles);
        result.put("recentFileCount", candidates.size());
        result.put("totalIssueCount", totalIssues);
        result.put("files", files);
        return result;
    }

    private Map<String, Object> scanFile(Path p, Pattern issuePattern) {
        Map<String, Object> fileResult = new LinkedHashMap<>();
        fileResult.put("path", p.toString());

        FileTime mtime;
        long size;
        try {
            mtime = Files.getLastModifiedTime(p);
            size = Files.size(p);
        } catch (IOException e) {
            fileResult.put("lastModifiedEpochMs", 0L);
            fileResult.put("lastModified", null);
            fileResult.put("sizeBytes", 0L);
            fileResult.put("issueCount", 0);
            fileResult.put("matches", List.of());
            fileResult.put("error", e.getMessage());
            return fileResult;
        }

        fileResult.put("lastModifiedEpochMs", mtime.toMillis());
        fileResult.put("lastModified", mtime.toInstant().toString());
        fileResult.put("sizeBytes", size);

        List<Map<String, Object>> matches = new ArrayList<>();
        int issueCount = 0;
        if (size > MAX_FILE_BYTES) {
            fileResult.put("truncated", true);
        }

        try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (!issuePattern.matcher(line).find()) continue;
                issueCount++;
                if (matches.size() < MAX_MATCHES_PER_FILE) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("lineNumber", lineNumber);
                    m.put("level", classify(line));
                    m.put("text", line.length() > 2000 ? line.substring(0, 2000) + "..." : line);
                    matches.add(m);
                }
            }
        } catch (IOException | RuntimeException e) {
            fileResult.put("error", "Could not read file: " + e.getMessage());
        }

        fileResult.put("issueCount", issueCount);
        fileResult.put("matches", matches);
        return fileResult;
    }

    private String classify(String line) {
        String upper = line.toUpperCase();
        if (upper.contains("CRITICAL")) return "CRITICAL";
        if (upper.contains("ERROR")) return "ERROR";
        if (upper.contains("EXCEPTION")) return "EXCEPTION";
        return "MATCH";
    }

    /** Filters by an explicit file name pattern when given, otherwise falls back to the
     *  default binary/archive extension blocklist. */
    private boolean isSkippable(Path p, Pattern fileNamePattern) {
        String name = p.getFileName().toString();
        if (fileNamePattern != null) {
            return !fileNamePattern.matcher(name).matches();
        }
        String lower = name.toLowerCase();
        for (String ext : SKIP_EXTENSIONS) {
            if (lower.endsWith(ext)) return true;
        }
        return false;
    }

    /** Translates a simple glob (e.g. "*.log" or "*.log*") into an anchored, case-insensitive regex. */
    private Pattern globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (c == '*') {
                regex.append(".*");
            } else if (c == '?') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }
}
