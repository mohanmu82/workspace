package com.mycompany.batch.service;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Walks a top-level unix log directory, finds files modified within a recent window, and greps
 * each one for CRITICAL/ERROR/exception markers — backs the /logsdashboard/scan endpoint.
 */
@Service
public class LogsDashboardService {

    /** Extensions we never treat as text logs, even if they sit under the scanned tree. */
    private static final List<String> SKIP_EXTENSIONS = List.of(
            ".gz", ".zip", ".tar", ".bz2", ".xz", ".7z", ".jar", ".war", ".class",
            ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".so", ".dll", ".exe", ".bin");

    private static final long MAX_FILE_BYTES = 25L * 1024 * 1024; // 25MB cap per file read
    private static final int MAX_MATCHES_PER_FILE = 200;
    private static final int MAX_FILES_SCANNED = 2000;

    private static final Pattern ISSUE_PATTERN =
            Pattern.compile("CRITICAL|ERROR|EXCEPTION", Pattern.CASE_INSENSITIVE);

    public Map<String, Object> scan(String rootPath, int minutesBack) {
        Path root = Path.of(rootPath);
        if (!Files.exists(root)) {
            throw new IllegalArgumentException("Path does not exist: " + rootPath);
        }
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Path is not a directory: " + rootPath);
        }

        Instant cutoff = Instant.now().minusSeconds(minutesBack * 60L);

        List<Path> candidates = new ArrayList<>();
        int totalFiles = 0;
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                if (!Files.isRegularFile(p)) continue;
                totalFiles++;
                if (isSkippable(p)) continue;
                FileTime mtime;
                try {
                    mtime = Files.getLastModifiedTime(p);
                } catch (IOException e) {
                    continue;
                }
                if (mtime.toInstant().isBefore(cutoff)) continue;
                candidates.add(p);
                if (candidates.size() >= MAX_FILES_SCANNED) break;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to walk " + rootPath + ": " + e.getMessage(), e);
        }

        List<Map<String, Object>> files = new ArrayList<>();
        int totalIssues = 0;
        for (Path p : candidates) {
            Map<String, Object> fileResult = scanFile(p);
            files.add(fileResult);
            totalIssues += (int) fileResult.get("issueCount");
        }
        files.sort((a, b) -> Long.compare((long) b.get("lastModifiedEpochMs"), (long) a.get("lastModifiedEpochMs")));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("rootPath", root.toString());
        result.put("minutesBack", minutesBack);
        result.put("scannedAt", Instant.now().toString());
        result.put("totalFilesUnderPath", totalFiles);
        result.put("recentFileCount", candidates.size());
        result.put("totalIssueCount", totalIssues);
        result.put("files", files);
        return result;
    }

    private Map<String, Object> scanFile(Path p) {
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
                if (!ISSUE_PATTERN.matcher(line).find()) continue;
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
        return "EXCEPTION";
    }

    private boolean isSkippable(Path p) {
        String name = p.getFileName().toString().toLowerCase();
        for (String ext : SKIP_EXTENSIONS) {
            if (name.endsWith(ext)) return true;
        }
        return false;
    }
}
