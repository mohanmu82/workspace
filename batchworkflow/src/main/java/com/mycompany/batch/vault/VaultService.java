package com.mycompany.batch.vault;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Writes rows into an Obsidian vault as markdown notes: YAML front matter for the summary (which
 * Dataview can query across notes) and a markdown table for the rows. A vault is only a folder of
 * .md files, so writing the file is the whole integration — Obsidian picks it up as it lands.
 */
@Service
public class VaultService {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String vaultDir;

    public VaultService(@Value("${obsidian.vault-dir:}") String vaultDir) {
        this.vaultDir = vaultDir;
    }

    /**
     * Writes a note at {@code notePath}, relative to the vault, and returns the absolute file.
     * The path comes from the browser, so it must stay inside the vault.
     */
    public Path writeNote(String notePath, Map<String, Object> properties,
                          List<String> columns, List<Map<String, Object>> rows) throws Exception {
        if (vaultDir == null || vaultDir.isBlank()) {
            throw new IllegalStateException("No Obsidian vault configured: set obsidian.vault-dir");
        }
        if (notePath == null || notePath.isBlank()) throw new IllegalArgumentException("path is required");
        if (notePath.matches(".*[<>:\"|?*].*")) {
            throw new IllegalArgumentException("path may not contain any of < > : \" | ? *");
        }
        Path root = Path.of(vaultDir).toAbsolutePath().normalize();
        Path file = root.resolve(notePath.endsWith(".md") ? notePath : notePath + ".md").normalize();
        if (!file.startsWith(root) || file.equals(root)) {
            throw new IllegalArgumentException("path must stay inside the vault: " + notePath);
        }
        Files.createDirectories(file.getParent());
        Files.writeString(file, render(properties, columns, rows));
        return file;
    }

    /** Front matter (when there are properties) followed by the rows as a table. */
    public static String render(Map<String, Object> properties, List<String> columns,
                                List<Map<String, Object>> rows) {
        StringBuilder md = new StringBuilder();
        if (properties != null && !properties.isEmpty()) {
            md.append("---\n");
            // A JSON scalar is also a valid YAML scalar, which spares quoting rules of our own.
            properties.forEach((k, v) -> md.append(k).append(": ").append(yamlValue(v)).append('\n'));
            md.append("---\n\n");
        }
        md.append(table(columns, rows));
        return md.toString();
    }

    /** The rows as a markdown table; columns default to every key seen, in first-seen order. */
    public static String table(Collection<String> columns, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) return "_No rows._\n";
        Collection<String> cols = columns;
        if (cols == null || cols.isEmpty()) {
            LinkedHashSet<String> seen = new LinkedHashSet<>();
            rows.forEach(r -> seen.addAll(r.keySet()));
            cols = seen;
        }
        StringBuilder md = new StringBuilder();
        md.append('|');
        cols.forEach(c -> md.append(' ').append(cell(c)).append(" |"));
        md.append("\n|");
        cols.forEach(c -> md.append(" --- |"));
        md.append('\n');
        for (Map<String, Object> row : rows) {
            md.append('|');
            for (String c : cols) md.append(' ').append(cell(row.get(c))).append(" |");
            md.append('\n');
        }
        return md.toString();
    }

    private static String cell(Object value) {
        if (value == null) return "";
        String text = value instanceof Map || value instanceof Collection ? toJson(value) : value.toString();
        // A pipe would end the cell and a line break the row.
        return text.replace("\\", "\\\\").replace("|", "\\|").replaceAll("\\r?\\n|\\r", "<br>");
    }

    private static String yamlValue(Object value) {
        if (value == null) return "null";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        return toJson(value instanceof Map || value instanceof Collection ? value : value.toString());
    }

    private static String toJson(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }
}
