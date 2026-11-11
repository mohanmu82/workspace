package com.mycompany.batch.appcatalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * JDBC for the ORACLE and POSTGRES app modes: running a use case's SQL, and appending the rows a
 * page grid holds to a table so one run can be compared with the next.
 *
 * <p>A published table always starts with {@code run_uuid} (one per publish), {@code captured_at}
 * and {@code source} (page/action or grid the rows came from), then one text column per grid column.
 * It is created on first use and grows a column whenever a grid shows one it has not seen; nothing is
 * ever typed more precisely than text, because a grid cell is text.
 */
@Service
public class DbPublishService {

    /** Oracle's traditional identifier limit; Postgres allows 63, so this fits both. */
    static final int MAX_IDENTIFIER = 30;
    /** Oracle's VARCHAR2 limit in bytes; a longer cell is cut rather than failing the whole publish. */
    private static final int ORACLE_MAX_CHARS = 4000;
    /** Words that cannot be an unquoted column name in Oracle or Postgres and are likely grid headings. */
    private static final Set<String> RESERVED = Set.of(
            "access", "all", "and", "any", "as", "asc", "between", "by", "case", "char", "check", "column", "comment",
            "date", "decimal", "default", "delete", "desc", "distinct", "else", "end", "exists", "file", "float", "for",
            "from", "group", "having", "in", "index", "insert", "int", "integer", "into", "is", "key", "level", "like",
            "limit", "lock", "long", "mode", "not", "null", "number", "of", "offset", "on", "online", "option", "or",
            "order", "raw", "resource", "rowid", "select", "session", "set", "share", "size", "start", "table", "then",
            "timestamp", "to", "type", "uid", "union", "update", "user", "value", "values", "when", "where", "with");

    private final AppCatalogService catalog;
    private final ObjectMapper mapper;

    public DbPublishService(AppCatalogService catalog, ObjectMapper mapper) {
        this.catalog = catalog;
        this.mapper = mapper;
    }

    /** The environment's URL prefix is its JDBC URL; its username and password are the login. */
    static Connection connect(AppEnvironment env, String jdbcUrl) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, env.getUsername(), env.getPassword());
    }

    /**
     * Runs one statement and answers as JSON: a query's rows as an array of objects keyed by column
     * label, anything else (insert, update, create …) as {@code {"updatedRows": n}}.
     */
    static String runSql(ObjectMapper mapper, AppEnvironment env, String jdbcUrl, String sql, int timeoutMs) throws Exception {
        try (Connection conn = connect(env, jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(Math.max(1, timeoutMs / 1000));
            if (!stmt.execute(sql)) return mapper.writeValueAsString(Map.of("updatedRows", stmt.getUpdateCount()));
            List<Map<String, Object>> rows = new ArrayList<>();
            try (ResultSet rs = stmt.getResultSet()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) row.put(meta.getColumnLabel(i), cellValue(rs.getObject(i)));
                    rows.add(row);
                }
            }
            return mapper.writeValueAsString(rows);
        }
    }

    private static Object cellValue(Object v) throws SQLException {
        if (v == null || v instanceof Number || v instanceof Boolean || v instanceof String) return v;
        if (v instanceof Clob c) return c.getSubString(1, (int) c.length());
        return v.toString(); // dates, timestamps and a driver's own types: text is what a grid shows anyway
    }

    public record Published(String table, int inserted, String runId) {}

    /**
     * Appends {@code rows} to {@code table} in the named database app environment under one fresh
     * {@code run_uuid}. {@code columns} fixes the column order; blank takes the first row's keys.
     */
    public Published publish(String appName, String environment, String table, String source,
                             List<String> columns, List<Map<String, Object>> rows) throws Exception {
        AppDefinition app = catalog.getApp(appName);
        AppEnvironment env = catalog.getEnvironment(appName, environment);
        if (app == null || !app.isDatabase()) throw new IllegalArgumentException("'" + appName + "' is not an ORACLE or POSTGRES app");
        if (env == null) throw new IllegalArgumentException("App '" + appName + "' has no environment '" + environment + "'");
        if (table == null || table.isBlank()) throw new IllegalArgumentException("table is required");
        if (rows == null) rows = List.of();
        if (columns == null || columns.isEmpty()) columns = rows.isEmpty() ? List.of() : new ArrayList<>(rows.get(0).keySet());

        boolean oracle = "ORACLE".equalsIgnoreCase(app.getAppMode());
        String tableName = identifier(table);
        Map<String, String> names = columnNames(columns);
        String runId = UUID.randomUUID().toString();
        Timestamp now = new Timestamp(System.currentTimeMillis());

        List<String> all = new ArrayList<>(List.of("run_uuid", "captured_at", "source"));
        all.addAll(names.values());
        String insert = "INSERT INTO " + tableName + " (" + String.join(", ", all) + ") VALUES (?" + ", ?".repeat(all.size() - 1) + ")";
        try (Connection conn = connect(env, env.getUrlPrefix())) {
            ensureTable(conn, tableName, names.values(), oracle);
            try (PreparedStatement ps = conn.prepareStatement(insert)) {
                for (Map<String, Object> row : rows) {
                    ps.setString(1, runId);
                    ps.setTimestamp(2, now);
                    ps.setString(3, source);
                    int i = 4;
                    for (String column : names.keySet()) ps.setString(i++, text(row.get(column), oracle));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        return new Published(tableName, rows.size(), runId);
    }

    private static String text(Object value, boolean oracle) {
        if (value == null) return null;
        String s = value instanceof String str ? str : String.valueOf(value);
        // shortcut: cut by characters, not bytes; a cell of 4000 multibyte characters still fails on Oracle
        return oracle && s.length() > ORACLE_MAX_CHARS ? s.substring(0, ORACLE_MAX_CHARS) : s;
    }

    /** Creates the table, or adds the columns it is missing — nothing is ever dropped or retyped. */
    private static void ensureTable(Connection conn, String table, Collection<String> columns, boolean oracle) throws SQLException {
        String type = oracle ? "VARCHAR2(" + ORACLE_MAX_CHARS + ")" : "TEXT";
        Set<String> existing = existingColumns(conn, table);
        try (Statement stmt = conn.createStatement()) {
            if (existing == null) {
                stmt.execute(createTable(table, columns, type));
            } else {
                for (String c : columns)
                    if (!existing.contains(c.toUpperCase(Locale.ROOT))) stmt.execute("ALTER TABLE " + table + " ADD " + c + " " + type);
            }
        }
    }

    static String createTable(String table, Collection<String> columns, String type) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(table)
                .append(" (run_uuid VARCHAR(36), captured_at TIMESTAMP, source ").append(type);
        for (String c : columns) ddl.append(", ").append(c).append(' ').append(type);
        return ddl.append(')').toString();
    }

    /** The table's columns upper-cased, or null when the connection's schema has no such table. */
    private static Set<String> existingColumns(Connection conn, String table) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        // Oracle folds unquoted names to upper case, Postgres to lower: ask for the one it would have kept.
        for (String candidate : List.of(table.toUpperCase(Locale.ROOT), table.toLowerCase(Locale.ROOT))) {
            Set<String> cols = new HashSet<>();
            try (ResultSet rs = md.getColumns(null, conn.getSchema(), candidate, null)) {
                while (rs.next()) cols.add(rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
            }
            if (!cols.isEmpty()) return cols;
        }
        return null;
    }

    /**
     * A name safe to write unquoted in both dialects: lower-case letters, digits and underscores,
     * starting with a letter, clear of reserved words, at most {@value #MAX_IDENTIFIER} characters.
     */
    static String identifier(String raw) {
        String name = (raw == null ? "" : raw).trim().toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]+", "_").replaceAll("^_+|_+$", "");
        if (name.isEmpty() || !Character.isLetter(name.charAt(0))) name = "c_" + name;
        if (name.length() > MAX_IDENTIFIER) name = name.substring(0, MAX_IDENTIFIER);
        return RESERVED.contains(name) ? name + "_" : name;
    }

    /** Each grid column's identifier, in order, made distinct from each other and from the fixed columns. */
    static Map<String, String> columnNames(List<String> columns) {
        Map<String, String> out = new LinkedHashMap<>();
        Set<String> taken = new HashSet<>(List.of("run_uuid", "captured_at", "source"));
        for (String column : columns) {
            if (column == null || out.containsKey(column)) continue;
            String base = identifier(column), name = base;
            for (int n = 2; taken.contains(name); n++) {
                String suffix = "_" + n;
                name = base.substring(0, Math.min(base.length(), MAX_IDENTIFIER - suffix.length())) + suffix;
            }
            taken.add(name);
            out.put(column, name);
        }
        return out;
    }
}
