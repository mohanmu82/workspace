package com.mycompany.batch.enricher;

import com.mycompany.batch.cache.CacheFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Loads enricher config files and applies derived-attribute rules to DataRow maps.
 *
 * <h3>Placeholder syntax</h3>
 * <ul>
 *   <li>{@code ${DATESTAMP}} — today as {@code yyyyMMdd}; resolved in dataset names / paths / keys</li>
 *   <li>{@code ${colName}}  — value from the current DataRow (including previously computed attributes)</li>
 * </ul>
 *
 * <h3>Attribute types</h3>
 * <ul>
 *   <li>{@code replace} — string template interpolation</li>
 *   <li>{@code sum}     — numeric: split {@code value} on {@code +}, sum all parts</li>
 *   <li>{@code divide}  — numeric: split {@code value} on {@code /}, divide left-to-right</li>
 *   <li>{@code lookup}  — dataset lookup: resolve {@code key} → look up row in {@code dataset} → return {@code attribute}</li>
 * </ul>
 */
@Service
public class EnricherService {

    private static final DateTimeFormatter DATESTAMP_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final Pattern           PLACEHOLDER   = Pattern.compile("\\$\\{([^}]+)\\}");

    private final ObjectMapper objectMapper;
    private final CacheFactory cacheFactory;

    public EnricherService(ObjectMapper objectMapper, CacheFactory cacheFactory) {
        this.objectMapper = objectMapper;
        this.cacheFactory = cacheFactory;
    }

    // -------------------------------------------------------------------------
    // Config loading
    // -------------------------------------------------------------------------

    public EnricherConfig loadConfig(String source) throws Exception {
        try (InputStream is = openResource(source, "Enricher config")) {
            return objectMapper.readValue(is, EnricherConfig.class);
        }
    }

    // -------------------------------------------------------------------------
    // Dataset loading — call once per batch, share across all rows
    // -------------------------------------------------------------------------

    /**
     * Loads every dataset defined in the config.
     *
     * @return {@code datasetName → (keyValue → fullRow)}
     */
    public Map<String, Map<String, Map<String, Object>>> loadDatasets(EnricherConfig config) throws Exception {
        Map<String, String> sys = systemVars();
        Map<String, Map<String, Map<String, Object>>> result = new LinkedHashMap<>();
        for (DatasetDef def : config.getDataset()) {
            String resolvedName = resolveSystemVars(def.getName(), sys);
            result.put(resolvedName, loadDataset(def, sys));
        }
        return result;
    }

    private Map<String, Map<String, Object>> loadDataset(DatasetDef def,
                                                          Map<String, String> sys) throws Exception {
        DataSourceDef   ds        = def.getDataSource();
        DatasetCacheDef cacheCfg  = ds.getCache();
        String          cacheName = null;
        String          cacheKey  = null;

        if (cacheCfg != null && !cacheCfg.getName().isBlank()) {
            cacheName = cacheCfg.getName();
            cacheKey  = resolveSystemVars(cacheCfg.getKey(), sys);
            String hit = cacheFactory.get(cacheName, cacheKey);
            if (hit != null) {
                return objectMapper.readValue(hit,
                        new TypeReference<Map<String, Map<String, Object>>>() {});
            }
        }

        List<Map<String, Object>> rows = loadRawRows(ds, sys);

        Map<String, Map<String, Object>> indexed = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            Object keyVal = row.get(def.getKey());
            if (keyVal != null) indexed.put(keyVal.toString(), row);
        }

        if (cacheName != null) {
            String sourceRef = resolveSystemVars(
                    ds.getInputFilePath() != null ? ds.getInputFilePath() : ds.getInputHttpUrl(), sys);
            cacheFactory.save(cacheName, cacheKey,
                    objectMapper.writeValueAsString(indexed), sourceRef);
        }

        return indexed;
    }

    private List<Map<String, Object>> loadRawRows(DataSourceDef ds,
                                                   Map<String, String> sys) throws Exception {
        String type = ds.getInputSource() != null ? ds.getInputSource().trim().toUpperCase() : "FILE";
        return switch (type) {
            case "FILE" -> loadFromFile(resolveSystemVars(ds.getInputFilePath(), sys));
            case "HTTP", "HTTPCONFIG" -> loadFromHttp(resolveSystemVars(ds.getInputHttpUrl(), sys));
            default -> throw new IllegalArgumentException(
                    "Enricher dataset inputSource must be FILE or HTTPCONFIG, got: " + type);
        };
    }

    private List<Map<String, Object>> loadFromFile(String path) throws Exception {
        List<String> lines = Files.readAllLines(Path.of(path));
        if (lines.isEmpty()) return List.of();

        String   header      = lines.get(0);
        String[] commaTokens = header.split(",", -1);
        String   delimiter   = commaTokens.length > 1 ? "," : "|";
        String[] headers     = Arrays.stream(
                delimiter.equals(",") ? commaTokens : header.split(Pattern.quote("|"), -1))
                .map(String::trim).toArray(String[]::new);
        String delimPat = Pattern.quote(delimiter);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isBlank()) continue;
            String[] vals = line.split(delimPat, -1);
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < headers.length; j++) {
                row.put(headers[j], j < vals.length ? vals[j].trim() : "");
            }
            rows.add(row);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> loadFromHttp(String url) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new RuntimeException("Enricher dataset HTTP source returned HTTP " + resp.statusCode());
        }
        Map<String, Object> parsed = objectMapper.readValue(resp.body(), new TypeReference<>() {});
        Object data = parsed.get("data");
        if (!(data instanceof List<?>)) {
            throw new RuntimeException("Enricher dataset HTTP source: expected a 'data' array");
        }
        return ((List<Object>) data).stream()
                .filter(item -> item instanceof Map)
                .map(item -> (Map<String, Object>) item)
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Row enrichment — call per row; datasets must already be loaded
    // -------------------------------------------------------------------------

    /**
     * Evaluates each attribute definition in order and puts the result into {@code rowData}.
     * Attributes computed earlier in the list are visible to later ones via {@code ${...}} references.
     * On error: if {@code onError} is set it is resolved and stored; otherwise the attribute is skipped.
     */
    public void enrichRow(Map<String, Object> rowData,
                          List<DataAttributeDef> attrs,
                          Map<String, Map<String, Map<String, Object>>> datasets) {
        Map<String, String> sys = systemVars();
        for (DataAttributeDef attr : attrs) {
            String key = attr.getName().toUpperCase();
            try {
                rowData.put(key, compute(attr, rowData, datasets, sys));
            } catch (Exception e) {
                if (attr.getOnError() != null) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    String errMsg = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
                    Map<String, String> sysWithError = new LinkedHashMap<>(sys);
                    sysWithError.put("errorMessage", errMsg);
                    try {
                        rowData.put(key, resolveExpr(attr.getOnError(), rowData, sysWithError));
                    } catch (Exception ignored) {
                        rowData.put(key, attr.getOnError());
                    }
                }
                // no onError → attribute simply not added
            }
        }
    }

    // -------------------------------------------------------------------------
    // Attribute computation
    // -------------------------------------------------------------------------

    private String compute(DataAttributeDef attr,
                           Map<String, Object> rowData,
                           Map<String, Map<String, Map<String, Object>>> datasets,
                           Map<String, String> sys) throws Exception {
        return switch (attr.getType().toLowerCase()) {

            case "replace" -> resolveExpr(attr.getValue(), rowData, sys);

            case "sum" -> {
                String expr = resolveExpr(attr.getValue(), rowData, sys);
                double sum  = 0;
                for (String part : expr.split("\\+")) sum += Double.parseDouble(part.trim());
                yield formatNumber(sum);
            }

            case "divide" -> {
                String   expr   = resolveExpr(attr.getValue(), rowData, sys);
                String[] parts  = expr.split("/");
                double   result = Double.parseDouble(parts[0].trim());
                for (int i = 1; i < parts.length; i++) result /= Double.parseDouble(parts[i].trim());
                yield formatNumber(result);
            }

            case "lookup" -> {
                String datasetName = resolveExpr(attr.getDataset(),   rowData, sys);
                String keyVal      = resolveExpr(attr.getKey(),       rowData, sys);
                String attrName    = resolveExpr(attr.getAttribute(), rowData, sys);

                Map<String, Map<String, Object>> dataset = datasets.get(datasetName);
                if (dataset == null)
                    throw new RuntimeException("Dataset not found: " + datasetName);

                Map<String, Object> matchedRow = dataset.get(keyVal);
                if (matchedRow == null)
                    throw new RuntimeException("Key '" + keyVal + "' not found in dataset '" + datasetName + "'");

                Object val = matchedRow.get(attrName);
                if (val == null)
                    throw new RuntimeException("Attribute '" + attrName + "' not in dataset row");

                yield val.toString();
            }

            case "filecontents" -> {
                String resolvedPath = resolveExpr(attr.getValue(), rowData, sys);
                String raw = Files.readString(Path.of(resolvedPath));
                yield resolveExpr(raw, rowData, sys);
            }

            case "springexl" -> {
                String spel = attr.getValue() != null ? attr.getValue().trim() : "";
                if (spel.startsWith("#{") && spel.endsWith("}")) {
                    spel = spel.substring(2, spel.length() - 1);
                }
                ExpressionParser parser = new SpelExpressionParser();
                StandardEvaluationContext ctx = new StandardEvaluationContext(rowData);
                ctx.addPropertyAccessor(new MapAccessor());
                rowData.forEach(ctx::setVariable);
                Expression exp = parser.parseExpression(spel);
                Object result = exp.getValue(ctx);
                yield result != null ? result.toString() : "";
            }

            case "tokenize" -> {
                String input     = resolveExpr(attr.getString(), rowData, sys);
                String delimiter = attr.getDelimiter() != null ? attr.getDelimiter() : ".";
                int    index     = attr.getToken() != null ? Integer.parseInt(attr.getToken().trim()) : 1;
                String[] parts   = input.split(Pattern.quote(delimiter), -1);
                if (index < 1 || index > parts.length)
                    throw new RuntimeException("Token index " + index + " out of range (1–" + parts.length + ")");
                yield parts[index - 1];
            }

            case "base64decode" -> {
                String input = resolveExpr(attr.getString(), rowData, sys);
                yield new String(java.util.Base64.getDecoder().decode(input.trim()));
            }

            case "cascade" -> {
                String cascadeExpr = attr.getString() != null ? attr.getString() : attr.getValue();
                String[] candidates = cascadeExpr != null
                        ? cascadeExpr.split(Pattern.quote("|"), -1) : new String[0];
                for (String candidate : candidates) {
                    String key = candidate.trim();
                    Object val = rowData.get(key);
                    if (val == null) {
                        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
                            if (entry.getKey().equalsIgnoreCase(key)) { val = entry.getValue(); break; }
                        }
                    }
                    if (val != null && !val.toString().isEmpty()) yield val.toString();
                }
                throw new RuntimeException("cascade: no non-empty value found in candidates: " + cascadeExpr);
            }

            default -> throw new IllegalArgumentException("Unknown enricher type: " + attr.getType());
        };
    }

    // -------------------------------------------------------------------------
    // Template resolution helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves {@code ${key}} placeholders: rowData first, sysVars as fallback.
     * Throws if a placeholder cannot be resolved.
     */
    private static String resolveExpr(String template,
                                      Map<String, Object> rowData,
                                      Map<String, String> sys) {
        if (template == null) return "";
        Map<String, Object> ci = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ci.putAll(rowData);
        Map<String, String> ciSys = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ciSys.putAll(sys);
        Matcher      m  = PLACEHOLDER.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1);
            Object val = ci.get(key);
            if (val == null) val = ciSys.get(key);
            if (val == null)
                throw new IllegalArgumentException(
                        "Template references key '" + key + "' which is not present in the DataRow. Available row keys: " + rowData.keySet());
            m.appendReplacement(sb, Matcher.quoteReplacement(val.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** Resolves only system-level {@code ${...}} placeholders (used for dataset names / paths). */
    private static String resolveSystemVars(String template, Map<String, String> sys) {
        if (template == null) return null;
        Matcher      m  = PLACEHOLDER.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, Matcher.quoteReplacement(
                    sys.getOrDefault(m.group(1), "")));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static Map<String, String> systemVars() {
        return Map.of("DATESTAMP", LocalDate.now().format(DATESTAMP_FMT));
    }

    /** Formats a double as integer string when it has no fractional part. */
    private static String formatNumber(double d) {
        return d == Math.floor(d) && !Double.isInfinite(d)
                ? String.valueOf((long) d)
                : String.valueOf(d);
    }

    // -------------------------------------------------------------------------
    // Resource loader
    // -------------------------------------------------------------------------

    private InputStream openResource(String source, String label) throws Exception {
        if (source.startsWith("classpath:")) {
            String res = source.substring("classpath:".length());
            InputStream is = getClass().getClassLoader().getResourceAsStream(res);
            if (is == null) throw new FileNotFoundException(label + " not found on classpath: " + res);
            return is;
        }
        Path p = Path.of(source);
        if (!Files.exists(p)) throw new FileNotFoundException(label + " not found: " + source);
        return new FileInputStream(p.toFile());
    }
}
