package com.mycompany.batch.appcatalog;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.appcatalog.AppPageMcpTools.Output;
import com.mycompany.batch.staticdataset.StaticDatasetService;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java twin of what {@code apppage.js#bindEveryTarget} does to a response before a control shows it:
 * each target's transform chain (XML→JSON, JSONata), its path, its enriched columns and its bind
 * filters. It is what lets an MCP tool hand back what the grid would hold rather than the raw body.
 *
 * <p>Two things the page does are not repeated, and are reported as notes instead: lookups into
 * another grid on the page (a tool call has no page holding rows), and a pivot's grouping.
 */
@Component
public class AppPageOutputReader {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern BRACED = Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)\\}");
    private static final Pattern TOKEN = Pattern.compile("([^.\\/\\[\\]]+)|\\[([^\\]]*)\\]");
    private static final Pattern FILTER = Pattern.compile("^(.+?)\\s*(!=|=)\\s*([\\s\\S]*)$");
    private static final Pattern SLASHED = Pattern.compile("^/(.+)/([gimsuy]*)$");
    private static final Pattern GROUP_REF = Pattern.compile("\\$\\$|\\$&|\\$(\\d+)");
    private static final DateTimeFormatter STARTED = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final JsonataLibraryService library;
    private final StaticDatasetService datasets;

    public AppPageOutputReader(JsonataLibraryService library, StaticDatasetService datasets) {
        this.library = library;
        this.datasets = datasets;
    }

    /**
     * Each target's data under its key, in order. A target that cannot be read goes into
     * {@code problems} instead and the rest still read, as each binding does on the page.
     */
    public Map<String, Object> read(AppPage page, Map<String, Output> targets, AppUseCaseInstanceOutput out,
                                    Map<String, Object> sent, Map<String, Object> scope,
                                    Map<String, String> problems, Set<String> notes) {
        Map<String, Object> data = new LinkedHashMap<>();
        Map<String, Object> roots = new HashMap<>(); // one parse and chain per source + chain, as sharedRoot does
        targets.forEach((key, o) -> {
            try {
                String rootKey = o.source() + o.transformNames();
                if (!roots.containsKey(rootKey)) roots.put(rootKey, root(page, o, out, scope));
                data.put(key, target(o, roots.get(rootKey), out, sent, scope, notes, key));
            } catch (Exception e) {
                problems.put(key, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            }
        });
        return data;
    }

    // ── The root: source, then the transform chain ───────────────────────────

    private Object root(AppPage page, Output o, AppUseCaseInstanceOutput out, Map<String, Object> scope) throws Exception {
        Object data = AppPageAction.METADATA.equals(o.source()) ? record(out) : payload(out);
        for (String step : o.transformNames()) data = step(page, step, data, scope);
        return data;
    }

    /** The use case's own transform output when it has one, else the body parsed, else the body as text (XML). */
    static Object payload(AppUseCaseInstanceOutput out) {
        if (out.transformedResponse() != null) return out.transformedResponse();
        String body = out.responseBody();
        if (body == null || body.isBlank()) return null;
        try {
            return JSON.readValue(body, Object.class);
        } catch (Exception e) {
            return body;
        }
    }

    private Object step(AppPage page, String step, Object data, Map<String, Object> scope) throws Exception {
        AppPageTransform t = page.getTransforms().stream().filter(x -> step.equals(x.getName())).findFirst().orElse(null);
        if (t == null) return data; // a step the page no longer has is skipped, as on the page
        if (t.isXml2Json()) return xmlToJson(data, step);
        if (t.isXsl2Csv()) return xslToCsvRows(data, t.getExpression(), step);
        String expression = t.isLibraryRef() ? library.expressionOf(t.getJsonataRef()) : t.getExpression();
        if (t.isLibraryRef() && expression == null) {
            throw new IllegalStateException("transform \"" + step + "\" names the shared JSONata \""
                    + t.getJsonataRef() + "\", which is not in the library.");
        }
        if (expression == null || expression.isBlank()) return data;
        String text = resolveBraced(expression, scope, "transform \"" + step + "\"");
        try {
            return Jsonata.jsonata(text).evaluate(data);
        } catch (Exception e) {
            throw new IllegalStateException("JSONata \"" + step + "\" failed: " + e.getMessage(), e);
        }
    }

    /** Java twin of apppage.js#xmlToJsonValue. */
    static Object xmlToJson(Object data, String step) throws Exception {
        return nodeToObj(parseXml(data, step, "the response").getDocumentElement());
    }

    /** The XML text a first step was handed, parsed without DOCTYPEs. {@code what} names it in errors. */
    private static Document parseXml(Object data, String step, String what) throws Exception {
        if (data == null || "".equals(data)) {
            throw new IllegalArgumentException("transform \"" + step + "\" had no XML to convert — " + what + " was empty.");
        }
        if (!(data instanceof String text)) {
            throw new IllegalArgumentException("transform \"" + step + "\" converts XML text, but was handed "
                    + kindOf(data) + " — an XML step belongs first, on the body as it arrived.");
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true); // XSLT matches on namespaces; xml→json reads tag names, which this leaves as written
        // The body comes from a remote endpoint: no DOCTYPE, so no external entities.
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setExpandEntityReferences(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setErrorHandler(new DefaultHandler()); // quiet: the failure is reported below, not on stderr
        try {
            return builder.parse(new InputSource(new StringReader(text.trim())));
        } catch (org.xml.sax.SAXException e) {
            throw new IllegalArgumentException("transform \"" + step + "\" could not parse " + what + " as XML: " + e.getMessage());
        }
    }

    /** Java twin of apppage.js#xslToCsvRows: the stylesheet's text output, read as CSV with a header line. */
    static List<Map<String, String>> xslToCsvRows(Object data, String stylesheet, String step) throws Exception {
        Document xml = parseXml(data, step, "the response");
        Document xsl = parseXml(stylesheet, step, "the stylesheet");
        TransformerFactory factory = TransformerFactory.newInstance();
        // The stylesheet is page-authored: no document(), imports or DTDs reaching off the box.
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        StringWriter csv = new StringWriter();
        try {
            factory.newTransformer(new DOMSource(xsl)).transform(new DOMSource(xml), new StreamResult(csv));
        } catch (TransformerException e) {
            throw new IllegalArgumentException("XSL \"" + step + "\" failed: " + e.getMessage());
        }
        return csvRows(csv.toString());
    }

    /** CSV text as rows keyed by the first line: quoted fields may hold commas, doubled quotes and line breaks; blank lines are skipped. */
    static List<Map<String, String>> csvRows(String text) {
        List<List<String>> lines = new ArrayList<>();
        List<String> line = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quoted) {
                if (c != '"') field.append(c);
                else if (i + 1 < text.length() && text.charAt(i + 1) == '"') { field.append('"'); i++; }
                else quoted = false;
            } else if (c == '"') quoted = true;
            else if (c == ',') { line.add(field.toString()); field.setLength(0); }
            else if (c == '\n' || c == '\r') {
                if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') i++;
                line.add(field.toString()); field.setLength(0);
                if (line.size() > 1 || !line.get(0).isBlank()) lines.add(line);
                line = new ArrayList<>();
            } else field.append(c);
        }
        line.add(field.toString());
        if (line.size() > 1 || !line.get(0).isBlank()) lines.add(line);

        List<Map<String, String>> rows = new ArrayList<>();
        if (lines.isEmpty()) return rows;
        List<String> header = lines.get(0).stream().map(String::trim).toList();
        for (List<String> values : lines.subList(1, lines.size())) {
            Map<String, String> row = new LinkedHashMap<>();
            for (int c = 0; c < header.size(); c++) row.put(header.get(c), c < values.size() ? values.get(c) : "");
            rows.add(row);
        }
        return rows;
    }

    /** Java twin of apppage.js#xmlNodeToObj. */
    private static Object nodeToObj(Element node) {
        Map<String, Object> obj = new LinkedHashMap<>();
        NamedNodeMap attributes = node.getAttributes();
        if (attributes.getLength() > 0) {
            Map<String, Object> attrs = new LinkedHashMap<>();
            for (int i = 0; i < attributes.getLength(); i++)
                attrs.put(attributes.item(i).getNodeName(), attributes.item(i).getNodeValue());
            obj.put("@attributes", attrs);
        }
        for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
                String text = child.getNodeValue().trim();
                if (!text.isEmpty()) obj.merge("#text", text, (a, b) -> a.toString() + b);
            } else if (child.getNodeType() == Node.ELEMENT_NODE) {
                String key = ((Element) child).getTagName();
                Object value = nodeToObj((Element) child);
                Object was = obj.get(key);
                if (was == null) obj.put(key, value);
                else if (was instanceof List<?>) castList(was).add(value); // nodeToObj never returns a list, so this one is ours
                else obj.put(key, new ArrayList<>(List.of(was, value)));
            }
        }
        return obj.size() == 1 && obj.containsKey("#text") ? obj.get("#text") : obj;
    }

    // ── One target ───────────────────────────────────────────────────────────

    private Object target(Output o, Object root, AppUseCaseInstanceOutput out, Map<String, Object> sent,
                          Map<String, Object> scope, Set<String> notes, String key) {
        String path = resolveBraced(o.path(), scope, "the path");
        Object found = pathGet(root, path);
        if (!o.binding().rows()) return found;
        List<Object> rows = rowsAt(found, o, path);
        if (o.binding().keyValueGrid() && found instanceof Map<?, ?>) {
            // Key/value grid: enriched columns become further key/value rows, read off the object (apppage.js#enrichBound).
            Object enriched = enrich(new ArrayList<>(List.of(found)), o.binding().enrichColumns(), out, sent, notes, key).get(0);
            for (Map.Entry<?, ?> e : ((Map<?, ?>) enriched).entrySet()) {
                if (((Map<?, ?>) found).containsKey(e.getKey())) continue;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("key", e.getKey());
                row.put("value", isScalar(e.getValue()) ? e.getValue() : json(e.getValue()));
                rows.add(row);
            }
        } else {
            rows = enrich(rows, o.binding().enrichColumns(), out, sent, notes, key);
        }
        rows = keep(rows, o.binding().bindFilters(), scope);
        if (o.binding().pivoted()) notes.add(key + ": the page groups these rows (pivot); they are returned ungrouped.");
        return rows;
    }

    /** Java twin of apppage.js#rowsForGridAction. */
    private static List<Object> rowsAt(Object found, Output o, String path) {
        if (found == null) throw new IllegalArgumentException("nothing found at path \"" + path + "\".");
        if (found instanceof List<?> list) return new ArrayList<>(list);
        if (found instanceof Map<?, ?> map) {
            if (o.binding().keyValueGrid()) {
                List<Object> rows = new ArrayList<>();
                map.forEach((k, v) -> {
                    if (o.binding().scalarsOnly() && !isScalar(v)) return;
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("key", k);
                    row.put("value", isScalar(v) ? v : json(v));
                    rows.add(row);
                });
                return rows;
            }
            if (AppPageAction.METADATA.equals(o.source())) {
                Map<Object, Object> row = new LinkedHashMap<>(map);
                if (o.binding().scalarsOnly()) row.values().removeIf(v -> !isScalar(v));
                return new ArrayList<>(List.of(row));
            }
        }
        throw new IllegalArgumentException("expected an array" + (path.isEmpty() ? "" : " at \"" + path + "\"")
                + ", got " + kindOf(found) + ".");
    }

    /** Java twin of apppage.js#pathGet: dots or slashes, [n], [field="x"] / [field!="x"], names mapped over arrays. */
    static Object pathGet(Object data, String path) {
        if (path == null || path.isEmpty()) return data;
        Matcher m = TOKEN.matcher(path.replaceFirst("^\\$\\.?", ""));
        Object current = data;
        while (m.find()) {
            if (current == null) return null;
            current = m.group(1) != null ? nameStep(current, m.group(1)) : filterStep(current, m.group(2).trim());
        }
        return current;
    }

    private static Object nameStep(Object current, String key) {
        if (!(current instanceof List<?> list) || key.matches("\\d+")) return field(current, key);
        List<Object> out = new ArrayList<>();
        boolean flattened = false;
        for (Object item : list) {
            Object value = field(item, key);
            if (value == null) continue;
            if (value instanceof List<?> l) { flattened = true; out.addAll(l); }
            else out.add(value);
        }
        return out.size() == 1 && !flattened ? out.get(0) : out;
    }

    private static Object filterStep(Object current, String inside) {
        if (inside.isEmpty()) return current;
        if (inside.matches("\\d+")) return field(current, inside);
        Matcher parts = FILTER.matcher(inside);
        if (!parts.matches()) return null;
        String key = parts.group(1).trim();
        boolean negate = "!=".equals(parts.group(2));
        String wanted = parts.group(3).trim().replaceFirst("^([\"'])([\\s\\S]*)\\1$", "$2");
        List<?> list = current instanceof List<?> l ? l : Collections.singletonList(current);
        return list.stream().filter(item -> {
            Object value = item == null ? null : pathGet(item, key);
            return negate != String.valueOf(value == null ? "" : value).equals(wanted);
        }).toList();
    }

    private static Object field(Object item, String key) {
        if (item instanceof Map<?, ?> map) return map.get(key);
        if (item instanceof List<?> list && key.matches("\\d+")) {
            int i = Integer.parseInt(key);
            return i < list.size() ? list.get(i) : null;
        }
        return null;
    }

    // ── Enriched columns ─────────────────────────────────────────────────────

    /** Java twin of apppage.js#enrichRows, copying rows rather than writing into the parsed response. */
    private List<Object> enrich(List<Object> rows, List<AppPageEnrichColumn> all, AppUseCaseInstanceOutput out,
                                Map<String, Object> sent, Set<String> notes, String key) {
        List<AppPageEnrichColumn> columns = all.stream()
                .filter(c -> c != null && (!isBlank(c.name()) || c.bringsWholeRow())).toList();
        if (columns.isEmpty()) return rows;
        Map<String, Object> record = record(out);
        Map<AppPageEnrichColumn, Map<String, Map<?, ?>>> lookups = new HashMap<>();
        for (AppPageEnrichColumn c : columns) {
            String said = key + " column \"" + (isBlank(c.name()) ? nullToEmpty(c.prefix()) + "*" : c.name()) + "\": ";
            if (AppPageEnrichColumn.GRID_KINDS.contains(c.kindOrDefault())) {
                notes.add(said + "looks into another grid on the page, which a tool call has not got; left out.");
            } else if (AppPageEnrichColumn.VLOOKUP.equals(c.kindOrDefault())) {
                lookups.put(c, lookupIndex(c, said, notes));
            }
        }
        List<Object> enriched = new ArrayList<>(rows.size());
        for (Object row : rows) {
            Map<String, Object> copy = new LinkedHashMap<>();
            if (row instanceof Map<?, ?> m) m.forEach((k, v) -> copy.put(String.valueOf(k), v));
            else copy.put("value", row);
            for (AppPageEnrichColumn c : columns) {
                if (AppPageEnrichColumn.GRID_KINDS.contains(c.kindOrDefault())) continue;
                copy.put(c.name(), cell(c, copy, out, sent, record, lookups.get(c)));
            }
            enriched.add(copy);
        }
        return enriched;
    }

    private Map<String, Map<?, ?>> lookupIndex(AppPageEnrichColumn c, String said, Set<String> notes) {
        if (isBlank(c.datasetName()) || isBlank(c.keyColumn()) || isBlank(c.returnColumn()) || isBlank(c.lookupColumn())) {
            notes.add(said + "the lookup needs a grid column, a dataset, its key column and a column to return.");
            return null;
        }
        StaticDatasetService.DatasetState state = datasets.getState(c.datasetName());
        if (state == null || state.rows() == null) {
            notes.add(said + "static dataset \"" + c.datasetName() + "\" is not loaded.");
            return null;
        }
        Map<String, Map<?, ?>> index = new HashMap<>();
        for (Map<String, Object> row : state.rows()) {
            if (row == null) continue;
            String k = lookupKey(row.get(scopeKey(row, c.keyColumn())));
            if (!k.isEmpty()) index.putIfAbsent(k, row);
        }
        return index;
    }

    /** Java twin of apppage.js#enrichedCell. */
    private static Object cell(AppPageEnrichColumn c, Map<String, Object> row, AppUseCaseInstanceOutput out,
                               Map<String, Object> sent, Map<String, Object> record, Map<String, Map<?, ?>> index) {
        String expression = nullToEmpty(c.expression());
        switch (c.kindOrDefault()) {
            case AppPageEnrichColumn.HEADER: return header(out, expression);
            case AppPageEnrichColumn.REGEX: return regexCell(c, row);
            case AppPageEnrichColumn.VARIABLE: {
                Map<String, Object> saved = out.appUseCaseInstanceInputs() == null ? Map.of() : out.appUseCaseInstanceInputs();
                Object value = sent.containsKey(expression) ? sent.get(expression) : saved.get(expression);
                return value == null ? "" : isScalar(value) ? value : json(value);
            }
            case AppPageEnrichColumn.VLOOKUP: {
                if (index == null) return "";
                Map<?, ?> found = index.get(lookupKey(row.get(scopeKey(row, c.lookupColumn()))));
                return found == null ? "" : orEmpty(found.get(scopeKey(found, c.returnColumn())));
            }
            default: return orEmpty(record.get(expression));
        }
    }

    private static String header(AppUseCaseInstanceOutput out, String wanted) {
        if (out.responseHeaders() == null || wanted.isEmpty()) return "";
        return out.responseHeaders().entrySet().stream().filter(e -> wanted.equalsIgnoreCase(e.getKey()))
                .findFirst().map(e -> e.getValue() == null ? "" : String.join(", ", e.getValue())).orElse("");
    }

    /** Java twin of apppage.js#regexCell: {@code /pattern/flags}, {@code g} for every match, {@code $1 $& $$} in the replacement. */
    static String regexCell(AppPageEnrichColumn c, Map<String, Object> row) {
        Object text = row.get(scopeKey(row, c.lookupColumn()));
        if (text == null || "".equals(text) || isBlank(c.expression())) return "";
        String source = c.expression().trim();
        String flags = "";
        Matcher slashed = SLASHED.matcher(source);
        if (slashed.matches()) { source = slashed.group(1); flags = slashed.group(2); }
        Pattern pattern;
        try {
            // shortcut: JavaScript regex syntax read by java.util.regex; the common subset agrees, lookbehind and named groups may not.
            pattern = Pattern.compile(source, (flags.contains("i") ? Pattern.CASE_INSENSITIVE : 0)
                    | (flags.contains("s") ? Pattern.DOTALL : 0) | (flags.contains("m") ? Pattern.MULTILINE : 0));
        } catch (Exception e) {
            return "";
        }
        Matcher m = pattern.matcher(String.valueOf(text));
        if (!flags.contains("g")) return m.find() ? expand(m, c.replacement()) : "";
        List<String> all = new ArrayList<>();
        while (m.find()) all.add(expand(m, c.replacement()));
        return String.join(", ", all);
    }

    private static String expand(Matcher match, String replacement) {
        if (isBlank(replacement)) return nullToEmpty(match.groupCount() > 0 ? match.group(1) : match.group());
        Matcher ref = GROUP_REF.matcher(replacement);
        StringBuilder sb = new StringBuilder();
        while (ref.find()) {
            String text = "$$".equals(ref.group()) ? "$" : "$&".equals(ref.group()) ? match.group()
                    : Integer.parseInt(ref.group(1)) <= match.groupCount() ? nullToEmpty(match.group(Integer.parseInt(ref.group(1)))) : "";
            ref.appendReplacement(sb, Matcher.quoteReplacement(text));
        }
        return ref.appendTail(sb).toString();
    }

    // ── Bind filters ─────────────────────────────────────────────────────────

    /** Java twin of apppage.js#keptForBinding; a filter whose value resolves to nothing means "any". */
    static List<Object> keep(List<Object> rows, List<AppPageRowFilter> list, Map<String, Object> scope) {
        List<AppPageRowFilter> filters = list.stream()
                .map(f -> new AppPageRowFilter(nullToEmpty(f.column()).trim(), f.operatorOrDefault(),
                        AppPageMcpTools.resolveTemplate(f.value(), scope).trim()))
                .filter(f -> !f.column().isEmpty() && (AppPageRowFilter.VALUELESS.contains(f.operator()) || !f.value().isEmpty()))
                .toList();
        if (filters.isEmpty()) return rows;
        return rows.stream().filter(row -> filters.stream().allMatch(f -> passes(cellText(row, f.column()), f))).toList();
    }

    private static boolean passes(String text, AppPageRowFilter f) {
        String lower = text.toLowerCase(Locale.ROOT), wanted = f.value().toLowerCase(Locale.ROOT);
        return switch (f.operator()) {
            case "NOT_CONTAINS" -> !lower.contains(wanted);
            case "EQUALS" -> lower.equals(wanted);
            case "NOT_EQUALS" -> !lower.equals(wanted);
            case "STARTS_WITH" -> lower.startsWith(wanted);
            case "ENDS_WITH" -> lower.endsWith(wanted);
            case "GT" -> compare(text, f.value()) > 0;
            case "LT" -> compare(text, f.value()) < 0;
            case "EMPTY" -> text.isBlank();
            case "NOT_EMPTY" -> !text.isBlank();
            case "REGEX" -> {
                try { yield Pattern.compile(f.value()).matcher(text).find(); } catch (Exception e) { yield false; }
            }
            default -> lower.contains(wanted);
        };
    }

    /** Numbers as numbers, else text ignoring case. */
    private static int compare(String a, String b) {
        try {
            return Double.compare(Double.parseDouble(a), Double.parseDouble(b));
        } catch (NumberFormatException e) {
            // shortcut: the page's localeCompare also orders digit runs numerically ("a2" < "a10"); this does not.
            return String.CASE_INSENSITIVE_ORDER.compare(a, b);
        }
    }

    private static String cellText(Object row, String column) {
        Object cell = row instanceof Map<?, ?> m ? m.get(scopeKey(m, column)) : row;
        return cell == null ? "" : isScalar(cell) ? String.valueOf(cell) : json(cell);
    }

    // ── Shared bits ──────────────────────────────────────────────────────────

    /** Java twin of apppage.js#metaOf — the call as a flat record, what a METADATA binding and META column read. */
    static Map<String, Object> record(AppUseCaseInstanceOutput out) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("executionId", out.executionId());
        r.put("status", out.status());
        r.put("statusCode", out.statusCode());
        r.put("httpMethod", out.httpMethod());
        r.put("url", out.url());
        r.put("timeTaken", out.timeTaken());
        r.put("appName", out.appName());
        r.put("environment", out.environment());
        r.put("envClass", out.envClass());
        r.put("useCase", out.useCase());
        r.put("instanceLabel", out.instanceLabel());
        r.put("appUseCaseInstanceId", out.appUseCaseInstanceId());
        r.put("requestSize", out.requestBodySize() != null ? out.requestBodySize() : nullToEmpty(out.requestBody()).length());
        r.put("responseSize", out.responseBodySize() != null ? out.responseBodySize() : nullToEmpty(out.responseBody()).length());
        r.put("executedVia", out.executedVia());
        r.put("startedAt", out.startedAt());
        r.put("startedAtText", out.startedAt() > 0
                ? STARTED.format(Instant.ofEpochMilli(out.startedAt()).atZone(ZoneId.systemDefault())) : "");
        r.put("runIndex", out.runIndex());
        r.put("runCount", out.runCount());
        r.put("unresolvedVariables", out.unresolvedVariables() == null ? "" : String.join(", ", out.unresolvedVariables()));
        r.put("error", nullToEmpty(out.error()));
        return r;
    }

    /**
     * Java twin of apppage.js#resolvePath / #resolveExpression: only {@code ${name}}, since a bare
     * {@code $} is path and JSONata syntax. A missing value throws — an empty filter matches nothing silently.
     */
    static String resolveBraced(String text, Map<String, Object> scope, String what) {
        if (text == null || text.isEmpty()) return "";
        Matcher m = BRACED.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            Object value = scope.get(m.group(1));
            if (value == null || "".equals(value)) {
                throw new IllegalArgumentException(what + " needs " + m.group() + ", and \"" + m.group(1)
                        + "\" has no value — pass it as an argument.");
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(value)));
        }
        return m.appendTail(sb).toString();
    }

    /** A row's key for a name: exact first, else the first that differs only in case. */
    private static Object scopeKey(Map<?, ?> row, String name) {
        if (name == null || row.containsKey(name)) return name;
        String lower = name.trim().toLowerCase(Locale.ROOT);
        return row.keySet().stream().<Object>map(k -> k).filter(k -> String.valueOf(k).toLowerCase(Locale.ROOT).equals(lower))
                .findFirst().orElse(name);
    }

    private static String lookupKey(Object value) {
        return value == null ? "" : String.valueOf(value).trim().toLowerCase(Locale.ROOT);
    }

    private static boolean isScalar(Object v) {
        return !(v instanceof Map) && !(v instanceof List);
    }

    private static String kindOf(Object v) {
        return v instanceof List ? "an array" : v instanceof Map ? "an object" : v instanceof String ? "a string"
                : v instanceof Number ? "a number" : v instanceof Boolean ? "a boolean" : "nothing";
    }

    private static String json(Object v) {
        try { return JSON.writeValueAsString(v); } catch (Exception e) { return String.valueOf(v); }
    }

    @SuppressWarnings("unchecked")
    private static List<Object> castList(Object list) { return (List<Object>) list; }

    private static Object orEmpty(Object v) { return v == null ? "" : v; }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
