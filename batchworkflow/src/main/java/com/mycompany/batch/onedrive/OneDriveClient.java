package com.mycompany.batch.onedrive;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.config.OneDriveProperties;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads files from work/school OneDrive via Microsoft Graph, authenticating as the app
 * registration in {@link OneDriveProperties} (client credentials — no interactive sign-in).
 *
 * <p>A location is a drive-relative path, optionally prefixed with the owner's UPN:
 * {@code /Reports/servers.xlsx} reads {@code onedrive.default-user}'s drive, while
 * {@code jane@mycompany.com:/Reports/servers.xlsx} reads Jane's.
 */
@Component
public class OneDriveClient {

    private static final String GRAPH = "https://graph.microsoft.com/v1.0";

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            // The /content endpoint answers with a 302 to a pre-authenticated download URL.
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final OneDriveProperties props;
    private final ObjectMapper objectMapper;

    private String  accessToken;
    private Instant tokenExpiry = Instant.EPOCH;

    public OneDriveClient(OneDriveProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    /** A drive item as returned by {@link #listFolder}. */
    public record DriveItem(String name, String path, boolean folder, long size, String lastModified, String webUrl) {}

    /**
     * Rows of one sheet of an Excel workbook (.xlsx or .xls) — the first non-blank row supplies
     * the attribute names, every later non-blank row becomes one map. Cell values are the text
     * Excel displays (formulas evaluated, dates and numbers formatted), matching the string
     * values the file and paste sources produce.
     *
     * @param sheetName sheet to read; blank reads the first sheet
     */
    public List<Map<String, Object>> readExcelRows(String location, String sheetName) throws Exception {
        byte[] bytes = download(location);
        try (Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes))) {
            Sheet sheet;
            if (sheetName == null || sheetName.isBlank()) {
                sheet = wb.getSheetAt(0);
            } else {
                sheet = wb.getSheet(sheetName.trim());
                if (sheet == null) {
                    List<String> names = new ArrayList<>();
                    wb.sheetIterator().forEachRemaining(s -> names.add(s.getSheetName()));
                    throw new IllegalArgumentException(
                            "Sheet '" + sheetName.trim() + "' not found in " + location + " — available: " + names);
                }
            }
            return sheetToRows(sheet, wb.getCreationHelper().createFormulaEvaluator());
        }
    }

    /** Raw bytes of the file at {@code location}. */
    public byte[] download(String location) throws Exception {
        Target t = parseLocation(location);
        String url = GRAPH + "/users/" + encode(t.user()) + "/drive/root:" + encodePath(t.path()) + ":/content";
        HttpResponse<byte[]> resp = HTTP.send(authorized(url).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() >= 400) {
            throw new RuntimeException("OneDrive " + resp.statusCode() + " reading " + location + ": "
                    + graphError(new String(resp.body(), StandardCharsets.UTF_8)));
        }
        return resp.body();
    }

    /** Immediate children of a folder — {@code location} names the folder, e.g. {@code /Reports} or {@code /}. */
    public List<DriveItem> listFolder(String location) throws Exception {
        Target t = parseLocation(location == null || location.isBlank() ? "/" : location);
        String folder = t.path().replaceAll("/+$", "");
        String base = GRAPH + "/users/" + encode(t.user()) + "/drive/root"
                + (folder.isEmpty() ? "" : ":" + encodePath(folder) + ":");
        String url = base + "/children?$top=999&$select=name,size,lastModifiedDateTime,webUrl,folder,file";

        List<DriveItem> items = new ArrayList<>();
        while (url != null) {
            HttpResponse<String> resp = HTTP.send(authorized(url).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 400) {
                throw new RuntimeException("OneDrive " + resp.statusCode() + " listing " + location + ": "
                        + graphError(resp.body()));
            }
            JsonNode body = objectMapper.readTree(resp.body());
            for (JsonNode n : body.path("value")) {
                String name = n.path("name").asText();
                items.add(new DriveItem(
                        name,
                        folder + "/" + name,
                        n.has("folder"),
                        n.path("size").asLong(),
                        n.path("lastModifiedDateTime").asText(null),
                        n.path("webUrl").asText(null)));
            }
            url = body.hasNonNull("@odata.nextLink") ? body.get("@odata.nextLink").asText() : null;
        }
        return items;
    }

    // -------------------------------------------------------------------------
    // Excel parsing
    // -------------------------------------------------------------------------

    private List<Map<String, Object>> sheetToRows(Sheet sheet, FormulaEvaluator evaluator) {
        DataFormatter fmt = new DataFormatter();
        List<String> headers = null;
        List<Map<String, Object>> rows = new ArrayList<>();

        for (Row row : sheet) {
            List<String> values = new ArrayList<>();
            short last = row.getLastCellNum();
            for (int c = 0; c < Math.max(last, 0); c++) {
                Cell cell = row.getCell(c, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                values.add(cell == null ? "" : fmt.formatCellValue(cell, evaluator).trim());
            }
            if (values.stream().allMatch(String::isEmpty)) continue;

            if (headers == null) {
                headers = uniqueHeaders(values);
                continue;
            }
            Map<String, Object> m = new LinkedHashMap<>();
            for (int c = 0; c < headers.size(); c++) {
                m.put(headers.get(c), c < values.size() ? values.get(c) : "");
            }
            rows.add(m);
        }
        return rows;
    }

    /** Blank header cells become {@code Column N}; repeated names get a {@code _2}, {@code _3}… suffix. */
    private List<String> uniqueHeaders(List<String> raw) {
        List<String> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < raw.size(); i++) {
            String base = raw.get(i).isEmpty() ? "Column " + (i + 1) : raw.get(i);
            String name = base;
            for (int n = 2; !seen.add(name); n++) name = base + "_" + n;
            out.add(name);
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Graph plumbing
    // -------------------------------------------------------------------------

    private record Target(String user, String path) {}

    private Target parseLocation(String location) {
        if (location == null || location.isBlank()) throw new IllegalArgumentException("OneDrive location is required");
        String loc = location.trim().replace('\\', '/');
        String user = props.getDefaultUser();
        int sep = loc.indexOf(":/");
        if (sep > 0 && loc.substring(0, sep).contains("@")) {
            user = loc.substring(0, sep).trim();
            loc = loc.substring(sep + 1);
        }
        if (user.isBlank()) {
            throw new IllegalArgumentException("No OneDrive user — prefix the location with a UPN "
                    + "(jane@mycompany.com:/path/file.xlsx) or set onedrive.default-user");
        }
        if (!loc.startsWith("/")) loc = "/" + loc;
        return new Target(user, loc);
    }

    private HttpRequest.Builder authorized(String url) throws Exception {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(props.getTimeoutMs()))
                .header("Authorization", "Bearer " + token());
    }

    /** App-only Graph token, cached until a minute before it expires. */
    private synchronized String token() throws Exception {
        if (accessToken != null && Instant.now().isBefore(tokenExpiry)) return accessToken;
        if (!props.isConfigured()) {
            throw new IllegalStateException(
                    "OneDrive is not configured — set onedrive.tenant-id, onedrive.client-id and onedrive.client-secret");
        }
        String form = "grant_type=client_credentials"
                + "&client_id=" + encode(props.getClientId())
                + "&client_secret=" + encode(props.getClientSecret())
                + "&scope=" + encode("https://graph.microsoft.com/.default");
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("https://login.microsoftonline.com/" + encode(props.getTenantId()) + "/oauth2/v2.0/token"))
                .timeout(Duration.ofMillis(props.getTimeoutMs()))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        JsonNode body = objectMapper.readTree(resp.body());
        if (resp.statusCode() >= 400 || !body.hasNonNull("access_token")) {
            throw new RuntimeException("OneDrive token request failed (" + resp.statusCode() + "): "
                    + body.path("error_description").asText(body.path("error").asText(resp.body())));
        }
        accessToken = body.get("access_token").asText();
        tokenExpiry = Instant.now().plusSeconds(Math.max(body.path("expires_in").asLong(3600) - 60, 0));
        return accessToken;
    }

    private String graphError(String body) {
        try {
            JsonNode err = objectMapper.readTree(body).path("error");
            if (err.hasNonNull("message")) return err.path("code").asText("") + " " + err.get("message").asText();
        } catch (Exception ignored) { }
        return body;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String encodePath(String path) {
        return Arrays.stream(path.split("/", -1)).map(OneDriveClient::encode).collect(Collectors.joining("/"));
    }
}
