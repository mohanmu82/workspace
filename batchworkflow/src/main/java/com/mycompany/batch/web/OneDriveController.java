package com.mycompany.batch.web;

import com.mycompany.batch.onedrive.OneDriveClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Direct, uncached access to work OneDrive — browse a folder to find a workbook's path, or read
 * one sheet as a JSON array of row objects. To reuse the rows elsewhere, save them as a
 * {@code source=onedrive} static dataset instead.
 */
@RestController
@RequestMapping("/onedrive")
public class OneDriveController {

    private final OneDriveClient client;

    public OneDriveController(OneDriveClient client) {
        this.client = client;
    }

    /** {@code GET /onedrive/files?path=/Reports} (optionally {@code jane@mycompany.com:/Reports}). */
    @GetMapping("/files")
    public ResponseEntity<?> files(@RequestParam(defaultValue = "/") String path) {
        try {
            List<OneDriveClient.DriveItem> items = client.listFolder(path);
            return ResponseEntity.ok(items);
        } catch (Exception e) {
            return badRequest(e);
        }
    }

    /** {@code GET /onedrive/excel?path=/Reports/servers.xlsx&sheet=Prod} — the sheet's rows as an array. */
    @GetMapping("/excel")
    public ResponseEntity<?> excel(@RequestParam String path, @RequestParam(required = false) String sheet) {
        try {
            List<Map<String, Object>> rows = client.readExcelRows(path, sheet);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("path", path);
            response.put("sheet", sheet);
            response.put("count", rows.size());
            response.put("rows", rows);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return badRequest(e);
        }
    }

    private ResponseEntity<Map<String, Object>> badRequest(Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        return ResponseEntity.badRequest().body(Map.of("error", msg));
    }
}
