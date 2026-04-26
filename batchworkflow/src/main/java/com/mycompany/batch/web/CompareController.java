package com.mycompany.batch.web;

import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.service.BatchService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/batch")
public class CompareController {

    private final BatchService     batchService;
    private final BatchController  batchController;

    public CompareController(BatchService batchService, BatchController batchController) {
        this.batchService    = batchService;
        this.batchController = batchController;
    }

    // -------------------------------------------------------------------------
    // POST /batch/comparedatasets
    // -------------------------------------------------------------------------

    @PostMapping(value = "/comparedatasets", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> compareDatasets(@RequestBody Map<String, Object> body) {
        String key1 = str(body.get("key1")).trim();
        String key2 = str(body.get("key2")).trim();
        if (key2.isBlank()) key2 = key1;

        if (!body.containsKey("dataset1")) return badRequest("dataset1 is required");
        if (!body.containsKey("dataset2")) return badRequest("dataset2 is required");
        if (key1.isBlank())                return badRequest("key1 is required");

        RunRequest req1, req2;
        try {
            req1 = batchController.deserializeRunRequest(castMap(body.get("dataset1")));
            req2 = batchController.deserializeRunRequest(castMap(body.get("dataset2")));
        } catch (Exception e) {
            return badRequest("Invalid dataset request: " + e.getMessage());
        }

        BatchService.BatchResult result1;
        try {
            result1 = batchService.run(req1);
        } catch (Exception e) {
            return badRequest("Dataset 1 failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }

        BatchService.BatchResult result2;
        try {
            result2 = batchService.run(req2);
        } catch (Exception e) {
            return badRequest("Dataset 2 failed: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }

        List<Map<String, Object>> data1 = result1.results();
        List<Map<String, Object>> data2 = result2.results();

        // Validate key presence
        if (!data1.isEmpty()) {
            boolean found = data1.stream().limit(10).anyMatch(r -> r.containsKey(key1));
            if (!found) {
                Set<String> cols = data1.get(0).keySet();
                return badRequest("Key '" + key1 + "' not found in Dataset 1. Available columns: " + cols);
            }
        }
        final String finalKey2 = key2;
        if (!data2.isEmpty()) {
            boolean found = data2.stream().limit(10).anyMatch(r -> r.containsKey(finalKey2));
            if (!found) {
                Set<String> cols = data2.get(0).keySet();
                return badRequest("Key '" + finalKey2 + "' not found in Dataset 2. Available columns: " + cols);
            }
        }

        Map<String, Object> ds1Stats = new LinkedHashMap<>();
        ds1Stats.put("rowCount",    data1.size());
        ds1Stats.put("timeTakenMs", result1.timeTakenMs());
        ds1Stats.put("processed",   result1.processed());
        ds1Stats.put("succeeded",   result1.succeeded());
        ds1Stats.put("failed",      result1.failed());

        Map<String, Object> ds2Stats = new LinkedHashMap<>();
        ds2Stats.put("rowCount",    data2.size());
        ds2Stats.put("timeTakenMs", result2.timeTakenMs());
        ds2Stats.put("processed",   result2.processed());
        ds2Stats.put("succeeded",   result2.succeeded());
        ds2Stats.put("failed",      result2.failed());

        return ResponseEntity.ok(compare(data1, data2, key1, key2, ds1Stats, ds2Stats));
    }

    // -------------------------------------------------------------------------
    // Comparison logic
    // -------------------------------------------------------------------------

    private Map<String, Object> compare(
            List<Map<String, Object>> data1,
            List<Map<String, Object>> data2,
            String key1, String key2,
            Map<String, Object> ds1Stats,
            Map<String, Object> ds2Stats) {

        Map<String, Map<String, Object>> map1 = indexBy(data1, key1);
        Map<String, Map<String, Object>> map2 = indexBy(data2, key2);

        Set<String> cols1 = columnSet(data1, key1);
        Set<String> cols2 = columnSet(data2, key2);

        List<String> commonCols = cols1.stream().filter(cols2::contains).toList();
        List<String> only1Cols  = cols1.stream().filter(c -> !cols2.contains(c)).toList();
        List<String> only2Cols  = cols2.stream().filter(c -> !cols1.contains(c)).toList();

        // Per-column stats: int[]{matches, total}
        Map<String, int[]> colStats = new LinkedHashMap<>();
        commonCols.forEach(c -> colStats.put(c, new int[]{0, 0}));

        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(map1.keySet());
        allKeys.addAll(map2.keySet());

        List<Map<String, Object>> rows = new ArrayList<>();
        int cntSame = 0, cntMismatch = 0, cntDs1Only = 0, cntDs2Only = 0;

        for (String k : allKeys) {
            Map<String, Object> r1 = map1.get(k);
            Map<String, Object> r2 = map2.get(k);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("__compareKey", k);

            if (r1 != null && r2 == null) {
                row.put("compareStatus", "DATASET1_ONLY");
                commonCols.forEach(c -> row.put(c, r1.get(c)));
                only1Cols .forEach(c -> row.put(c, r1.get(c)));
                cntDs1Only++;
            } else if (r1 == null) {
                row.put("compareStatus", "DATASET2_ONLY");
                commonCols.forEach(c -> row.put(c, r2.get(c)));
                only2Cols .forEach(c -> row.put(c, r2.get(c)));
                cntDs2Only++;
            } else {
                List<String> mismatched = new ArrayList<>();
                for (String c : commonCols) {
                    String v1 = toStr(r1.get(c));
                    String v2 = toStr(r2.get(c));
                    row.put(c, r1.get(c));
                    colStats.get(c)[1]++;
                    if (v1.equals(v2)) {
                        colStats.get(c)[0]++;
                    } else {
                        mismatched.add(c);
                        row.put("__ds2_" + c, r2.get(c));
                    }
                }
                only1Cols.forEach(c -> row.put(c, r1.get(c)));
                only2Cols.forEach(c -> row.put(c, r2.get(c)));
                if (mismatched.isEmpty()) {
                    row.put("compareStatus", "SAME");
                    cntSame++;
                } else {
                    row.put("compareStatus", "MISMATCH");
                    row.put("__mismatchedColumns", mismatched);
                    cntMismatch++;
                }
            }
            rows.add(row);
        }

        // Per-column match % and mismatch count
        Map<String, Integer> matchPct      = new LinkedHashMap<>();
        Map<String, Integer> mismatchCount = new LinkedHashMap<>();
        colStats.forEach((c, s) -> {
            matchPct.put(c,      s[1] > 0 ? (int) Math.round(s[0] * 100.0 / s[1]) : 100);
            mismatchCount.put(c, s[1] - s[0]);
        });

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("same",         cntSame);
        summary.put("mismatch",     cntMismatch);
        summary.put("dataset1Only", cntDs1Only);
        summary.put("dataset2Only", cntDs2Only);
        summary.put("total",        rows.size());

        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("common",       commonCols);
        columns.put("dataset1Only", only1Cols);
        columns.put("dataset2Only", only2Cols);
        columns.put("matchPct",     matchPct);
        columns.put("mismatchCount",mismatchCount);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("dataset1", ds1Stats);
        metadata.put("dataset2", ds2Stats);
        metadata.put("summary",  summary);
        metadata.put("columns",  columns);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata", metadata);
        response.put("data",     rows);
        return response;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Map<String, Map<String, Object>> indexBy(List<Map<String, Object>> rows, String key) {
        Map<String, Map<String, Object>> index = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            Object k = r.get(key);
            if (k != null) index.put(String.valueOf(k), r);
        }
        return index;
    }

    private static Set<String> columnSet(List<Map<String, Object>> rows, String excludeKey) {
        Set<String> cols = new LinkedHashSet<>();
        rows.forEach(r -> r.keySet().stream().filter(c -> !c.equals(excludeKey)).forEach(cols::add));
        return cols;
    }

    private static String toStr(Object v) { return v == null ? "" : String.valueOf(v); }
    private static String str(Object v)   { return v == null ? "" : String.valueOf(v); }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object o) {
        if (o instanceof Map<?, ?> m) return (Map<String, Object>) m;
        throw new IllegalArgumentException("Expected a JSON object");
    }

    private ResponseEntity<Map<String, Object>> badRequest(String message) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("error", message);
        return ResponseEntity.badRequest().body(err);
    }
}
