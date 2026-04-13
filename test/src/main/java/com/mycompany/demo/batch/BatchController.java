package com.mycompany.demo.batch;

import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@Validated
@RestController
@RequestMapping("/batch")
public class BatchController {

    private final BatchService batchService;

    public BatchController(BatchService batchService) {
        this.batchService = batchService;
    }

    @GetMapping("/run")
    public ResponseEntity<Map<String, Object>> runGet(
            @RequestParam @NotBlank(message = "filePath must not be blank") String filePath) throws Exception {
        return run(filePath);
    }

    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runPost(
            @RequestParam @NotBlank(message = "filePath must not be blank") String filePath) throws Exception {
        return run(filePath);
    }

    @GetMapping("/run/psv")
    public ResponseEntity<Map<String, Object>> runPsvGet(
            @RequestParam @NotBlank(message = "filePath must not be blank") String filePath) throws Exception {
        return runPsv(filePath);
    }

    @PostMapping("/run/psv")
    public ResponseEntity<Map<String, Object>> runPsvPost(
            @RequestParam @NotBlank(message = "filePath must not be blank") String filePath) throws Exception {
        return runPsv(filePath);
    }

    private ResponseEntity<Map<String, Object>> run(String filePath) throws Exception {
        BatchService.BatchResult result = batchService.run(filePath);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("processed", result.processed());
        metadata.put("succeeded", result.succeeded());
        metadata.put("failed", result.failed());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata", metadata);
        response.put("data", result.results());
        return ResponseEntity.ok(response);
    }

    private ResponseEntity<Map<String, Object>> runPsv(String filePath) throws Exception {
        BatchService.PsvResult result = batchService.runToPsv(filePath);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("processed", result.processed());
        metadata.put("succeeded", result.succeeded());
        metadata.put("failed", result.failed());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("metadata", metadata);
        response.put("outputFile", result.outputFile());
        return ResponseEntity.ok(response);
    }
}
