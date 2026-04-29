package com.mycompany.batch.web;

import com.mycompany.batch.model.stream.ExecuteRequest;
import com.mycompany.batch.model.stream.UnsubscribeRequest;
import com.mycompany.batch.service.StreamSessionService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
@RequestMapping("/stream")
public class StreamController {

    private final StreamSessionService streamSessionService;

    public StreamController(StreamSessionService streamSessionService) {
        this.streamSessionService = streamSessionService;
    }

    // -------------------------------------------------------------------------
    // GET /stream/{sessionId}  — open SSE stream
    // -------------------------------------------------------------------------

    @GetMapping(value = "/{sessionId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter createSession(
            @PathVariable String sessionId,
            @RequestParam(required = false) String createdBy) {
        return streamSessionService.createSession(sessionId, createdBy);
    }

    // -------------------------------------------------------------------------
    // POST /stream/execute  — register dataset subscription and push data
    // -------------------------------------------------------------------------

    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> execute(@RequestBody ExecuteRequest request) {
        try {
            Map<String, Object> result = streamSessionService.execute(request);
            return ResponseEntity.accepted().body(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }

    // -------------------------------------------------------------------------
    // POST /stream/unsubscribe  — stop streaming a dataset alias
    // -------------------------------------------------------------------------

    @PostMapping("/unsubscribe")
    public ResponseEntity<Map<String, Object>> unsubscribe(@RequestBody UnsubscribeRequest request) {
        try {
            Map<String, Object> result = streamSessionService.unsubscribe(request);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
