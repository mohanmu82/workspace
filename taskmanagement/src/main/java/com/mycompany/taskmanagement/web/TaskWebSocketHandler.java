package com.mycompany.taskmanagement.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.dto.TaskSearchRequest;
import com.mycompany.taskmanagement.event.TaskChangedEvent;
import com.mycompany.taskmanagement.service.TaskService;
import org.springframework.context.event.EventListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket endpoint at {@code /ws/tasks}.
 *
 * <p>Lets the browser run searches and receive live task change notifications over a single
 * persistent connection instead of a new HTTP request per query/poll.
 *
 * <p>Client requests:
 * <pre>
 * {"type":"search",        "requestId":"…", "payload": TaskSearchRequest}
 * {"type":"filter-options","requestId":"…"}
 * {"type":"ping",          "requestId":"…"}
 * </pre>
 *
 * <p>Server messages:
 * <pre>
 * {"type":"connected"}
 * {"type":"search-result",        "requestId":"…", "data": TaskListResponse}
 * {"type":"filter-options-result","requestId":"…", "data": {...}}
 * {"type":"pong",                 "requestId":"…"}
 * {"type":"error",                "requestId":"…", "message":"…"}
 * {"type":"task-changed", "changeType":"created|updated|deleted", "taskId":1, "task": Task}
 * </pre>
 */
@Component
public class TaskWebSocketHandler extends TextWebSocketHandler {

    private final TaskService taskService;
    private final ObjectMapper objectMapper;
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public TaskWebSocketHandler(TaskService taskService, ObjectMapper objectMapper) {
        this.taskService = taskService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(@NonNull WebSocketSession session) {
        sessions.put(session.getId(), session);
        sendJson(session, Map.of("type", "connected"));
    }

    @Override
    public void afterConnectionClosed(@NonNull WebSocketSession session, @NonNull CloseStatus status) {
        sessions.remove(session.getId());
    }

    @Override
    protected void handleTextMessage(@NonNull WebSocketSession session, @NonNull TextMessage message) {
        Map<String, Object> request;
        try {
            request = objectMapper.readValue(message.getPayload(), new TypeReference<>() {});
        } catch (Exception e) {
            sendJson(session, Map.of("type", "error", "message", "Invalid message: " + e.getMessage()));
            return;
        }

        String type = String.valueOf(request.get("type"));
        Object requestId = request.get("requestId");

        try {
            switch (type) {
                case "search" -> {
                    TaskSearchRequest searchRequest = objectMapper.convertValue(
                            request.getOrDefault("payload", Map.of()), TaskSearchRequest.class);
                    sendResult(session, requestId, "search-result", taskService.search(searchRequest));
                }
                case "filter-options" -> sendResult(session, requestId, "filter-options-result", taskService.getFilterOptions());
                case "ping" -> sendResult(session, requestId, "pong", null);
                default -> sendJson(session, Map.of("type", "error", "requestId", nullToEmpty(requestId),
                        "message", "Unknown message type: " + type));
            }
        } catch (Exception e) {
            sendJson(session, Map.of("type", "error", "requestId", nullToEmpty(requestId),
                    "message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    /**
     * Broadcasts every create/update/delete to all connected browsers so open task views
     * stay in sync without polling. Decoupled from {@link TaskService} via the Spring event bus.
     */
    @EventListener
    public void onTaskChanged(TaskChangedEvent event) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "task-changed");
        msg.put("changeType", event.changeType());
        msg.put("taskId", event.taskId());
        msg.put("task", event.task());
        String json = writeJson(msg);
        if (json == null) return;
        sessions.values().forEach(session -> sendRaw(session, json));
    }

    private void sendResult(WebSocketSession session, Object requestId, String type, Object data) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", type);
        msg.put("requestId", nullToEmpty(requestId));
        msg.put("data", data);
        sendJson(session, msg);
    }

    private void sendJson(WebSocketSession session, Object payload) {
        String json = writeJson(payload);
        if (json != null) sendRaw(session, json);
    }

    private String writeJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return null;
        }
    }

    private void sendRaw(WebSocketSession session, String json) {
        try {
            synchronized (session) {
                if (session.isOpen()) session.sendMessage(new TextMessage(json));
            }
        } catch (Exception ignored) {}
    }

    private static Object nullToEmpty(Object requestId) {
        return requestId != null ? requestId : "";
    }
}
