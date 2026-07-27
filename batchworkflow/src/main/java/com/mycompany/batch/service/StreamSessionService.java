package com.mycompany.batch.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.batch.model.DataRow;
import com.mycompany.batch.model.ExecutionMode;
import com.mycompany.batch.model.RunRequest;
import com.mycompany.batch.model.stream.DatasetSubscription;
import com.mycompany.batch.model.stream.ExecuteRequest;
import com.mycompany.batch.model.stream.LiveKeyInfo;
import com.mycompany.batch.model.stream.StreamSession;
import com.mycompany.batch.model.stream.UnsubscribeRequest;
import com.mycompany.batch.util.Threads;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class StreamSessionService {

    private final ObjectMapper objectMapper;
    private final BatchService batchService;
    private final ConcurrentHashMap<String, StreamSession> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final HttpClient httpClient = HttpClient.newBuilder().build();

    @Value("${server.port:8080}")
    private int serverPort;

    public StreamSessionService(ObjectMapper objectMapper, BatchService batchService) {
        this.objectMapper = objectMapper;
        this.batchService = batchService;
    }

    // -------------------------------------------------------------------------
    // Session lifecycle
    // -------------------------------------------------------------------------

    public SseEmitter createSession(String sessionId, String createdBy) {
        StreamSession existing = sessions.get(sessionId);
        if (existing != null && "active".equals(existing.getStatus())) {
            throw new IllegalStateException("Session already active: " + sessionId);
        }

        SseEmitter emitter = new SseEmitter(0L);
        StreamSession session = new StreamSession(sessionId, "/stream/" + sessionId,
                createdBy != null ? createdBy : "", Instant.now(), emitter);

        emitter.onCompletion(() -> closeSession(sessionId));
        emitter.onTimeout(()    -> closeSession(sessionId));
        emitter.onError(e       -> closeSession(sessionId));

        sessions.put(sessionId, session);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sessionId",  sessionId);
        payload.put("streamUri",  "/stream/" + sessionId);
        payload.put("createdBy",  session.getCreatedBy());
        payload.put("createdAt",  session.getCreatedAt().toString());
        sendEvent(session, "session.created", payload);

        return emitter;
    }

    private void closeSession(String sessionId) {
        StreamSession s = sessions.remove(sessionId);
        if (s != null) {
            s.setStatus("closed");
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "session.closed");
            event.put("sessionId", sessionId);
            event.put("timestamp", Instant.now().toString());
            try { sendEvent(s, "session.closed", event); } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Execute
    // -------------------------------------------------------------------------

    public Map<String, Object> execute(ExecuteRequest req) {
        ExecuteRequest.StreamInfo  si = req.getStream();
        ExecuteRequest.DatasetInfo di = req.getDataset();

        if (si == null || si.getStreamUri() == null)
            throw new IllegalArgumentException("stream.streamUri is required");
        if (di == null || di.getDatasetAlias() == null)
            throw new IllegalArgumentException("dataset.datasetAlias is required");

        String sessionId    = extractSessionId(si.getStreamUri());
        StreamSession session = sessions.get(sessionId);
        if (session == null || !"active".equals(session.getStatus()))
            throw new IllegalArgumentException("Session not found or not active: " + sessionId);

        String datasetAlias = di.getDatasetAlias();
        boolean isBroadcast = "broadcast".equalsIgnoreCase(si.getDestination());
        boolean isLive      = "live".equalsIgnoreCase(si.getStreamType());

        // Inherit metadata from an existing subscription when requested
        if (di.isInheritMetadata()) {
            DatasetSubscription proto = findSubscription(datasetAlias);
            if (proto != null) {
                if (di.getReloadUrl() == null) di.setReloadUrl(proto.getReloadUrl());
                if (di.getLiveKey()   == null) di.setLiveKey(proto.getLiveKey());
                if (req.getUrl()      == null) req.setUrl(proto.getUrl());
            }
        }

        // Async batch streaming: run a batch operation and push each DataRow as an SSE event
        if (req.getBatch() != null && req.getExecutionMode() == ExecutionMode.ASYNC) {
            runBatchAsync(session, req.getBatch(), datasetAlias, req.getRequestId());
            return Map.of(
                    "status",       "accepted",
                    "requestId",    req.getRequestId() != null ? req.getRequestId() : "",
                    "datasetAlias", datasetAlias,
                    "mode",         "batch.async");
        }

        // Register subscription for requesting session
        DatasetSubscription sub = buildSubscription(req, datasetAlias, si.getDestination(), si.getStreamType());
        session.getDatasets().put(datasetAlias, sub);

        // Schedule TTL expiry
        if (req.getTimeToLive() > 0) {
            scheduler.schedule(() -> expireDataset(datasetAlias, isBroadcast ? null : sessionId),
                    req.getTimeToLive(), TimeUnit.SECONDS);
        }

        // Determine whether this is an update to an existing live subscription (rowUpdate) or a fresh load
        String fetchUrl = (isLive && di.getReloadUrl() != null) ? di.getReloadUrl() : req.getUrl();
        boolean isRowUpdate = isLive && di.getLiveKey() != null
                && session.getDatasets().containsKey(datasetAlias);

        String urlMethod = req.getUrlMethod() != null ? req.getUrlMethod().toUpperCase() : "GET";
        String urlBody   = req.getUrlBody();

        Threads.startDaemon(() -> {
            try {
                Object data      = fetchData(fetchUrl, urlMethod, urlBody);
                String eventType = isRowUpdate ? "dataset.rowUpdate" : "dataset.load";

                Map<String, Object> payload = buildPayload(req, datasetAlias, eventType,
                        isRowUpdate ? firstRow(data) : data, isRowUpdate ? di.getLiveKey() : null);

                // Push to requesting session
                sendEvent(session, eventType, payload);

                // If broadcast: also notify all OTHER sessions subscribed to this alias
                if (isBroadcast) {
                    Map<String, Object> broadcastPayload = buildBroadcastPayload(
                            sessionId, req.getRequestedBy(), datasetAlias,
                            di.getLiveKey(), firstRow(data));
                    broadcastToOthers(sessionId, datasetAlias, "dataset.broadcast", broadcastPayload);
                }
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("eventType",    "dataset.error");
                err.put("datasetAlias", datasetAlias);
                err.put("error",        msg);
                err.put("timestamp",    Instant.now().toString());
                sendEvent(session, "dataset.error", err);
                if (isBroadcast) broadcastToOthers(sessionId, datasetAlias, "dataset.error", err);
            }
        });

        return Map.of(
                "status",       "accepted",
                "requestId",    req.getRequestId() != null ? req.getRequestId() : "",
                "datasetAlias", datasetAlias);
    }

    // -------------------------------------------------------------------------
    // Unsubscribe
    // -------------------------------------------------------------------------

    public Map<String, Object> unsubscribe(UnsubscribeRequest req) {
        if (req.getSessionId() == null)     throw new IllegalArgumentException("sessionId is required");
        if (req.getDatasetAlias() == null)  throw new IllegalArgumentException("datasetAlias is required");

        StreamSession session = sessions.get(req.getSessionId());
        if (session == null)
            throw new IllegalArgumentException("Session not found: " + req.getSessionId());

        session.getDatasets().remove(req.getDatasetAlias());

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType",    "dataset.unsubscribed");
        event.put("datasetAlias", req.getDatasetAlias());
        event.put("timestamp",    Instant.now().toString());
        sendEvent(session, "dataset.unsubscribed", event);

        return Map.of("status", "unsubscribed", "datasetAlias", req.getDatasetAlias());
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void runBatchAsync(StreamSession session, RunRequest batchReq,
                               String datasetAlias, String requestId) {
        Threads.startDaemon(() -> {
            String batchUuid = java.util.UUID.randomUUID().toString();
            try {
                RunRequest resolvedReq = batchService.resolveAlias(batchReq);
                java.util.Map<String, String> opProps = batchService.loadRequestProperties(resolvedReq);
                java.util.List<DataRow> rows = batchService.buildInputRows(resolvedReq, opProps);

                Map<String, Object> ack = new LinkedHashMap<>();
                ack.put("eventType",    "batch.ack");
                ack.put("datasetAlias", datasetAlias);
                ack.put("batchUuid",    batchUuid);
                ack.put("rowCount",     rows.size());
                if (requestId != null) ack.put("requestId", requestId);
                ack.put("timestamp",    Instant.now().toString());
                sendEvent(session, "batch.ack", ack);

                batchService.runAsync(rows, resolvedReq, row -> {
                    Map<String, Object> rowMsg = new LinkedHashMap<>();
                    rowMsg.put("eventType",    "batch.row");
                    rowMsg.put("datasetAlias", datasetAlias);
                    rowMsg.put("batchUuid",    batchUuid);
                    rowMsg.put("row",          row);
                    rowMsg.put("timestamp",    Instant.now().toString());
                    sendEvent(session, "batch.row", rowMsg);
                }).thenAccept(result -> {
                    Map<String, Object> meta = new LinkedHashMap<>();
                    meta.put("processed",      result.processed());
                    meta.put("succeeded",      result.succeeded());
                    meta.put("failed",         result.failed());
                    meta.put("timeTakenMs",    result.timeTakenMs());
                    meta.put("responseSizeKb", result.responseSizeKb());
                    meta.put("timestamp",      result.timestamp());
                    meta.put("batchUuid",      batchUuid);

                    Map<String, Object> done = new LinkedHashMap<>();
                    done.put("eventType",    "batch.done");
                    done.put("datasetAlias", datasetAlias);
                    done.put("batchUuid",    batchUuid);
                    done.put("metadata",     meta);
                    done.put("columns",      result.columns());
                    if (requestId != null) done.put("requestId", requestId);
                    done.put("timestamp",    Instant.now().toString());
                    sendEvent(session, "batch.done", done);
                }).exceptionally(ex -> {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    Map<String, Object> err = new LinkedHashMap<>();
                    err.put("eventType",    "batch.error");
                    err.put("datasetAlias", datasetAlias);
                    err.put("batchUuid",    batchUuid);
                    err.put("error",        cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName());
                    if (requestId != null) err.put("requestId", requestId);
                    err.put("timestamp",    Instant.now().toString());
                    sendEvent(session, "batch.error", err);
                    return null;
                });
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("eventType",    "batch.error");
                err.put("datasetAlias", datasetAlias);
                err.put("batchUuid",    batchUuid);
                err.put("error",        e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                if (requestId != null) err.put("requestId", requestId);
                err.put("timestamp",    Instant.now().toString());
                sendEvent(session, "batch.error", err);
            }
        });
    }

    private void sendEvent(StreamSession session, String eventType, Object data) {
        synchronized (session) {
            try {
                String json = objectMapper.writeValueAsString(data);
                SseEmitter.SseEventBuilder builder = SseEmitter.event()
                        .name(eventType != null ? eventType : "message")
                        .data(json != null ? json : "null", MediaType.APPLICATION_JSON);
                session.getEmitter().send(builder);
            } catch (Exception e) {
                session.setStatus("closed");
            }
        }
    }

    private void broadcastToOthers(String originSessionId, String datasetAlias,
                                   String eventType, Object payload) {
        for (StreamSession s : sessions.values()) {
            if (!s.getSessionId().equals(originSessionId)
                    && "active".equals(s.getStatus())
                    && s.getDatasets().containsKey(datasetAlias)) {
                sendEvent(s, eventType, payload);
            }
        }
    }

    private void expireDataset(String datasetAlias, String sessionId) {
        Iterable<StreamSession> targets = sessionId != null
                ? List.of(sessions.get(sessionId))
                : sessions.values();

        for (StreamSession s : targets) {
            if (s == null || !"active".equals(s.getStatus())) continue;
            DatasetSubscription sub = s.getDatasets().remove(datasetAlias);
            if (sub == null) continue;
            sub.setStatus("expired");
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType",    "dataset.expired");
            event.put("datasetAlias", datasetAlias);
            event.put("timestamp",    Instant.now().toString());
            sendEvent(s, "dataset.expired", event);
        }
    }

    private Object fetchData(String url, String method, String body) throws Exception {
        String resolved = resolveUrl(url);
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(resolved))
                .header("Content-Type", "application/json");
        if ("POST".equals(method)) {
            builder.POST(HttpRequest.BodyPublishers.ofString(body != null ? body : "{}"));
        } else {
            builder.GET();
        }
        HttpResponse<String> resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300)
            throw new RuntimeException("HTTP " + resp.statusCode() + ": " + resp.body());
        Object parsed = objectMapper.readValue(resp.body(), new TypeReference<Object>() {});
        if (parsed instanceof Map<?, ?> m && m.get("data") instanceof List<?>) {
            return m.get("data");
        }
        return parsed;
    }

    private String resolveUrl(String url) {
        if (url == null || url.isBlank()) return "http://localhost:" + serverPort + "/";
        if (url.startsWith("http://") || url.startsWith("https://")) return url;
        return "http://localhost:" + serverPort + (url.startsWith("/") ? url : "/" + url);
    }

    private static String extractSessionId(String streamUri) {
        String prefix = "/stream/";
        return streamUri.startsWith(prefix) ? streamUri.substring(prefix.length()) : streamUri;
    }

    private DatasetSubscription findSubscription(String datasetAlias) {
        for (StreamSession s : sessions.values()) {
            DatasetSubscription sub = s.getDatasets().get(datasetAlias);
            if (sub != null) return sub;
        }
        return null;
    }

    private DatasetSubscription buildSubscription(ExecuteRequest req, String datasetAlias,
                                                   String destination, String streamType) {
        Instant now = Instant.now();
        return new DatasetSubscription(
                datasetAlias,
                req.getRequestId(),
                req.getRequestedBy(),
                now,
                req.getTimeToLive(),
                req.getTimeToLive() > 0 ? now.plusSeconds(req.getTimeToLive()) : null,
                req.getUrl(),
                destination,
                streamType,
                req.getDataset() != null ? req.getDataset().getReloadUrl() : null,
                req.getDataset() != null ? req.getDataset().getLiveKey()   : null);
    }

    private Map<String, Object> buildPayload(ExecuteRequest req, String datasetAlias,
                                              String eventType, Object data, LiveKeyInfo rowKey) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("eventType",    eventType);
        p.put("datasetAlias", datasetAlias);
        if (req.getRequestId() != null) p.put("requestId", req.getRequestId());
        p.put("streamType",   req.getStream().getStreamType());
        p.put("destination",  req.getStream().getDestination());
        p.put("timestamp",    Instant.now().toString());
        if (rowKey != null) p.put("rowKey", rowKey);
        p.put("payload",      data);
        return p;
    }

    private Map<String, Object> buildBroadcastPayload(String originSessionId, String originRequestedBy,
                                                       String datasetAlias, LiveKeyInfo rowKey, Object data) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("eventType",         "dataset.broadcast");
        p.put("datasetAlias",      datasetAlias);
        p.put("originSessionId",   originSessionId);
        p.put("originRequestedBy", originRequestedBy != null ? originRequestedBy : "");
        p.put("timestamp",         Instant.now().toString());
        if (rowKey != null) p.put("rowKey", rowKey);
        p.put("payload", data);
        return p;
    }

    private Object firstRow(Object data) {
        if (data instanceof List<?> list && !list.isEmpty()) return list.get(0);
        return data;
    }
}
