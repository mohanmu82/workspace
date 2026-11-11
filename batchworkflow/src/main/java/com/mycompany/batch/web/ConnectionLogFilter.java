package com.mycompany.batch.web;

import com.mycompany.batch.service.ConnectionLogService;
import com.mycompany.batch.service.ConnectionLogService.Connection;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Logs every HTTP exchange on the MCP endpoints ({@code /mcp}, {@code /sse}, {@code /mcp/message})
 * and every response streamed as {@code text/event-stream}, into {@link ConnectionLogService}. A
 * streaming response stays OPEN until its async request completes, times out or errors.
 */
@Component
public class ConnectionLogFilter extends OncePerRequestFilter {

    static final String STREAMABLE = "MCP (Streamable HTTP)";
    static final String MCP_SSE = "MCP (HTTP+SSE)";
    static final String SSE = "SSE";

    private final ConnectionLogService log;

    public ConnectionLogFilter(ConnectionLogService log) {
        this.log = log;
    }

    static String mcpProtocol(String path) {
        return switch (path) {
            case "/mcp" -> STREAMABLE;
            case "/sse", "/mcp/message" -> MCP_SSE;
            default -> null;
        };
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain) throws ServletException, IOException {
        String path = request.getRequestURI();
        String protocol = mcpProtocol(path);
        // Only MCP bodies are cached: those are the JSON-RPC requests worth showing.
        HttpServletRequest req = protocol != null ? new ContentCachingRequestWrapper(request, ConnectionLogService.MAX_TEXT) : request;
        long start = System.currentTimeMillis();
        chain.doFilter(req, response);

        String contentType = response.getContentType();
        if (protocol == null && (contentType == null || !contentType.startsWith("text/event-stream"))) return;

        String body = req instanceof ContentCachingRequestWrapper w && w.getContentAsByteArray().length > 0
                ? new String(w.getContentAsByteArray(), StandardCharsets.UTF_8) : null;
        String sessionId = firstNonBlank(request.getHeader("Mcp-Session-Id"), response.getHeader("Mcp-Session-Id"),
                request.getParameter("sessionId"));
        // shortcut: GET /sse's session id is only sent inside the stream, so that row shows none; its /mcp/message posts carry it.
        Connection c = log.open(protocol != null ? protocol : SSE, request.getMethod(), path, sessionId,
                body != null ? body : request.getQueryString(), request.getRemoteAddr(), request.getHeader("User-Agent"));
        c.startedAt = start;

        if (!request.isAsyncStarted()) {
            log.close(c, response.getStatus(), "DONE");
            return;
        }
        request.getAsyncContext().addListener(new AsyncListener() {
            public void onComplete(AsyncEvent e) { log.close(c, response.getStatus(), "CLOSED"); }
            public void onTimeout(AsyncEvent e)  { log.close(c, response.getStatus(), "TIMEOUT"); }
            public void onError(AsyncEvent e)    { log.close(c, response.getStatus(), "ERROR"); }
            public void onStartAsync(AsyncEvent e) {}
        });
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) if (v != null && !v.isBlank()) return v;
        return null;
    }
}
