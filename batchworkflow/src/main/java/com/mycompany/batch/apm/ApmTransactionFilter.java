package com.mycompany.batch.apm;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Times every HTTP request the app serves and, once an APM server is registered (see
 * {@link ApmForwardingService}), hands each one off to be forwarded as a transaction. Runs on
 * every request but does no work beyond an {@code isRegistered()} check when nothing is
 * registered, and the actual forward happens on a virtual thread so it never adds APM-server
 * latency to the real request/response cycle.
 *
 * <p>Requests under {@code /apm/} are excluded to avoid the filter forwarding its own
 * register/status/requests-list calls back to itself.
 */
@Component
public class ApmTransactionFilter extends OncePerRequestFilter {

    private static final ExecutorService FORWARD_POOL = Executors.newVirtualThreadPerTaskExecutor();

    private final ApmForwardingService forwardingService;

    public ApmTransactionFilter(ApmForwardingService forwardingService) {
        this.forwardingService = forwardingService;
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response,
                                     @NonNull FilterChain chain) throws ServletException, IOException {
        String path = request.getRequestURI();
        if (path.startsWith("/apm/") || !forwardingService.isRegistered()) {
            chain.doFilter(request, response);
            return;
        }

        long start = System.currentTimeMillis();
        try {
            chain.doFilter(request, response);
        } finally {
            long latencyMs = System.currentTimeMillis() - start;
            String method = request.getMethod();
            int statusCode = response.getStatus();
            FORWARD_POOL.submit(() -> forwardingService.recordAndForward(method, path, statusCode, latencyMs));
        }
    }
}
