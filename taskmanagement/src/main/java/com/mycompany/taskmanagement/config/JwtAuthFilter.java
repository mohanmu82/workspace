package com.mycompany.taskmanagement.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mycompany.taskmanagement.model.UserAccess;
import com.mycompany.taskmanagement.service.UserAccessService;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.UnsupportedJwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies the browser-SSO JWT on every /api/** call and enforces the per-user
 * read/create/update/delete grants from {@link UserAccessService}. Disabled entirely
 * when auth.enabled=false (local dev).
 */
@Component
@RequiredArgsConstructor
public class JwtAuthFilter extends OncePerRequestFilter {

    public static final String ATTR_USER_ID = "authUserId";
    public static final String ATTR_IS_ADMIN = "authIsAdmin";

    private final AuthProperties authProperties;
    private final UserAccessService userAccessService;
    private final JwtPublicKeyProvider publicKeyProvider;
    private final ObjectMapper objectMapper;

    @Override
    protected boolean shouldNotFilter(@NonNull HttpServletRequest request) {
        String path = request.getRequestURI();
        return !authProperties.isEnabled() || !path.startsWith("/api/") || path.equals("/api/auth/config");
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response,
                                     @NonNull FilterChain chain) throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        String token = (header != null && header.startsWith("Bearer ")) ? header.substring(7) : null;
        if (token == null) {
            reject(response, HttpStatus.UNAUTHORIZED, "Missing token");
            return;
        }

        Claims claims;
        try {
            String expectedKeyId = authProperties.getKeyId();
            claims = Jwts.parser()
                    .keyLocator(jwtHeader -> {
                        Object kid = jwtHeader.get("kid");
                        if (!expectedKeyId.equals(kid)) {
                            throw new UnsupportedJwtException("Unknown key id: " + kid);
                        }
                        return publicKeyProvider.getPublicKey();
                    })
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();
        } catch (JwtException | IllegalArgumentException e) {
            reject(response, HttpStatus.UNAUTHORIZED, "Invalid or expired token");
            return;
        }

        String userId = claims.get(authProperties.getBridClaim(), String.class);
        boolean isAdmin = authProperties.getAdminUserids().contains(userId);

        request.setAttribute(ATTR_USER_ID, userId);
        request.setAttribute(ATTR_IS_ADMIN, isAdmin);

        // /api/auth/me must be reachable even without a grant, so the frontend can show
        // a tailored "no access" screen instead of a bare 403.
        if ("/api/auth/me".equals(request.getRequestURI())) {
            chain.doFilter(request, response);
            return;
        }

        if (!isAdmin) {
            Optional<UserAccess> grant = userAccessService.findByUserId(userId);
            if (grant.isEmpty() || !grant.get().isActive()) {
                reject(response, HttpStatus.FORBIDDEN, "No access granted");
                return;
            }
            if (!permits(grant.get(), request.getMethod())) {
                reject(response, HttpStatus.FORBIDDEN, "Insufficient permission");
                return;
            }
        }

        chain.doFilter(request, response);
    }

    private boolean permits(UserAccess access, String method) {
        return switch (method) {
            case "GET", "HEAD", "OPTIONS" -> access.isCanRead();
            case "POST" -> access.isCanCreate();
            case "PUT", "PATCH" -> access.isCanUpdate();
            case "DELETE" -> access.isCanDelete();
            default -> false;
        };
    }

    private void reject(HttpServletResponse response, HttpStatus status, String message) throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json");
        objectMapper.writeValue(response.getWriter(), Map.of("error", message));
    }
}
