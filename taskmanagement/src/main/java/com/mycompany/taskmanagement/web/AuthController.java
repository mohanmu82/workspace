package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.config.AuthProperties;
import com.mycompany.taskmanagement.config.JwtAuthFilter;
import com.mycompany.taskmanagement.model.UserAccess;
import com.mycompany.taskmanagement.service.UserAccessService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthProperties authProperties;
    private final UserAccessService userAccessService;

    @GetMapping("/config")
    public Map<String, Object> config() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("enabled", authProperties.isEnabled());
        config.put("jwtUrl", authProperties.getJwtUrl());
        config.put("redirectParam", authProperties.getRedirectParam());
        config.put("tokenParam", authProperties.getTokenParam());
        return config;
    }

    @GetMapping("/me")
    public Map<String, Object> me(HttpServletRequest request) {
        if (!authProperties.isEnabled()) {
            return Map.of(
                    "userid", "local-dev",
                    "name", "Local Dev",
                    "isAdmin", true,
                    "permission", fullPermission());
        }

        String userId = (String) request.getAttribute(JwtAuthFilter.ATTR_USER_ID);
        boolean isAdmin = Boolean.TRUE.equals(request.getAttribute(JwtAuthFilter.ATTR_IS_ADMIN));
        Optional<UserAccess> grant = userAccessService.findByUserId(userId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userid", userId);
        result.put("name", resolveName(grant, userId));
        result.put("isAdmin", isAdmin);
        if (isAdmin) {
            result.put("permission", fullPermission());
        } else {
            boolean active = grant.map(UserAccess::isActive).orElse(false);
            result.put("permission", active ? grant.map(this::toPermissionMap).orElse(null) : null);
        }
        return result;
    }

    private String resolveName(Optional<UserAccess> grant, String userId) {
        Map<String, String> attributes = grant.map(UserAccess::getAttributes).orElseGet(Map::of);
        String nickname = attributes.get("nickname");
        if (nickname != null && !nickname.isBlank()) return nickname;
        String fullName = (attributes.getOrDefault("firstName", "") + " " + attributes.getOrDefault("lastName", "")).trim();
        return fullName.isBlank() ? userId : fullName;
    }

    private Map<String, Object> fullPermission() {
        return Map.of("read", true, "create", true, "update", true, "delete", true);
    }

    private Map<String, Object> toPermissionMap(UserAccess a) {
        return Map.of("read", a.isCanRead(), "create", a.isCanCreate(),
                "update", a.isCanUpdate(), "delete", a.isCanDelete());
    }
}
