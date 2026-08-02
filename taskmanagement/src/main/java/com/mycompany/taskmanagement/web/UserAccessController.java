package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.config.AuthProperties;
import com.mycompany.taskmanagement.config.JwtAuthFilter;
import com.mycompany.taskmanagement.model.UserAccess;
import com.mycompany.taskmanagement.service.UserAccessService;
import com.mycompany.taskmanagement.service.UserDetailService;
import com.mycompany.taskmanagement.service.UserInfoConfigService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/user-access")
@RequiredArgsConstructor
public class UserAccessController {

    private final UserAccessService userAccessService;
    private final AuthProperties authProperties;
    private final UserDetailService userDetailService;
    private final UserInfoConfigService userInfoConfigService;

    @GetMapping
    public List<UserAccess> list(HttpServletRequest request) {
        requireAdmin(request);
        return userAccessService.list();
    }

    // Fetches user-info attributes from auth.user-detail-url for the given BRID, applying the
    // admin-configured JSONPath per attribute. Does not persist - the admin UI prefills the new
    // user's row with the result and lets them edit before saving via PUT /{userId}.
    @GetMapping("/lookup/{brid}")
    public Map<String, String> lookup(@PathVariable String brid, HttpServletRequest request) {
        requireAdmin(request);
        return userDetailService.fetchAttributes(brid, userInfoConfigService.list());
    }

    @PutMapping("/{userId}")
    public UserAccess save(@PathVariable String userId, @RequestBody UserAccess body, HttpServletRequest request) {
        requireAdmin(request);
        return userAccessService.save(userId, body);
    }

    @DeleteMapping("/{userId}")
    public ResponseEntity<Void> delete(@PathVariable String userId, HttpServletRequest request) {
        requireAdmin(request);
        userAccessService.delete(userId);
        return ResponseEntity.noContent().build();
    }

    private void requireAdmin(HttpServletRequest request) {
        if (!authProperties.isEnabled()) return; // dev mode: auth disabled, everything open
        if (!Boolean.TRUE.equals(request.getAttribute(JwtAuthFilter.ATTR_IS_ADMIN))) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required");
        }
    }
}
