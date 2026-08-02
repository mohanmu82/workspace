package com.mycompany.taskmanagement.web;

import com.mycompany.taskmanagement.config.AuthProperties;
import com.mycompany.taskmanagement.config.JwtAuthFilter;
import com.mycompany.taskmanagement.model.UserInfoAttributeConfig;
import com.mycompany.taskmanagement.service.UserInfoConfigService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/user-info-config")
@RequiredArgsConstructor
public class UserInfoConfigController {

    private final UserInfoConfigService userInfoConfigService;
    private final AuthProperties authProperties;

    @GetMapping
    public List<UserInfoAttributeConfig> list(HttpServletRequest request) {
        requireAdmin(request);
        return userInfoConfigService.list();
    }

    @PutMapping
    public List<UserInfoAttributeConfig> save(@RequestBody List<UserInfoAttributeConfig> configs,
                                               HttpServletRequest request) {
        requireAdmin(request);
        return userInfoConfigService.save(configs);
    }

    private void requireAdmin(HttpServletRequest request) {
        if (!authProperties.isEnabled()) return; // dev mode: auth disabled, everything open
        if (!Boolean.TRUE.equals(request.getAttribute(JwtAuthFilter.ATTR_IS_ADMIN))) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required");
        }
    }
}
