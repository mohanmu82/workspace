package com.mycompany.taskmanagement.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "auth")
@Data
public class AuthProperties {
    private boolean enabled = false;
    private String jwtUrl;
    private String redirectParam = "redirect_uri";
    private String tokenParam = "token";
    private String jwkUrl;
    private String keyId;
    private String bridClaim = "BRID";
    private String userDetailUrl;
    private List<String> adminUserids = List.of();
}
