package com.mycompany.taskmanagement.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Admin-defined extra user-info attribute (e.g. firstName, nickname, orgUnit). Describes how to
 * pull the value out of the auth.user-detail-url response when a BRID is looked up.
 */
@Data
@NoArgsConstructor
public class UserInfoAttributeConfig {
    private String key;
    private String label;
    private String jsonPath;
    private String defaultValue;
}
