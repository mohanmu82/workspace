package com.mycompany.taskmanagement.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@Entity
@Table(name = "user_access")
public class UserAccess {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    @Column(name = "user_id", nullable = false, unique = true, length = 100)
    private String userId;

    @Column(name = "can_read", nullable = false)
    private boolean canRead = true;

    @Column(name = "can_create", nullable = false)
    private boolean canCreate = false;

    @Column(name = "can_update", nullable = false)
    private boolean canUpdate = false;

    @Column(name = "can_delete", nullable = false)
    private boolean canDelete = false;

    @Column(name = "active", nullable = false)
    private boolean active = true;

    // Admin-configured user-info attributes (firstName, lastName, nickname, orgUnit, team, ...),
    // pulled from auth.user-detail-url when the user is added and editable afterward. Keys match
    // the "key" of a UserInfoAttributeConfig entry.
    @ElementCollection
    @CollectionTable(name = "user_access_attribute", joinColumns = @JoinColumn(name = "user_access_id"))
    @MapKeyColumn(name = "attr_key")
    @Column(name = "attr_value", length = 500)
    private Map<String, String> attributes = new LinkedHashMap<>();

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    @PreUpdate
    void touch() {
        updatedAt = LocalDateTime.now();
    }
}
