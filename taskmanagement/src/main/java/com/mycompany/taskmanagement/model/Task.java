package com.mycompany.taskmanagement.model;

import com.mycompany.taskmanagement.converter.StringListConverter;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@Entity
@Table(name = "tasks")
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    @Column(nullable = false, length = 255)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(length = 50)
    private String status = "TODO";

    @Column(length = 50)
    private String priority = "MEDIUM";

    @Column(length = 100)
    private String assignee;

    @Column(name = "createdby", length = 100)
    private String createdBy;

    @Column(name = "due_date")
    private LocalDateTime dueDate;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(length = 100)
    private String category;

    @Convert(converter = StringListConverter.class)
    @Column(length = 500)
    private List<String> tags;

    @Column(name = "estimated_hours", precision = 8, scale = 2)
    private BigDecimal estimatedHours;

    @Column(name = "actual_hours", precision = 8, scale = 2)
    private BigDecimal actualHours;

    @Column(length = 100)
    private String programme;

    @Column(length = 100)
    private String project;

    @Column(name = "assetclass", length = 100)
    private String assetClass;

    @Column(name = "working_group", length = 100)
    private String workingGroup;

    @Column(length = 100)
    private String stakeholder;

    @Column(length = 100)
    private String jira;

    @Column(length = 255)
    private String links;

    @Column(name = "parent_task_id")
    private Long parentTaskId;

    @Column(name = "custom_field_1", length = 255)
    private String customField1;
    @Column(name = "custom_field_2", length = 255)
    private String customField2;
    @Column(name = "custom_field_3", length = 255)
    private String customField3;
    @Column(name = "custom_field_4", length = 255)
    private String customField4;
    @Column(name = "custom_field_5", length = 255)
    private String customField5;
    @Column(name = "custom_field_6", length = 255)
    private String customField6;
    @Column(name = "custom_field_7", length = 255)
    private String customField7;
    @Column(name = "custom_field_8", length = 255)
    private String customField8;
    @Column(name = "custom_field_9", length = 255)
    private String customField9;
    @Column(name = "custom_field_10", length = 255)
    private String customField10;

    @PrePersist
    void prePersist() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
