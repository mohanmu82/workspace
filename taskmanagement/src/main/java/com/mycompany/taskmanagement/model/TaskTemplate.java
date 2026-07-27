package com.mycompany.taskmanagement.model;

import com.mycompany.taskmanagement.converter.StringListConverter;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A reusable set of default task fields. Applying a template pre-fills the task
 * form; it does not itself create a task.
 */
@Data
@NoArgsConstructor
@Entity
@Table(name = "task_templates")
public class TaskTemplate {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    @Column(nullable = false, unique = true, length = 150)
    private String name;

    @NotBlank
    @Column(nullable = false, length = 255)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(length = 50)
    private String priority = "MEDIUM";

    @Column(length = 100)
    private String category;

    @Convert(converter = StringListConverter.class)
    @Column(length = 500)
    private List<String> tags;

    @Column(name = "estimated_hours", precision = 8, scale = 2)
    private BigDecimal estimatedHours;

    @Column(length = 100)
    private String programme;

    @Column(length = 100)
    private String project;

    @Column(name = "assetclass", length = 100)
    private String assetClass;

    @Column(name = "working_group", length = 100)
    private String workingGroup;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

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
