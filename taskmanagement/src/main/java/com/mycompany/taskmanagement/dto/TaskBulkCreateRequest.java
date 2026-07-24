package com.mycompany.taskmanagement.dto;

import lombok.Data;

import java.util.List;

@Data
public class TaskBulkCreateRequest {
    private String project;
    private String programme;
    private List<String> titles;
}
