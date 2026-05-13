package com.mycompany.taskmanagement.dto;

import lombok.Data;

import java.time.LocalDate;
import java.util.List;

@Data
public class TaskSearchRequest {
    private String q;
    private List<String> status;
    private List<String> priority;
    private String assignee;
    private LocalDate dueFrom;
    private LocalDate dueTo;
    private String category;
    private String programme;
    private String project;
    private String workingGroup;
    private String assetClass;
    private String stakeholder;
    private String jira;
    private List<String> tags;
    private String groupBy;
    private int page = 0;
    private int size = 200;
    private String sortBy = "dueDate";
    private String sortDir = "asc";
}
