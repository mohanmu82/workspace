package com.mycompany.taskmanagement.dto;

import com.mycompany.taskmanagement.model.Task;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TaskListResponse {
    private List<Task> tasks;
    private long total;
    private int page;
    private int size;
}
