package com.mycompany.taskmanagement.dto;

import com.mycompany.taskmanagement.model.Task;
import com.mycompany.taskmanagement.model.TaskDependency;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TaskDependencyGraphResponse {
    private List<Task> tasks;
    private List<TaskDependency> dependencies;
}
