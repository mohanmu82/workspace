package com.mycompany.taskmanagement.event;

import com.mycompany.taskmanagement.model.Task;

/**
 * Published by {@link com.mycompany.taskmanagement.service.TaskService} whenever a task
 * is created, updated or deleted, so that {@link com.mycompany.taskmanagement.web.TaskWebSocketHandler}
 * can broadcast the change to connected browsers without a direct dependency between the two.
 */
public record TaskChangedEvent(String changeType, Long taskId, Task task) {

    public static TaskChangedEvent created(Task task) {
        return new TaskChangedEvent("created", task.getId(), task);
    }

    public static TaskChangedEvent updated(Task task) {
        return new TaskChangedEvent("updated", task.getId(), task);
    }

    public static TaskChangedEvent deleted(Long taskId) {
        return new TaskChangedEvent("deleted", taskId, null);
    }
}
