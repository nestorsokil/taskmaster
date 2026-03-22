package com.example.taskmaster.api.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Request body for {@code POST /tasks/v1/{taskId}/complete}.
 *
 * @param workerId the worker completing the task; must match the current owner
 * @param result   opaque result string stored on the task (e.g. {@code "sent:msg-id-123"})
 */
public record CompleteTaskRequest(
        @NotBlank String workerId,
        String result
) {}
