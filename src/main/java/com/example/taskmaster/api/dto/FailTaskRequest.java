package com.example.taskmaster.api.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Request body for {@code POST /tasks/v1/{taskId}/fail}.
 *
 * @param workerId the worker reporting the failure; must match the current owner
 * @param error    human-readable error message stored as {@code last_error}
 */
public record FailTaskRequest(
        @NotBlank String workerId,
        @NotBlank String error
) {}
