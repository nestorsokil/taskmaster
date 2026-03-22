package com.example.taskmaster.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * Request body for {@code POST /tasks/v1/claim}.
 *
 * @param workerId  the claiming worker's ID; auto-registered if not already known
 * @param queueName queue to claim from; must match the worker's registered queue
 * @param maxTasks  upper bound on tasks returned in a single claim call (1–100)
 */
public record ClaimTasksRequest(
        @NotBlank String workerId,
        @NotBlank String queueName,
        @Min(1) @Max(100) int maxTasks
) {}
