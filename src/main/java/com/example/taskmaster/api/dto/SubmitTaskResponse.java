package com.example.taskmaster.api.dto;

import java.util.UUID;

/**
 * Response body for {@code POST /tasks} — 202 Accepted.
 *
 * @param taskId the server-assigned UUID for the newly created task
 * @param status always {@code "PENDING"} at submission time
 */
public record SubmitTaskResponse(UUID taskId, String status) {}
