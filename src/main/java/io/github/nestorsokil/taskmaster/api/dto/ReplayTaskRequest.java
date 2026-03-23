package io.github.nestorsokil.taskmaster.api.dto;

import jakarta.validation.constraints.Min;

import java.time.Instant;

/**
 * Request body for {@code POST /tasks/v1/{taskId}/replay}.
 * All fields are optional.
 *
 * @param maxAttempts override max attempts for the retried task; if null, reset to original value
 * @param deadline   override deadline; useful when the original deadline has already expired
 */
public record ReplayTaskRequest(
        @Min(1) Integer maxAttempts,
        Instant deadline
) {}
