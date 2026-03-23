package io.github.nestorsokil.taskmaster.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;

/**
 * Request body for {@code POST /tasks/v1}.
 *
 * @param queueName   the logical queue to submit the task into
 * @param payload     arbitrary JSON passed through unchanged to the worker
 * @param priority    scheduling priority — higher is claimed first; defaults to 0
 * @param maxAttempts max execution attempts before the task is marked DEAD; omit to use default (3)
 * @param deadline    optional hard deadline; if the task is still PENDING after this
 *                    instant the DeadlineReaper will move it to DEAD; null means no deadline
 */
public record SubmitTaskRequest(
        @NotBlank String queueName,
        @NotNull JsonNode payload,
        int priority,
        @Min(1) Integer maxAttempts,
        Instant deadline,
        @Size(max = 16) List<@Size(max = 64) String> tags,
        String callbackUrl
) {
    public SubmitTaskRequest {
        if (maxAttempts == null) maxAttempts = 3;
        if (tags == null) tags = List.of();
    }
}
