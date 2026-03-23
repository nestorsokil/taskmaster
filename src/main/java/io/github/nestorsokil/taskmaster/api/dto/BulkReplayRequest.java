package io.github.nestorsokil.taskmaster.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.Instant;

/**
 * Request body for {@code POST /tasks/v1/replay}.
 *
 * @param queueName   which queue to replay (required)
 * @param deadSince   only replay tasks whose finished_at is after this timestamp; null replays all
 * @param maxAttempts override max attempts for all replayed tasks; null preserves original values
 */
public record BulkReplayRequest(
@NotBlank String queueName,
Instant deadSince,
@Min(1) Integer maxAttempts
) {}
