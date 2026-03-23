package io.github.nestorsokil.taskmaster.api.dto;

/**
 * Response body for {@code POST /tasks/v1/replay}.
 *
 * @param replayed number of DEAD tasks moved back to PENDING
 */
public record BulkReplayResponse(int replayed) {}
