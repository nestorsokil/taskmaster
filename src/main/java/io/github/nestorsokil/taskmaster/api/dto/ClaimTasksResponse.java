package io.github.nestorsokil.taskmaster.api.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.UUID;

/**
 * Response body for {@code POST /tasks/claim}.
 * Returns an empty list when no tasks are available — never a 404.
 *
 * @param tasks the atomically claimed tasks; may be empty
 */
public record ClaimTasksResponse(List<ClaimedTask> tasks) {

    /**
     * A single claimed task as seen by the worker.
     * Only the fields the worker actually needs are exposed.
     *
     * @param taskId     task identifier the worker must echo back on complete/fail
     * @param payload    the original JSON payload submitted by the producer
     * @param attempts   how many times this task has been attempted (including this claim)
     * @param complexity relative execution cost declared at submission time
     */
    public record ClaimedTask(UUID taskId, JsonNode payload, int attempts, int complexity) {}
}
