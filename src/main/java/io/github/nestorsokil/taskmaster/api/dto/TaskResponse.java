package io.github.nestorsokil.taskmaster.api.dto;

import io.github.nestorsokil.taskmaster.domain.Task;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Response body for {@code GET /tasks/{taskId}} and items in {@code GET /tasks}.
 *
 * @param taskId     unique task identifier
 * @param queueName  queue the task belongs to
 * @param status     current lifecycle state
 * @param result     worker-supplied result string; non-null only when status is DONE
 * @param lastError  error message from the most recent failed attempt
 * @param attempts   number of execution attempts consumed so far
 * @param createdAt  when the task was submitted
 * @param finishedAt when the task reached a terminal state; null if still in progress
 */
public record TaskResponse(
        UUID taskId,
        String queueName,
        String status,
        String result,
        String lastError,
        int attempts,
        List<String> tags,
        String callbackUrl,
        Instant createdAt,
        Instant finishedAt
) {
    public static TaskResponse from(Task task) {
        return new TaskResponse(
                task.id(),
                task.queueName(),
                task.status(),
                task.result(),
                task.lastError(),
                task.attempts(),
                task.tags().values(),
                task.callbackUrl(),
                task.createdAt(),
                task.finishedAt()
        );
    }
}
