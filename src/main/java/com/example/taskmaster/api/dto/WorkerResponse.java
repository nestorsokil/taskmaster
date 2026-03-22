package com.example.taskmaster.api.dto;

import com.example.taskmaster.domain.Worker;

import java.time.Instant;
import java.util.List;

/**
 * Response body for items in {@code GET /workers}.
 *
 * @param workerId       stable worker identifier
 * @param queueName      queue the worker consumes from
 * @param status         liveness status: ACTIVE, STALE, or DEAD
 * @param lastHeartbeat  timestamp of the most recent heartbeat
 * @param maxConcurrency max tasks the worker holds concurrently
 */
public record WorkerResponse(
        String workerId,
        String queueName,
        String status,
        Instant lastHeartbeat,
        int maxConcurrency,
        List<String> tags
) {
    public static WorkerResponse from(Worker worker) {
        return new WorkerResponse(
                worker.id(),
                worker.queueName(),
                worker.status(),
                worker.lastHeartbeat(),
                worker.maxConcurrency(),
                worker.tags().values()
        );
    }
}
