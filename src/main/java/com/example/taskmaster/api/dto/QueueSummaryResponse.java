package com.example.taskmaster.api.dto;

import com.example.taskmaster.service.ObservabilityService.QueueSummary;

/**
 * Response body for items in {@code GET /queues/v1}.
 */
public record QueueSummaryResponse(
        String queueName,
        long pending,
        long running,
        long failed,
        long dead,
        long activeWorkers
) {
    public static QueueSummaryResponse from(QueueSummary summary) {
        return new QueueSummaryResponse(
                summary.queueName(),
                summary.pending(),
                summary.running(),
                summary.failed(),
                summary.dead(),
                summary.activeWorkers()
        );
    }
}
