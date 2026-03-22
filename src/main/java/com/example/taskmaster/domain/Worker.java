package com.example.taskmaster.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

/**
 * Spring Data JDBC entity representing a row in the {@code workers} table.
 *
 * <p>Workers are external processes that register themselves on startup, send
 * periodic heartbeats, and poll for tasks. Taskmaster uses this record purely
 * for liveness tracking — no worker-side logic lives here.
 */
@Table("workers")
public record Worker(

        /**
         * Human-readable, worker-supplied identifier (e.g. {@code "email-worker-pod-3"}).
         * Must be unique across all workers. Used as the foreign key in {@code tasks.worker_id}.
         * Workers are responsible for choosing a stable, collision-free ID
         * (e.g. Kubernetes pod name or hostname + process ID).
         */
        @Id
        String id,

        /**
         * The queue this worker consumes from (e.g. {@code "email-queue"}).
         * A worker only claims tasks whose {@code queue_name} matches this value.
         * A single worker instance is bound to exactly one queue.
         */
        @Column("queue_name")
        String queueName,

        /**
         * Maximum number of tasks this worker is willing to hold concurrently.
         * Passed as {@code maxTasks} in the claim request so the worker never
         * receives more work than it can handle. Defaults to {@code 4}.
         */
        @Column("max_concurrency")
        int maxConcurrency,

        /**
         * Wall-clock time when the worker first registered (or last re-registered).
         * Set by the database default on upsert. Used for audit and observability only.
         */
        @Column("registered_at")
        Instant registeredAt,

        /**
         * Wall-clock time of the most recent heartbeat received from this worker.
         * Updated to {@code now()} on every {@code POST /workers/{workerId}/heartbeat} call.
         * The {@code HeartbeatReaper} compares this against configured thresholds to
         * transition workers through {@code ACTIVE → STALE → DEAD}.
         */
        @Column("last_heartbeat")
        Instant lastHeartbeat,

        /**
         * Capability tags this worker advertises. During claim, a task is only eligible
         * if its tags are a subset of this set. Empty means the worker can only claim
         * tasks with no tags.
         */
        Tags tags,

        /**
         * Liveness status of the worker. One of:
         * <ul>
         *   <li>{@code ACTIVE} — heartbeats arriving within the stale threshold (default 30 s)</li>
         *   <li>{@code STALE}  — no heartbeat for &gt; stale threshold but &lt; dead threshold</li>
         *   <li>{@code DEAD}   — no heartbeat for &gt; dead threshold (default 2 min);
         *                        all running tasks are requeued</li>
         * </ul>
         */
        String status

) {
    public Worker {
        if (tags == null) tags = Tags.EMPTY;
    }
}
