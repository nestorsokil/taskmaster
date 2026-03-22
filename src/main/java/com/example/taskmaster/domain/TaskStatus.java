package com.example.taskmaster.domain;

import java.time.Instant;

/**
 * Represents the lifecycle state of a task.
 *
 * <p>State transitions:
 * <pre>
 *   PENDING → RUNNING → DONE
 *                     ↘ PENDING  (if retries remain, with backoff)
 *                     ↘ DEAD     (max attempts exhausted)
 * </pre>
 *
 * <p>Stored as a plain VARCHAR in the database ({@code PENDING}, {@code RUNNING},
 * {@code DONE}, {@code DEAD}). Conversion between the string column value and
 * this sealed interface is handled by a Spring Data JDBC converter.
 */
public sealed interface TaskStatus permits
        TaskStatus.Pending,
        TaskStatus.Running,
        TaskStatus.Done,
        TaskStatus.Dead {

    /**
     * The task has been submitted and is waiting to be claimed by a worker.
     * A task re-enters this state after a failed attempt when retries remain,
     * with {@code next_attempt_at} set to enforce exponential backoff.
     */
    record Pending() implements TaskStatus {}

    /**
     * The task has been atomically claimed by a worker and is actively being processed.
     *
     * @param workerId  the ID of the worker that claimed this task (matches {@code workers.id})
     * @param claimedAt the moment the worker claimed the task; used to detect stuck tasks
     *                  if the worker dies without reporting completion or failure
     */
    record Running(String workerId, Instant claimedAt) implements TaskStatus {}

    /**
     * The task finished successfully. Terminal state.
     *
     * @param result an opaque string returned by the worker describing the outcome
     *               (e.g. {@code "sent:msg-id-123"}); stored in {@code tasks.result}
     */
    record Done(String result) implements TaskStatus {}

    /**
     * The task exhausted all allowed attempts and will not be retried. Terminal state.
     * Operators must intervene manually (e.g. re-submit or investigate).
     *
     * @param lastError the error message from the final failed attempt
     */
    record Dead(String lastError) implements TaskStatus {}
}
