package io.github.nestorsokil.taskmaster.repository;

import io.github.nestorsokil.taskmaster.domain.Task;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface TaskRepository extends ListCrudRepository<Task, UUID> {

        /**
         * Atomically claims up to {@code maxTasks} PENDING tasks for a worker using
         * FOR UPDATE SKIP LOCKED — concurrent claim requests never block each other
         * and never double-claim the same task.
         *
         * <p>
         * The UPDATE increments {@code attempts} in the same statement so the
         * worker always sees the correct attempt count without a separate read.
         */
        @Query("""
                        UPDATE tasks
                           SET status     = 'RUNNING',
                               worker_id  = :workerId,
                               claimed_at = now(),
                               attempts   = attempts + 1
                         WHERE id IN (
                               SELECT id FROM tasks
                                WHERE queue_name = :queueName
                                  AND status     = 'PENDING'
                                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                                  AND tags <@ (SELECT tags FROM workers WHERE id = :workerId)
                                ORDER BY priority DESC, created_at ASC
                                LIMIT :maxTasks
                                FOR UPDATE SKIP LOCKED
                         )
                        RETURNING *
                        """)
        List<Task> claimTasks(@Param("workerId") String workerId,
                        @Param("queueName") String queueName,
                        @Param("maxTasks") int maxTasks);

        /**
         * Marks a task DONE, records the worker-supplied result, and stamps
         * finished_at.
         * Only succeeds if the task is currently owned by the given worker (status =
         * RUNNING
         * AND worker_id matches), preventing a stale worker from overwriting a
         * re-claimed task.
         */
        @Modifying
        @Query("""
                        UPDATE tasks
                           SET status      = 'DONE',
                               result      = :result,
                               finished_at = now(),
                               worker_id   = NULL
                         WHERE id        = :taskId
                           AND worker_id = :workerId
                           AND status    = 'RUNNING'
                        """)
        int completeTask(@Param("taskId") UUID taskId,
                        @Param("workerId") String workerId,
                        @Param("result") String result);

        /**
         * Atomically records a task failure and transitions it to the correct next
         * state:
         * <ul>
         * <li>{@code DEAD} if attempts are exhausted
         * ({@code attempts >= max_attempts})</li>
         * <li>{@code PENDING} with exponential backoff otherwise</li>
         * </ul>
         *
         * <p>
         * Returns the updated row so the caller can emit the right metrics,
         * or an empty list if the task is not RUNNING or not owned by the given worker.
         */
        @Query("""
                        UPDATE tasks
                           SET status          = CASE
                                                   WHEN attempts >= max_attempts THEN 'DEAD'
                                                   ELSE 'PENDING'
                                                 END,
                               last_error      = :error,
                               worker_id       = NULL,
                               finished_at     = now(),
                               next_attempt_at = CASE
                                                   WHEN attempts >= max_attempts THEN NULL
                                                   ELSE now() + least(
                                                            make_interval(secs => power(2, attempts) * :baseDelay),
                                                            interval '5 minutes'
                                                        )
                                                 END
                         WHERE id        = :taskId
                           AND worker_id = :workerId
                           AND status    = 'RUNNING'
                        RETURNING *
                        """)
        List<Task> failTask(@Param("taskId") UUID taskId,
                        @Param("workerId") String workerId,
                        @Param("error") String error,
                        @Param("baseDelay") double baseDelay);

        /**
         * Requeues all RUNNING tasks owned by dead workers.
         * If a task has exhausted its attempts it is moved to DEAD instead of PENDING.
         * Called by the HeartbeatReaper after marking workers DEAD.
         * Returns affected tasks so callers can compute metrics and fire webhooks.
         */
        @Query("""
                        UPDATE tasks
                           SET status          = CASE
                                                   WHEN attempts >= max_attempts THEN 'DEAD'
                                                   ELSE 'PENDING'
                                                 END,
                               worker_id       = NULL,
                               claimed_at      = NULL
                         WHERE status    = 'RUNNING'
                           AND worker_id IN (:workerIds)
                        RETURNING *
                        """)
        List<Task> requeueOrMarkDeadFromDeadWorkers(@Param("workerIds") List<String> workerIds);

        /**
         * Returns the number of tasks that will be dead-lettered (attempts exhausted)
         * from the dead workers list. Used for the {@code tasks.dead_lettered} counter.
         */
        @Query("""
                        SELECT COUNT(*) FROM tasks
                         WHERE status    = 'RUNNING'
                           AND worker_id IN (:workerIds)
                           AND attempts >= max_attempts
                        """)
        int countDeadLetterable(@Param("workerIds") List<String> workerIds);

        /**
         * Returns the number of tasks requeued (status set to PENDING) from the dead
         * workers list.
         * Used for emitting the {@code tasks.requeued} Micrometer counter.
         */
        @Query("""
                        SELECT COUNT(*) FROM tasks
                         WHERE status    = 'RUNNING'
                           AND worker_id IN (:workerIds)
                           AND attempts < max_attempts
                        """)
        int countRequeuable(@Param("workerIds") List<String> workerIds);

        /**
         * Filtered task list for the observability endpoint ({@code GET /tasks}).
         * Both parameters are optional — passing {@code null} omits the filter.
         */
        @Query("""
                        SELECT * FROM tasks
                         WHERE (CAST(:queueName AS TEXT) IS NULL OR queue_name = :queueName)
                           AND (CAST(:status    AS TEXT) IS NULL OR status     = :status)
                         ORDER BY created_at DESC
                         LIMIT :limit
                        """)
        List<Task> findFiltered(
                @Param("queueName") String queueName,
                @Param("status") String status,
                @Param("limit") int limit);

        /**
         * Dead-letters all PENDING tasks whose deadline has passed.
         * Returns the affected tasks so callers can fire webhooks for those with callback URLs.
         * No-op when no tasks have an expired deadline.
         */
        @Query("""
                        UPDATE tasks
                           SET status      = 'DEAD',
                               finished_at = now()
                         WHERE status   = 'PENDING'
                           AND deadline IS NOT NULL
                           AND deadline  < now()
                        RETURNING *
                        """)
        List<Task> deadlineExpired();

        /**
         * Deletes a batch of terminal tasks (DONE or DEAD) whose {@code finished_at}
         * is older than the retention threshold. Returns the number of rows deleted.
         *
         * <p>
         * Uses a sub-select with {@code LIMIT} so each call is bounded, avoiding
         * long-running transactions on large backlogs.
         */
        @Modifying
        @Query("""
                        DELETE FROM tasks
                         WHERE id IN (
                               SELECT id FROM tasks
                                WHERE status IN ('DONE', 'DEAD')
                                  AND finished_at < now() - CAST(:ttlSeconds || ' seconds' AS interval)
                                LIMIT :batchSize
                         )
                        """)
        int deleteExpiredTerminalTasks(
                        @Param("ttlSeconds") long ttlSeconds,
                        @Param("batchSize") int batchSize);

        /**
         * Aggregated per-queue statistics used by {@code GET /queues}.
         * Returns one row per queue_name with counts for each status.
         */
        @Query("""
                        SELECT queue_name,
                               COUNT(*) FILTER (WHERE status = 'PENDING')                            AS pending,
                               COUNT(*) FILTER (WHERE status = 'RUNNING')                            AS running,
                               COUNT(*) FILTER (WHERE status = 'PENDING' AND last_error IS NOT NULL) AS failed,
                               COUNT(*) FILTER (WHERE status = 'DEAD')                               AS dead
                          FROM tasks
                         GROUP BY queue_name
                        """)
        List<QueueStats> getQueueStats();

        /**
         * Projection returned by {@link #getQueueStats()}.
         * Column names must match exactly (snake_case) for Spring Data JDBC record
         * mapping.
         */
        record QueueStats(
                        String queue_name,
                        long pending,
                        long running,
                        long failed,
                        long dead) {
        }
}
