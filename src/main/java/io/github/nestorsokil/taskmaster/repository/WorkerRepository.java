package io.github.nestorsokil.taskmaster.repository;

import io.github.nestorsokil.taskmaster.domain.Tags;
import io.github.nestorsokil.taskmaster.domain.Worker;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface WorkerRepository extends ListCrudRepository<Worker, String> {

        /**
         * Inserts a new worker or updates an existing one (idempotent registration).
         * Uses PostgreSQL {@code ON CONFLICT} to handle the upsert atomically.
         * Timestamps are set via {@code now()} to avoid JDBC Instant mapping issues.
         */
        @Modifying
        @Query("""
                        INSERT INTO workers (id, queue_name, max_concurrency, tags, registered_at, last_heartbeat, status)
                        VALUES (:id, :queueName, :maxConcurrency, :tags, now(), now(), 'ACTIVE')
                        ON CONFLICT (id) DO UPDATE
                           SET queue_name      = EXCLUDED.queue_name,
                               max_concurrency = EXCLUDED.max_concurrency,
                               tags            = EXCLUDED.tags,
                               last_heartbeat  = now(),
                               status          = 'ACTIVE'
        """)
        void upsert(@Param("id") String id,
                        @Param("queueName") String queueName,
                        @Param("maxConcurrency") int maxConcurrency,
                        @Param("tags") Tags tags);

        /**
         * Ensures a worker exists, inserting with defaults if new.
         * If already registered, only refreshes heartbeat and status — preserves
         * existing queue_name and max_concurrency so a claim never clobbers
         * values set by an explicit /register call.
         */
        @Modifying
        @Query("""
                        INSERT INTO workers (id, queue_name, max_concurrency, tags, registered_at, last_heartbeat, status)
                        VALUES (:id, :queueName, :maxConcurrency, ARRAY[]::TEXT[], now(), now(), 'ACTIVE')
                        ON CONFLICT (id) DO UPDATE
                           SET last_heartbeat = now(),
                               status         = 'ACTIVE'
                        """)
        void ensureExists(@Param("id") String id,
                        @Param("queueName") String queueName,
                        @Param("maxConcurrency") int maxConcurrency);

        /**
         * Updates last_heartbeat to now() for an existing worker.
         * Returns the number of rows affected (0 if worker not found).
         */
        @Modifying
        @Query("UPDATE workers SET last_heartbeat = now() WHERE id = :workerId")
        int updateHeartbeat(@Param("workerId") String workerId);

        /**
         * Returns all ACTIVE workers grouped by queue, used for queue stats
         * aggregation.
         */
        @Query("SELECT * FROM workers WHERE status = 'ACTIVE'")
        List<Worker> findActive();

        /**
         * Deletes a batch of DEAD workers whose {@code last_heartbeat} is older than
         * the retention threshold and that have no remaining task references (FK safe).
         * Returns the number of rows deleted.
         */
        @Modifying
        @Query("""
                        DELETE FROM workers
                         WHERE id IN (
                               SELECT w.id FROM workers w
                                WHERE w.status = 'DEAD'
                                  AND w.last_heartbeat < now() - CAST(:ttlSeconds || ' seconds' AS interval)
                                LIMIT :batchSize
                         )
                        """)
        int deleteExpiredDeadWorkers(@Param("ttlSeconds") long ttlSeconds,
                        @Param("batchSize") int batchSize);

        /**
         * Marks workers STALE whose last heartbeat is older than the stale threshold.
         * Only transitions ACTIVE → STALE (does not touch already-STALE or DEAD
         * workers).
         */
        @Modifying
        @Query("""
                        UPDATE workers
                           SET status = 'STALE'
                         WHERE status = 'ACTIVE'
                           AND last_heartbeat < now() - make_interval(secs => :thresholdSeconds)
                        """)
        int markStale(@Param("thresholdSeconds") long thresholdSeconds);

        /**
         * Marks workers DEAD whose last heartbeat is older than the dead threshold.
         * Transitions both ACTIVE and STALE workers (a worker can jump straight to DEAD
         * if the reaper hasn't run since it went silent).
         * Returns the IDs of workers just marked DEAD so their tasks can be requeued.
         */
        @Query("""
                        UPDATE workers
                           SET status = 'DEAD'
                         WHERE status IN ('ACTIVE', 'STALE')
                           AND last_heartbeat < now() - make_interval(secs => :thresholdSeconds)
                        RETURNING id
                        """)
        List<String> markDeadAndReturnIds(@Param("thresholdSeconds") long thresholdSeconds);
}
