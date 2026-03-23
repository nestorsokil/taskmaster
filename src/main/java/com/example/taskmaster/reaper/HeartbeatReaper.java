package com.example.taskmaster.reaper;

import com.example.taskmaster.config.TaskmasterMetrics;
import com.example.taskmaster.config.TaskmasterProperties;
import com.example.taskmaster.repository.TaskRepository;
import com.example.taskmaster.repository.WorkerRepository;
import com.example.taskmaster.service.WebhookService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Periodically sweeps for silent workers and requeues their abandoned tasks.
 *
 * <p>Runs every {@code taskmaster.reaper.interval-ms} (default 15 s).
 * All three steps — mark stale, mark dead, requeue tasks — are wrapped in a single
 * transaction so the database never sees an inconsistent intermediate state
 * (e.g. a worker marked DEAD whose tasks are still RUNNING).
 *
 * <p>Emits a {@code tasks.requeued} Micrometer counter tagged with
 * {@code reason=worker_dead} for each task returned to the PENDING queue.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HeartbeatReaper {

    private final WorkerRepository workerRepository;
    private final TaskRepository taskRepository;
    private final TaskmasterProperties properties;
    private final TaskmasterMetrics metrics;
    private final WebhookService webhookService;

    @Scheduled(fixedDelayString = "${taskmaster.reaper.interval-ms}")
    @Transactional
    public void reap() {
        long staleSeconds = properties.heartbeat().staleThresholdSeconds();
        long deadSeconds  = properties.heartbeat().deadThresholdSeconds();

        // Step 1: ACTIVE → STALE
        int staled = workerRepository.markStale(staleSeconds);
        if (staled > 0) {
            log.warn("Marked {} worker(s) STALE", staled);
        }

        // Step 2: ACTIVE/STALE → DEAD; get their IDs to requeue tasks
        List<String> deadWorkerIds = workerRepository.markDeadAndReturnIds(deadSeconds);
        if (deadWorkerIds.isEmpty()) {
            return;
        }
        log.warn("Marked {} worker(s) DEAD: {}", deadWorkerIds.size(), deadWorkerIds);
        metrics.workersDied(deadWorkerIds.size());

        // Step 3: requeue RUNNING tasks owned by dead workers (or dead-letter if exhausted)
        var affected = taskRepository.requeueOrMarkDeadFromDeadWorkers(deadWorkerIds);

        int requeueCount = (int) affected.stream().filter(t -> "PENDING".equals(t.status())).count();
        int deadLetterCount = (int) affected.stream().filter(t -> "DEAD".equals(t.status())).count();
        metrics.tasksRequeued("worker_dead", requeueCount);
        metrics.taskDeadLetteredBatch("worker_dead", deadLetterCount);
        if (requeueCount > 0) {
            log.info("Requeued {} task(s) from dead workers", requeueCount);
        }

        affected.stream()
                .filter(t -> "DEAD".equals(t.status()))
                .forEach(webhookService::deliverIfConfigured);
    }
}
