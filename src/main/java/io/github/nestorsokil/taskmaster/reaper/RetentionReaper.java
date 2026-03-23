package io.github.nestorsokil.taskmaster.reaper;

import io.github.nestorsokil.taskmaster.config.TaskmasterMetrics;
import io.github.nestorsokil.taskmaster.config.TaskmasterProperties;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically removes terminal tasks (DONE/DEAD) and dead workers that have
 * exceeded the configured retention TTL. Deletes in bounded batches to avoid
 * long transactions and lock contention.
 *
 * <p>Disabled when {@code taskmaster.retention.ttl} is zero or negative.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetentionReaper {

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;
    private final TaskmasterProperties properties;
    private final TaskmasterMetrics metrics;

    @Scheduled(fixedDelayString = "${taskmaster.retention.interval-ms}")
    public void reap() {
        var retention = properties.retention();
        if (retention.ttl().isZero() || retention.ttl().isNegative()) {
            return;
        }

        long ttlSeconds = retention.ttl().toSeconds();
        int batchSize = retention.batchSize();

        cleanTasks(ttlSeconds, batchSize);
        cleanWorkers(ttlSeconds, batchSize);
    }

    private void cleanTasks(long ttlSeconds, int batchSize) {
        int totalDeleted = 0;
        int deleted;
        do {
            deleted = taskRepository.deleteExpiredTerminalTasks(ttlSeconds, batchSize);
            totalDeleted += deleted;
        } while (deleted >= batchSize);

        if (totalDeleted > 0) {
            metrics.tasksCleaned(totalDeleted);
            log.info("Retention cleanup: deleted {} expired terminal task(s)", totalDeleted);
        }
    }

    private void cleanWorkers(long ttlSeconds, int batchSize) {
        int totalDeleted = 0;
        int deleted;
        do {
            deleted = workerRepository.deleteExpiredDeadWorkers(ttlSeconds, batchSize);
            totalDeleted += deleted;
        } while (deleted >= batchSize);

        if (totalDeleted > 0) {
            metrics.workersCleaned(totalDeleted);
            log.info("Retention cleanup: deleted {} expired dead worker(s)", totalDeleted);
        }
    }
}
