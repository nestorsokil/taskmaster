package com.example.taskmaster.reaper;

import com.example.taskmaster.config.TaskmasterMetrics;
import com.example.taskmaster.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Periodically dead-letters tasks whose submission deadline has passed.
 *
 * <p>Runs every 30 seconds. A task is dead-lettered when its {@code deadline}
 * column is set and the deadline has passed while the task is still PENDING
 * (i.e. it was never claimed in time).
 *
 * <p>Note: the {@code deadline} column is not in the current schema — this reaper
 * is a no-op until a {@code V2} migration adds it. Wired up now so the
 * scheduling infrastructure is in place.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadlineReaper {

    private final TaskRepository taskRepository;
    private final TaskmasterMetrics metrics;

    @Scheduled(fixedDelay = 30_000)
    @Transactional
    public void reap() {
        int count = taskRepository.deadlineExpired();
        if (count > 0) {
            metrics.taskDeadLetteredBatch("deadline", count);
            log.warn("Dead-lettered {} task(s) past their deadline", count);
        }
    }
}
