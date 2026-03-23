package io.github.nestorsokil.taskmaster.service;

import io.github.nestorsokil.taskmaster.config.TaskmasterMetrics;
import io.github.nestorsokil.taskmaster.domain.Task;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import io.micrometer.observation.annotation.Observed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

@Slf4j
@Service
@Observed
@RequiredArgsConstructor
public class ClaimService {

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;
    private final TaskmasterMetrics metrics;

    private static final int DEFAULT_MAX_CONCURRENCY = 4;

    /**
     * Atomically claims up to {@code maxTasks} PENDING tasks for the given worker.
     *
     * <p>If the worker is not yet registered, it is auto-registered with default
     * concurrency. If already registered, the heartbeat is refreshed.
     *
     * <p>The entire operation runs in a single transaction so the
     * {@code FOR UPDATE SKIP LOCKED} in the SELECT and the UPDATE are atomic —
     * concurrent callers will never receive the same task.
     *
     * <p>Returns an empty list (not an error) when no tasks are available.
     */
    @Transactional
    public List<Task> claim(@NonNull String workerId, @NonNull String queueName, int maxTasks) {
        workerRepository.ensureExists(workerId, queueName, DEFAULT_MAX_CONCURRENCY);
        var claimed = taskRepository.claimTasks(workerId, queueName, maxTasks);
        log.info("Tasks claimed: worker={}, queue={}, requested={}, granted={}", workerId, queueName, maxTasks, claimed.size());
        metrics.tasksClaimed(queueName, claimed.size());        
        return claimed.stream()
                .peek(task -> metrics.recordQueueWaitTime(queueName, Duration.between(task.createdAt(), task.claimedAt())))
                // RETURNING * does not preserve the subquery's ORDER BY, so re-sort here
                .sorted(Comparator
                    .comparingInt(Task::priority)
                    .reversed()
                    .thenComparing(Task::createdAt))
                .toList();
    }
}
