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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@Observed
@RequiredArgsConstructor
public class ClaimService {

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;
    private final TaskmasterMetrics metrics;

    private static final int DEFAULT_MAX_CONCURRENCY = 4;
    // How many candidates to fetch per maxTasks slot to allow greedy skip-ahead
    private static final int LOOK_AHEAD_MULTIPLIER = 4;

    /**
     * Atomically claims PENDING tasks for the given worker, respecting the worker's
     * complexity budget ({@code maxConcurrency - currentLoad}).
     *
     * <p>Tasks are selected greedily in priority/FIFO order: a task is included if its
     * complexity fits within the remaining budget; if not, it is skipped and lighter tasks
     * behind it are still considered. This avoids a single heavy task blocking all lighter
     * tasks behind it.
     *
     * <p>If the worker is not yet registered it is auto-registered with default concurrency.
     * The entire operation runs in a single transaction so the FOR UPDATE SKIP LOCKED in
     * {@code fetchClaimCandidates} is still held when {@code claimByIds} executes —
     * concurrent callers will never receive the same task.
     *
     * <p>Returns an empty list (not an error) when no tasks fit within the budget.
     */
    @Transactional
    public List<Task> claim(@NonNull String workerId, @NonNull String queueName, int maxTasks) {
        int capacity = workerRepository
                .ensureExists(workerId, queueName, DEFAULT_MAX_CONCURRENCY)
                .maxConcurrency();
        int currentLoad = taskRepository.getWorkerLoad(workerId);
        int remainingBudget = capacity - currentLoad;

        if (remainingBudget <= 0) {
            log.info("Claim skipped: worker={}, queue={}, load={}/{} (at capacity)", workerId, queueName, currentLoad, capacity);
            metrics.setWorkerLoad(workerId, queueName, currentLoad);
            return List.of();
        }

        // Fetch more candidates than maxTasks to allow greedy skipping over heavy tasks
        int limit = maxTasks * LOOK_AHEAD_MULTIPLIER;
        var candidates = taskRepository.fetchClaimCandidates(workerId, queueName, limit);

        // Greedy selection: walk candidates in priority/FIFO order, include if complexity fits
        var selectedIds = new ArrayList<UUID>(maxTasks);
        int accumulated = 0;
        for (var task : candidates) {
            if (selectedIds.size() >= maxTasks) {
                break;
            }
            if (accumulated + task.complexity() <= remainingBudget) {
                selectedIds.add(task.id());
                accumulated += task.complexity();
            }
        }

        if (selectedIds.isEmpty()) {
            log.info("Claim returned empty: worker={}, queue={}, budget={}, candidates={}", workerId, queueName, remainingBudget, candidates.size());
            metrics.setWorkerLoad(workerId, queueName, currentLoad);
            return List.of();
        }

        var claimed = taskRepository.claimByIds(workerId, selectedIds);
        int newLoad = currentLoad + accumulated;
        metrics.setWorkerLoad(workerId, queueName, newLoad);
        log.info("Tasks claimed: worker={}, queue={}, granted={}, load={}/{}", workerId, queueName, claimed.size(), newLoad, capacity);
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
