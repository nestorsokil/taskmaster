package io.github.nestorsokil.taskmaster.service;

import io.github.nestorsokil.taskmaster.config.TaskmasterMetrics;
import io.github.nestorsokil.taskmaster.config.TaskmasterProperties;
import io.github.nestorsokil.taskmaster.domain.Tags;
import io.github.nestorsokil.taskmaster.domain.Task;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import io.micrometer.observation.annotation.Observed;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@Observed
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;
    private final TaskmasterMetrics metrics;
    private final TaskmasterProperties properties;
    private final WebhookService webhookService;

    /**
     * Persists a new task in PENDING state. The database supplies the UUID and timestamps.
     *
     * <p>If {@code complexity} exceeds the capacity of every currently-registered active worker
     * on the queue, a warning is logged (the task is still accepted — a capable worker may
     * register later).
     */
    public Task submit(String queueName, String payload, int priority, int maxAttempts,
                       Instant deadline, Tags tags, int complexity, String callbackUrl) {
        warnIfNoCapableWorker(queueName, complexity);
        @SuppressWarnings("null")
        var saved = taskRepository.save(Task.builder()
                .queueName(queueName)
                .payload(payload)
                .priority(priority)
                .status("PENDING")
                .maxAttempts(maxAttempts)
                .createdAt(Instant.now())
                .deadline(deadline)
                .tags(tags)
                .complexity(complexity)
                .callbackUrl(callbackUrl)
                .build());
        metrics.taskSubmitted(queueName);
        log.info("Task submitted: id={}, queue={}, priority={}, complexity={}, tags={}",
                saved.id(), queueName, priority, complexity, tags.values());
        return saved;
    }

    private void warnIfNoCapableWorker(String queueName, int complexity) {
        if (complexity <= 1) {
            return; // default complexity always fits default capacity
        }
        var activeWorkers = workerRepository.findActiveOnQueue(queueName);
        if (!activeWorkers.isEmpty() && activeWorkers.stream().noneMatch(w -> w.maxConcurrency() >= complexity)) {
            log.warn("Task submitted with complexity={} but no active worker on queue='{}' has sufficient capacity; task may never be claimed",
                    complexity, queueName);
        }
    }

    public Task getTask(@NonNull UUID taskId) {
        return taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Task not found: " + taskId));
    }

    /**
     * Marks a task DONE. Throws 404 if the task doesn't exist, 409 if the caller
     * is not the current owner (prevents a stale worker from overwriting a re-claimed task).
     */
    public void complete(UUID taskId, String workerId, String result) {
        Task task = getTask(taskId);
        if (!workerId.equals(task.workerId()) || !"RUNNING".equals(task.status())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Task " + taskId + " is not owned by worker " + workerId);
        }
        if (taskRepository.completeTask(taskId, workerId, result) == 0) {
            return; // already completed by a concurrent call
        }
        Instant now = Instant.now();
        metrics.taskCompleted(task.queueName());
        metrics.recordExecutionDuration(task.queueName(), Duration.between(task.claimedAt(), now));
        metrics.recordEndToEndDuration(task.queueName(), Duration.between(task.createdAt(), now));
        metrics.setWorkerLoad(workerId, task.queueName(), taskRepository.getWorkerLoad(workerId));
        log.info("Task completed: id={}, queue={}, worker={}", taskId, task.queueName(), workerId);
        webhookService.deliverIfConfigured(task.toBuilder().status("DONE").result(result).build());
    }

    /**
     * Records a task failure and atomically transitions it to the correct next state:
     * DEAD if all attempts are exhausted, or PENDING with exponential backoff otherwise.
     * Throws 404 if the task doesn't exist, 409 if the caller is not the current owner.
     */
    public void fail(UUID taskId, String workerId, String error) {
        var updated = taskRepository.failTask(taskId, workerId, error, properties.retry().baseDelaySeconds());
        if (updated.isEmpty()) {
            getTask(taskId); // throws 404 if missing
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Task " + taskId + " is not owned by worker " + workerId);
        }
        Task task = updated.getFirst();
        metrics.taskFailed(task.queueName());
        metrics.recordExecutionDuration(task.queueName(), Duration.between(task.claimedAt(), task.finishedAt()));
        metrics.setWorkerLoad(workerId, task.queueName(), taskRepository.getWorkerLoad(workerId));
        if ("DEAD".equals(task.status())) {
            metrics.taskDeadLettered(task.queueName(), "exhausted");
            metrics.recordEndToEndDuration(task.queueName(), Duration.between(task.createdAt(), task.finishedAt()));
            log.info("Task dead-lettered: id={}, queue={}, attempts={}", taskId, task.queueName(), task.attempts());
            webhookService.deliverIfConfigured(task);
        } else {
            log.info("Task requeued for retry: id={}, queue={}, attempt={}/{}", taskId, task.queueName(), task.attempts(), task.maxAttempts());
        }
    }

    /**
     * Replays a single DEAD task back to PENDING. Resets attempts to 0 and
     * max_attempts to its original value (or the provided override).
     * Throws 404 if the task doesn't exist, 409 if the task is not DEAD.
     */
    public Task replay(UUID taskId, Integer maxAttemptsOverride, Instant deadlineOverride) {
        Task task = getTask(taskId);
        int maxAttempts = maxAttemptsOverride != null ? maxAttemptsOverride : task.maxAttempts();
        var updated = taskRepository.replayTask(taskId, maxAttempts, deadlineOverride);
        if (updated.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Task " + taskId + " is not in DEAD status (current: " + task.status() + ")");
        }
        Task replayed = updated.getFirst();
        metrics.tasksReplayed(replayed.queueName(), 1);
        log.info("Task replayed: id={}, queue={}, maxAttempts={}", taskId, replayed.queueName(), maxAttempts);
        return replayed;
    }

    /**
     * Replays all DEAD tasks in a queue back to PENDING. Optionally filters by age.
     * Returns the count of tasks replayed.
     */
    public int bulkReplay(String queueName, Instant deadSince, Integer maxAttemptsOverride) {
        int maxAttempts = maxAttemptsOverride != null ? maxAttemptsOverride : 0; // 0 signals "keep original"
        int count = taskRepository.bulkReplayTasks(queueName, deadSince, maxAttempts);
        metrics.tasksReplayed(queueName, count);
        log.info("Bulk replay: queue={}, count={}, deadSince={}", queueName, count, deadSince);
        return count;
    }
}
