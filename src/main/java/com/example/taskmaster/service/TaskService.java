package com.example.taskmaster.service;

import com.example.taskmaster.config.TaskmasterMetrics;
import com.example.taskmaster.config.TaskmasterProperties;
import com.example.taskmaster.domain.Tags;
import com.example.taskmaster.domain.Task;
import com.example.taskmaster.repository.TaskRepository;
import io.micrometer.observation.annotation.Observed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import lombok.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@Observed
@RequiredArgsConstructor
public class TaskService {

    private final TaskRepository taskRepository;
    private final TaskmasterMetrics metrics;
    private final TaskmasterProperties properties;

    /**
     * Persists a new task in PENDING state. The database supplies the UUID and timestamps.
     */
    public Task submit(String queueName, String payload, int priority, int maxAttempts, Instant deadline, Tags tags) {
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
                .build());
        metrics.taskSubmitted(queueName);
        log.info("Task submitted: id={}, queue={}, priority={}, tags={}", saved.id(), queueName, priority, tags.values());
        return saved;
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
        log.info("Task completed: id={}, queue={}, worker={}", taskId, task.queueName(), workerId);
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
        if ("DEAD".equals(task.status())) {
            metrics.taskDeadLettered(task.queueName(), "exhausted");
            metrics.recordEndToEndDuration(task.queueName(), Duration.between(task.createdAt(), task.finishedAt()));
            log.info("Task dead-lettered: id={}, queue={}, attempts={}", taskId, task.queueName(), task.attempts());
        } else {
            log.info("Task requeued for retry: id={}, queue={}, attempt={}/{}", taskId, task.queueName(), task.attempts(), task.maxAttempts());
        }
    }
}
