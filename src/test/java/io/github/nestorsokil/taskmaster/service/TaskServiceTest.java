package io.github.nestorsokil.taskmaster.service;

import io.github.nestorsokil.taskmaster.config.TaskmasterMetrics;
import io.github.nestorsokil.taskmaster.config.TaskmasterProperties;
import io.github.nestorsokil.taskmaster.domain.Tags;
import io.github.nestorsokil.taskmaster.domain.Task;
import io.github.nestorsokil.taskmaster.domain.Worker;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("null")
@ExtendWith(MockitoExtension.class)
class TaskServiceTest {

    @Mock TaskRepository taskRepository;
    @Mock WorkerRepository workerRepository;
    @Mock TaskmasterMetrics metrics;
    @Mock TaskmasterProperties properties;
    @Mock WebhookService webhookService;

    @InjectMocks TaskService taskService;

    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "worker-1";
    private static final UUID TASK_ID = UUID.randomUUID();

    private Task runningTask;

    @BeforeEach
    void setup() {
        lenient().when(properties.retry()).thenReturn(new TaskmasterProperties.Retry(5.0));

        Instant now = Instant.now();
        runningTask = Task.builder()
                .id(TASK_ID)
                .queueName(QUEUE)
                .status("RUNNING")
                .workerId(WORKER_ID)
                .priority(1)
                .complexity(1)
                .createdAt(now.minusSeconds(10))
                .claimedAt(now.minusSeconds(5))
                .maxAttempts(3)
                .attempts(1)
                .build();
    }

    // --- submit ---

    @Test
    void submit_complexityOne_skipsWorkerCapacityCheck() {
        when(taskRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        taskService.submit(QUEUE, "{}", 0, 3, null, Tags.EMPTY, 1, null);
        verify(workerRepository, never()).findActiveOnQueue(any());
    }

    @Test
    void submit_complexityHigherThanAllWorkers_stillAcceptsTask() {
        var smallWorker = new Worker(WORKER_ID, QUEUE, 2, Instant.now(), Instant.now(), Tags.EMPTY, "ACTIVE");
        when(workerRepository.findActiveOnQueue(QUEUE)).thenReturn(List.of(smallWorker));
        when(taskRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        // complexity=5 exceeds worker capacity=2 → warning logged but task is accepted
        assertDoesNotThrow(() -> taskService.submit(QUEUE, "{}", 0, 3, null, Tags.EMPTY, 5, null));
        verify(taskRepository).save(any());
    }

    @Test
    void submit_noActiveWorkers_doesNotWarnAndAcceptsTask() {
        // Empty list → the warn condition is skipped (capable worker may register later)
        when(workerRepository.findActiveOnQueue(QUEUE)).thenReturn(List.of());
        when(taskRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        assertDoesNotThrow(() -> taskService.submit(QUEUE, "{}", 0, 3, null, Tags.EMPTY, 5, null));
    }

    @Test
    void submit_recordsSubmittedMetric() {
        when(taskRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        taskService.submit(QUEUE, "{}", 0, 3, null, Tags.EMPTY, 1, null);
        verify(metrics).taskSubmitted(QUEUE);
    }

    // --- complete ---

    @Test
    void complete_wrongWorker_throws409() {
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(runningTask));
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.complete(TASK_ID, "other-worker", "ok"));
        assertEquals(409, ex.getStatusCode().value());
        verify(taskRepository, never()).completeTask(any(), any(), any());
    }

    @Test
    void complete_taskNotRunning_throws409() {
        Task pending = runningTask.toBuilder().status("PENDING").workerId(WORKER_ID).build();
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(pending));
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.complete(TASK_ID, WORKER_ID, "ok"));
        assertEquals(409, ex.getStatusCode().value());
    }

    @Test
    void complete_concurrentCompletion_silentNoOp() {
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(runningTask));
        when(taskRepository.completeTask(TASK_ID, WORKER_ID, "ok")).thenReturn(0);
        assertDoesNotThrow(() -> taskService.complete(TASK_ID, WORKER_ID, "ok"));
        verify(metrics, never()).taskCompleted(any());
        verify(webhookService, never()).deliverIfConfigured(any());
    }

    @Test
    void complete_success_recordsMetricsAndFiresWebhook() {
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(runningTask));
        when(taskRepository.completeTask(TASK_ID, WORKER_ID, "done")).thenReturn(1);
        when(taskRepository.getWorkerLoad(WORKER_ID)).thenReturn(0);
        taskService.complete(TASK_ID, WORKER_ID, "done");
        verify(metrics).taskCompleted(QUEUE);
        verify(metrics).setWorkerLoad(eq(WORKER_ID), eq(QUEUE), anyInt());
        verify(webhookService).deliverIfConfigured(any(Task.class));
    }

    // --- fail ---

    @Test
    void fail_taskNotFound_throws404() {
        when(taskRepository.failTask(eq(TASK_ID), eq(WORKER_ID), any(), anyDouble())).thenReturn(List.of());
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.empty());
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.fail(TASK_ID, WORKER_ID, "err"));
        assertEquals(404, ex.getStatusCode().value());
    }

    @Test
    void fail_notOwner_throws409() {
        when(taskRepository.failTask(eq(TASK_ID), eq(WORKER_ID), any(), anyDouble())).thenReturn(List.of());
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(runningTask));
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.fail(TASK_ID, WORKER_ID, "err"));
        assertEquals(409, ex.getStatusCode().value());
    }

    @Test
    void fail_retryable_requeuesWithoutWebhookOrDeadLetterMetric() {
        Task requeued = runningTask.toBuilder().status("PENDING").finishedAt(Instant.now()).build();
        when(taskRepository.failTask(eq(TASK_ID), eq(WORKER_ID), any(), anyDouble())).thenReturn(List.of(requeued));
        when(taskRepository.getWorkerLoad(WORKER_ID)).thenReturn(0);
        taskService.fail(TASK_ID, WORKER_ID, "transient error");
        verify(metrics, never()).taskDeadLettered(any(), any());
        verify(webhookService, never()).deliverIfConfigured(any());
        verify(metrics).taskFailed(QUEUE);
    }

    @Test
    void fail_exhausted_deadLettersAndFiresWebhook() {
        Task dead = runningTask.toBuilder().status("DEAD").finishedAt(Instant.now()).build();
        when(taskRepository.failTask(eq(TASK_ID), eq(WORKER_ID), any(), anyDouble())).thenReturn(List.of(dead));
        when(taskRepository.getWorkerLoad(WORKER_ID)).thenReturn(0);
        taskService.fail(TASK_ID, WORKER_ID, "fatal");
        verify(metrics).taskDeadLettered(QUEUE, "exhausted");
        verify(webhookService).deliverIfConfigured(dead);
    }

    // --- replay ---

    @Test
    void replay_taskNotFound_throws404() {
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.empty());
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.replay(TASK_ID, null, null));
        assertEquals(404, ex.getStatusCode().value());
    }

    @Test
    void replay_taskNotDead_throws409() {
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(runningTask)); // RUNNING, not DEAD
        when(taskRepository.replayTask(eq(TASK_ID), anyInt(), any())).thenReturn(List.of());
        var ex = assertThrows(ResponseStatusException.class,
                () -> taskService.replay(TASK_ID, null, null));
        assertEquals(409, ex.getStatusCode().value());
    }

    @Test
    void replay_usesMaxAttemptsOverride() {
        Task dead = runningTask.toBuilder().status("DEAD").build();
        Task replayed = dead.toBuilder().status("PENDING").build();
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(dead));
        when(taskRepository.replayTask(TASK_ID, 10, null)).thenReturn(List.of(replayed));
        Task result = taskService.replay(TASK_ID, 10, null);
        assertEquals(replayed.id(), result.id());
        verify(metrics).tasksReplayed(QUEUE, 1);
    }

    @Test
    void replay_noOverride_usesTaskOriginalMaxAttempts() {
        Task dead = runningTask.toBuilder().status("DEAD").maxAttempts(5).build();
        Task replayed = dead.toBuilder().status("PENDING").build();
        when(taskRepository.findById(TASK_ID)).thenReturn(Optional.of(dead));
        when(taskRepository.replayTask(TASK_ID, 5, null)).thenReturn(List.of(replayed));
        taskService.replay(TASK_ID, null, null);
        verify(taskRepository).replayTask(TASK_ID, 5, null);
    }

    // --- bulkReplay ---

    @Test
    void bulkReplay_returnsCountAndRecordsMetric() {
        when(taskRepository.bulkReplayTasks(eq(QUEUE), any(), eq(0))).thenReturn(7);
        int count = taskService.bulkReplay(QUEUE, null, null);
        assertEquals(7, count);
        verify(metrics).tasksReplayed(QUEUE, 7);
    }

    @Test
    void bulkReplay_nullOverride_passesZeroToRepository() {
        // 0 signals "keep original max_attempts" in the SQL
        taskService.bulkReplay(QUEUE, null, null);
        verify(taskRepository).bulkReplayTasks(QUEUE, null, 0);
    }

    @Test
    void bulkReplay_withMaxAttemptsOverride_passesItThrough() {
        when(taskRepository.bulkReplayTasks(eq(QUEUE), any(), eq(5))).thenReturn(3);
        int count = taskService.bulkReplay(QUEUE, null, 5);
        assertEquals(3, count);
        verify(taskRepository).bulkReplayTasks(QUEUE, null, 5);
    }
}
