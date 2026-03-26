package io.github.nestorsokil.taskmaster.service;

import io.github.nestorsokil.taskmaster.domain.Tags;
import io.github.nestorsokil.taskmaster.domain.Task;
import io.github.nestorsokil.taskmaster.domain.Worker;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import io.github.nestorsokil.taskmaster.config.TaskmasterMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ClaimServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private WorkerRepository workerRepository;

    @Mock
    private TaskmasterMetrics metrics;

    @InjectMocks
    private ClaimService claimService;

    private final String workerId = "worker-1";
    private final String queue = "email-queue";
    private final int maxTasks = 2;

    @BeforeEach
    void setup() {
        // Default behavior for ensureExists
        when(workerRepository.ensureExists(anyString(), anyString(), anyInt()))
                .thenReturn(new Worker(workerId, queue, 4, Instant.now(), Instant.now(), Tags.EMPTY, "ACTIVE"));
    }

    @Test
    void claim_workerAtCapacity_returnsEmpty() {
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(4); // equal to maxConcurrency
        List<Task> result = claimService.claim(workerId, queue, maxTasks);
        assertTrue(result.isEmpty());
        verify(metrics).setWorkerLoad(eq(workerId), eq(queue), eq(4));
        verify(taskRepository, never()).fetchClaimCandidates(anyString(), anyString(), anyInt());
    }

    @Test
    void claim_noCandidatesReturnsEmpty() {
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(0);
        when(taskRepository.fetchClaimCandidates(workerId, queue, maxTasks * 4)).thenReturn(List.of());
        List<Task> result = claimService.claim(workerId, queue, maxTasks);
        assertTrue(result.isEmpty());
        verify(metrics).setWorkerLoad(eq(workerId), eq(queue), eq(0));
        verify(taskRepository, never()).claimByIds(anyString(), anyList());
    }

    @Test
    void claim_greedySelection_skipsHeavy() {
        // capacity 4, current load 0
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(0);
        Instant now = Instant.now();
        Task heavy = Task.builder()
                .id(UUID.randomUUID())
                .queueName(queue)
                .priority(1)
                .status("PENDING")
                .complexity(5)
                .createdAt(now)
                .claimedAt(now)
                .build();
        Task light = Task.builder()
                .id(UUID.randomUUID())
                .queueName(queue)
                .priority(1)
                .status("PENDING")
                .complexity(1)
                .createdAt(now.plusSeconds(1))
                .claimedAt(now)
                .build();
        when(taskRepository.fetchClaimCandidates(workerId, queue, maxTasks * 4)).thenReturn(List.of(heavy, light));
        // claimByIds should return the selected task(s)
        when(taskRepository.claimByIds(eq(workerId), anyList())).thenReturn(List.of(light));
        List<Task> result = claimService.claim(workerId, queue, maxTasks);
        assertEquals(1, result.size());
        assertEquals(light.id(), result.get(0).id());
        // Verify that the selected list contained the light task id
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<UUID>> captor = ArgumentCaptor.forClass(List.class);
        verify(taskRepository).claimByIds(eq(workerId), captor.capture());
        assertTrue(captor.getValue().contains(light.id()));
        assertFalse(captor.getValue().contains(heavy.id()));
    }

    @Test
    void claim_successfulSelection_returnsSortedByPriorityThenCreatedAt() {
        // Use maxTasks=3 so all three candidates are selected (budget=4, each complexity=1)
        int localMaxTasks = 3;
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(0);
        Instant now = Instant.now();
        Task t1 = Task.builder().id(UUID.randomUUID()).queueName(queue).priority(5).status("PENDING").complexity(1)
                .createdAt(now).claimedAt(now).build();
        Task t2 = Task.builder().id(UUID.randomUUID()).queueName(queue).priority(3).status("PENDING").complexity(1)
                .createdAt(now.plusSeconds(1)).claimedAt(now).build();
        Task t3 = Task.builder().id(UUID.randomUUID()).queueName(queue).priority(5).status("PENDING").complexity(1)
                .createdAt(now.plusSeconds(2)).claimedAt(now).build();
        // claimByIds returns in DB order (not priority order) to prove the re-sort is applied
        when(taskRepository.fetchClaimCandidates(workerId, queue, localMaxTasks * 4)).thenReturn(List.of(t1, t2, t3));
        when(taskRepository.claimByIds(eq(workerId), anyList())).thenReturn(List.of(t2, t3, t1));
        List<Task> result = claimService.claim(workerId, queue, localMaxTasks);
        assertEquals(3, result.size());
        // Expected: priority DESC then createdAt ASC → t1(p=5,t=0), t3(p=5,t=2), t2(p=3,t=1)
        assertEquals(t1.id(), result.get(0).id());
        assertEquals(t3.id(), result.get(1).id());
        assertEquals(t2.id(), result.get(2).id());
    }

    @Test
    void claim_partialBudget_claimsTasksThatFit() {
        // Worker has load=2, capacity=4 → remaining budget=2; only tasks with complexity<=2 fit
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(2);
        Instant now = Instant.now();
        Task fits = task(now, 1, 1);
        Task tooHeavy = task(now.plusSeconds(1), 1, 3);
        when(taskRepository.fetchClaimCandidates(workerId, queue, maxTasks * 4)).thenReturn(List.of(tooHeavy, fits));
        when(taskRepository.claimByIds(eq(workerId), anyList())).thenReturn(List.of(fits));
        List<Task> result = claimService.claim(workerId, queue, maxTasks);
        assertEquals(1, result.size());
        assertEquals(fits.id(), result.getFirst().id());
    }

    @Test
    void claim_maxTasksCapRespectsLimit() {
        // Budget=4 allows 4 tasks of complexity=1, but maxTasks=2 must cap the result
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(0);
        Instant now = Instant.now();
        List<Task> candidates = List.of(
                task(now, 1, 1), task(now.plusSeconds(1), 1, 1),
                task(now.plusSeconds(2), 1, 1), task(now.plusSeconds(3), 1, 1));
        when(taskRepository.fetchClaimCandidates(workerId, queue, maxTasks * 4)).thenReturn(candidates);
        when(taskRepository.claimByIds(eq(workerId), anyList())).thenReturn(candidates.subList(0, 2));
        claimService.claim(workerId, queue, maxTasks);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<UUID>> captor = ArgumentCaptor.forClass(List.class);
        verify(taskRepository).claimByIds(eq(workerId), captor.capture());
        assertEquals(2, captor.getValue().size());
    }

    @Test
    void claim_allCandidatesTooHeavy_returnsEmpty() {
        // Budget=2 but every candidate has complexity=3 → nothing fits
        when(taskRepository.getWorkerLoad(workerId)).thenReturn(2); // capacity=4, budget=2
        Instant now = Instant.now();
        when(taskRepository.fetchClaimCandidates(workerId, queue, maxTasks * 4))
                .thenReturn(List.of(task(now, 1, 3), task(now.plusSeconds(1), 1, 3)));
        List<Task> result = claimService.claim(workerId, queue, maxTasks);
        assertTrue(result.isEmpty());
        verify(taskRepository, never()).claimByIds(anyString(), anyList());
    }

    // --- helpers ---

    private Task task(Instant createdAt, int priority, int complexity) {
        return Task.builder()
                .id(UUID.randomUUID())
                .queueName(queue)
                .priority(priority)
                .status("PENDING")
                .complexity(complexity)
                .createdAt(createdAt)
                .claimedAt(createdAt)
                .build();
    }
}
