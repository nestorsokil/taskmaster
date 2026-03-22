package com.example.taskmaster.integration;

import com.example.taskmaster.api.dto.ClaimTasksResponse.ClaimedTask;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Claim atomicity, priority, FIFO, empty claim, queue isolation.
 */
@Tag("integration")
class ClaimBehaviorIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "claim-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Submit 10 tasks, two workers claim concurrently with maxTasks=10.
     * Each task is claimed by exactly one worker; all 10 are accounted for.
     */
    @Test
    void claimAtomicity() throws Exception {
        var queue = uniqueQueue();
        var worker1 = "w1-" + UUID.randomUUID();
        var worker2 = "w2-" + UUID.randomUUID();
        client.registerWorker(worker1, queue);
        client.registerWorker(worker2, queue);

        var taskIds = new HashSet<UUID>();
        for (int i = 0; i < 10; i++) {
            taskIds.add(client.submitTask(queue, Map.of("i", i)).taskId());
        }

        // Concurrent claims
        try (var executor = Executors.newFixedThreadPool(2)) {
            var future1 = CompletableFuture.supplyAsync(
                    () -> client.claimTasks(worker1, queue, 10), executor);
            var future2 = CompletableFuture.supplyAsync(
                    () -> client.claimTasks(worker2, queue, 10), executor);

            var result1 = future1.get();
            var result2 = future2.get();

            var claimedIds1 = result1.tasks().stream().map(ClaimedTask::taskId).toList();
            var claimedIds2 = result2.tasks().stream().map(ClaimedTask::taskId).toList();

            // No overlap
            var allClaimed = new ArrayList<>(claimedIds1);
            allClaimed.addAll(claimedIds2);
            assertThat(new HashSet<>(allClaimed)).hasSize(allClaimed.size());

            // All 10 accounted for
            assertThat(new HashSet<>(allClaimed)).isEqualTo(taskIds);
        }
    }

    /**
     * Submit tasks with priorities 1, 5, 10. Claimed in order 10, 5, 1.
     */
    @Test
    void priorityOrdering() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var low = client.submitTask(queue, Map.of("p", 1), 1);
        var mid = client.submitTask(queue, Map.of("p", 5), 5);
        var high = client.submitTask(queue, Map.of("p", 10), 10);

        var claimed = client.claimTasks(workerId, queue, 3);
        assertThat(claimed.tasks()).hasSize(3);
        assertThat(claimed.tasks().get(0).taskId()).isEqualTo(high.taskId());
        assertThat(claimed.tasks().get(1).taskId()).isEqualTo(mid.taskId());
        assertThat(claimed.tasks().get(2).taskId()).isEqualTo(low.taskId());
    }

    /**
     * Submit 3 tasks with equal priority. Claimed in submission order (FIFO).
     */
    @Test
    void fifoWithinSamePriority() throws InterruptedException {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var first = client.submitTask(queue, Map.of("order", 1));
        Thread.sleep(50); // small gap to guarantee ordering
        var second = client.submitTask(queue, Map.of("order", 2));
        Thread.sleep(50);
        var third = client.submitTask(queue, Map.of("order", 3));

        var claimed = client.claimTasks(workerId, queue, 3);
        assertThat(claimed.tasks()).hasSize(3);
        assertThat(claimed.tasks().get(0).taskId()).isEqualTo(first.taskId());
        assertThat(claimed.tasks().get(1).taskId()).isEqualTo(second.taskId());
        assertThat(claimed.tasks().get(2).taskId()).isEqualTo(third.taskId());
    }

    /**
     * Claim from a queue with no pending tasks returns an empty list (not an error).
     */
    @Test
    void claimReturnsEmptyWhenNoWork() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var result = client.claimTasks(workerId, queue, 5);
        assertThat(result.tasks()).isEmpty();
    }

    /**
     * A task submitted to queue A is invisible to a worker registered on queue B.
     */
    @Test
    void queueIsolation() {
        var queueA = uniqueQueue();
        var queueB = uniqueQueue();
        var workerB = "worker-" + UUID.randomUUID();

        client.submitTask(queueA, Map.of("data", "for-queue-a"));
        client.registerWorker(workerB, queueB);

        var result = client.claimTasks(workerB, queueB, 10);
        assertThat(result.tasks()).isEmpty();
    }
}
