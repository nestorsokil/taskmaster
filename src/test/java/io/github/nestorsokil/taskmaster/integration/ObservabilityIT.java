package io.github.nestorsokil.taskmaster.integration;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Queue stats and filtered task listing.
 */
@Slf4j
@Tag("integration")
class ObservabilityIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "obs-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Submit tasks to two queues, complete some, fail some, verify per-queue counts.
     */
    @Test
    void queueStats() {
        var queueA = uniqueQueue();
        var queueB = uniqueQueue();
        var workerA = "wa-" + UUID.randomUUID();
        var workerB = "wb-" + UUID.randomUUID();
        client.registerWorker(workerA, queueA);
        client.registerWorker(workerB, queueB);

        // Queue A: 3 tasks — complete 1, fail 1 (will go DEAD since maxAttempts=1), leave 1 pending
        var a1 = client.submitTask(queueA, Map.of("i", 1), 0, 1);
        var a2 = client.submitTask(queueA, Map.of("i", 2), 0, 1);
        client.submitTask(queueA, Map.of("i", 3));

        var claimedA = client.claimTasks(workerA, queueA, 2);
        assertThat(claimedA.tasks()).hasSize(2);
        client.completeTask(a1.taskId(), workerA, "ok");
        client.failTask(a2.taskId(), workerA, "boom");

        // Queue B: 2 tasks — leave both pending
        client.submitTask(queueB, Map.of("i", 1));
        client.submitTask(queueB, Map.of("i", 2));

        var stats = client.getQueueStats();
        var statA = stats.stream().filter(s -> s.queueName().equals(queueA)).findFirst().orElseThrow();
        var statB = stats.stream().filter(s -> s.queueName().equals(queueB)).findFirst().orElseThrow();

        assertThat(statA.pending()).isEqualTo(1);
        assertThat(statA.dead()).isEqualTo(1); // failed with maxAttempts=1 → DEAD
        assertThat(statB.pending()).isEqualTo(2);
    }

    /**
     * Filtered task listing by queue, status, limit, and reverse chronological order.
     */
    @Test
    void filteredTaskListing() throws InterruptedException {
        var queueA = uniqueQueue();
        var queueB = uniqueQueue();
        var worker = "w-" + UUID.randomUUID();
        client.registerWorker(worker, queueA);

        // Submit tasks with small delays for ordering
        client.submitTask(queueA, Map.of("order", 1));
        Thread.sleep(50);
        client.submitTask(queueA, Map.of("order", 2));
        Thread.sleep(50);
        client.submitTask(queueA, Map.of("order", 3));
        client.submitTask(queueB, Map.of("other", true));

        // Complete one task in queue A to create mixed statuses
        var claimed = client.claimTasks(worker, queueA, 1);
        client.completeTask(claimed.tasks().getFirst().taskId(), worker, "done");

        // Filter by queue — only queue A tasks
        var byQueue = client.listTasks(queueA, null, null);
        assertThat(byQueue).allMatch(t -> t.queueName().equals(queueA));
        assertThat(byQueue).hasSize(3);

        // Filter by status — only PENDING
        var pending = client.listTasks(queueA, "PENDING", null);
        assertThat(pending).allMatch(t -> t.status().equals("PENDING"));
        assertThat(pending).hasSize(2);

        // Limit
        var limited = client.listTasks(queueA, null, 2);
        assertThat(limited).hasSize(2);

        // Reverse chronological order (most recent first)
        var allA = client.listTasks(queueA, null, null);
        for (int i = 0; i < allA.size() - 1; i++) {
            Instant current = allA.get(i).createdAt();
            Instant next = allA.get(i + 1).createdAt();
            assertThat(current).isAfterOrEqualTo(next);
        }
    }
}
