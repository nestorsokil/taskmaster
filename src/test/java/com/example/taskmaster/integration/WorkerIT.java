package com.example.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Worker registration, heartbeat, and death/requeue.
 */
@Tag("integration")
class WorkerIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "worker-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Register a worker twice with the same ID; verify idempotent (one entry).
     */
    @Test
    void idempotentRegistration() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue);
        client.registerWorker(workerId, queue);

        var workers = client.listWorkers();
        var matches = workers.stream()
                .filter(w -> w.workerId().equals(workerId))
                .toList();
        assertThat(matches).hasSize(1);
    }

    /**
     * Heartbeat succeeds for a registered worker (204);
     * fails for an unknown worker (404).
     */
    @Test
    void heartbeat() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue);
        client.heartbeat(workerId); // 204 — no exception

        // Non-existent worker → 404
        var response = client.heartbeatRaw("nonexistent-" + UUID.randomUUID());
        assertThat(response.statusCode()).isEqualTo(404);
    }

    /**
     * Worker stops heartbeating → transitions to DEAD → its tasks are requeued.
     *
     * <p>Depends on {@code taskmaster.heartbeat.dead-threshold-seconds} and
     * {@code taskmaster.reaper.interval-ms} — default dev config uses short values.
     */
    @Test
    void workerDeathAndTaskRequeue() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTask(queue, Map.of("data", "important"));
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);

        // Stop sending heartbeats and wait for dead threshold + reaper cycle.
        // The worker may be cleaned up by the retention reaper before we observe
        // DEAD status, so treat "absent" as proof of death.
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    var workers = client.listWorkers();
                    var status = workers.stream()
                            .filter(w -> w.workerId().equals(workerId))
                            .findFirst()
                            .map(w -> w.status())
                            .orElse("DEAD"); // absent → already cleaned up
                    assertThat(status).isEqualTo("DEAD");
                });

        // Verify the task has been requeued (or moved to DEAD if max attempts exhausted)
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    var task = client.getTask(submitted.taskId());
                    assertThat(task.status()).isIn("PENDING", "DEAD");
                });

        // A new worker should be able to claim the requeued task
        var newWorkerId = "worker-" + UUID.randomUUID();
        client.registerWorker(newWorkerId, queue);

        var task = client.getTask(submitted.taskId());
        if ("PENDING".equals(task.status())) {
            var reclaimed = client.claimTasks(newWorkerId, queue, 1);
            assertThat(reclaimed.tasks()).hasSize(1);
            assertThat(reclaimed.tasks().getFirst().taskId()).isEqualTo(submitted.taskId());
        }
    }
}
