package io.github.nestorsokil.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that terminal tasks (DONE/DEAD) are automatically cleaned up
 * after the configured retention TTL.
 *
 * <p>Relies on short dev config values for {@code taskmaster.retention.*}.
 */
@Tag("integration")
class RetentionIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "retention-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void completedTaskIsCleanedUpAfterTtl() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Submit, claim, and complete a task
        var submitted = client.submitTask(queue, Map.of("data", "ephemeral"));
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        client.completeTask(submitted.taskId(), workerId, "done");

        // Verify it's DONE
        var task = client.getTask(submitted.taskId());
        assertThat(task.status()).isEqualTo("DONE");

        // Wait for retention reaper to clean it up
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    var response = client.getTaskRaw(submitted.taskId());
                    assertThat(response.statusCode()).isEqualTo(404);
                });
    }

    @Test
    void deadTaskIsCleanedUpAfterTtl() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Submit with maxAttempts=1 so first failure dead-letters it
        var submitted = client.submitTask(queue, Map.of("data", "doomed"), 0, 1);
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        client.failTask(submitted.taskId(), workerId, "fatal error");

        // Verify it's DEAD
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    var task = client.getTask(submitted.taskId());
                    assertThat(task.status()).isEqualTo("DEAD");
                });

        // Wait for retention reaper to clean it up
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    var response = client.getTaskRaw(submitted.taskId());
                    assertThat(response.statusCode()).isEqualTo(404);
                });
    }

    @Test
    void deadWorkerIsCleanedUpAfterTtl() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Wait for the worker to die and then be cleaned up by the retention reaper.
        // With dead-threshold=10s, TTL=3s, reaper-interval=2s the worker can be
        // removed as soon as ~15s after registration, so we just wait for absence.
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    var workers = client.listWorkers();
                    var match = workers.stream()
                            .filter(w -> w.workerId().equals(workerId))
                            .findFirst();
                    assertThat(match).isEmpty();
                });
    }

    @Test
    void pendingTaskIsNotCleanedUp() throws InterruptedException {
        var queue = uniqueQueue();

        // Submit a task but never claim it — stays PENDING
        var submitted = client.submitTask(queue, Map.of("data", "persistent"));

        // Sleep past the retention TTL (3s) + at least one reaper cycle (2s)
        Thread.sleep(Duration.ofSeconds(8));

        // Task should still exist and be PENDING
        var task = client.getTask(submitted.taskId());
        assertThat(task.status()).isEqualTo("PENDING");
    }
}
