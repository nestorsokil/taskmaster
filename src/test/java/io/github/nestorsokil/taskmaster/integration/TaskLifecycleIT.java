package io.github.nestorsokil.taskmaster.integration;

import io.github.nestorsokil.taskmaster.api.dto.ClaimTasksResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Task lifecycle happy path, failure/retry, and ownership guard.
 */
@Tag("integration")
class TaskLifecycleIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "lifecycle-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Submit → retrieve → register worker → claim → complete → verify DONE.
     */
    @Test
    void happyPath() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        var payload = Map.of("action", "send-email", "to", "user@test.com");

        // Submit
        var submitted = client.submitTask(queue, payload);
        assertThat(submitted.taskId()).isNotNull();
        assertThat(submitted.status()).isEqualTo("PENDING");

        // Retrieve and verify fields
        var task = client.getTask(submitted.taskId());
        assertThat(task.queueName()).isEqualTo(queue);
        assertThat(task.status()).isEqualTo("PENDING");
        assertThat(task.attempts()).isZero();
        assertThat(task.createdAt()).isNotNull();

        // Register worker, claim
        client.registerWorker(workerId, queue);
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        assertThat(claimed.tasks().getFirst().taskId()).isEqualTo(submitted.taskId());
        assertThat(claimed.tasks().getFirst().attempts()).isEqualTo(1);

        // Complete
        client.completeTask(submitted.taskId(), workerId, "email-sent");

        // Verify terminal state
        var done = client.getTask(submitted.taskId());
        assertThat(done.status()).isEqualTo("DONE");
        assertThat(done.result()).isEqualTo("email-sent");
        assertThat(done.finishedAt()).isNotNull();
    }

    /**
     * Submit with maxAttempts=3, fail three times, verify retry and eventual DEAD.
     */
    @Test
    void failureAndRetry() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTask(queue, Map.of("data", "test"), 0, 3);

        // Attempt 1: claim and fail
        var claim1 = client.claimTasks(workerId, queue, 1);
        assertThat(claim1.tasks()).hasSize(1);
        client.failTask(submitted.taskId(), workerId, "connection timeout");

        var afterFail1 = client.getTask(submitted.taskId());
        assertThat(afterFail1.status()).isEqualTo("PENDING");
        assertThat(afterFail1.lastError()).isEqualTo("connection timeout");
        assertThat(afterFail1.attempts()).isEqualTo(1);

        // Attempt 2: wait for backoff, then claim
        // Default base delay is 5s (2^1*5=10s). For faster tests, configure base-delay-seconds: 1 (2^1*1=2s).
        ClaimTasksResponse claim2 = await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> client.claimTasks(workerId, queue, 1),
                        c -> !c.tasks().isEmpty());
        assertThat(claim2.tasks().getFirst().attempts()).isEqualTo(2);
        client.failTask(submitted.taskId(), workerId, "timeout again");

        // Attempt 3: wait for backoff, then claim
        ClaimTasksResponse claim3 = await().atMost(Duration.ofSeconds(25))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> client.claimTasks(workerId, queue, 1),
                        c -> !c.tasks().isEmpty());
        assertThat(claim3.tasks().getFirst().attempts()).isEqualTo(3);
        client.failTask(submitted.taskId(), workerId, "final failure");

        // Verify DEAD after exhausting all attempts
        var dead = client.getTask(submitted.taskId());
        assertThat(dead.status()).isEqualTo("DEAD");
        assertThat(dead.lastError()).isEqualTo("final failure");
    }

    /**
     * Worker A claims a task; worker B cannot complete or fail it (409).
     * Worker A can complete it successfully.
     */
    @Test
    void ownershipGuard() {
        var queue = uniqueQueue();
        var workerA = "workerA-" + UUID.randomUUID();
        var workerB = "workerB-" + UUID.randomUUID();
        client.registerWorker(workerA, queue);
        client.registerWorker(workerB, queue);

        client.submitTask(queue, Map.of("key", "value"));

        // Worker A claims
        var claimed = client.claimTasks(workerA, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        var taskId = claimed.tasks().getFirst().taskId();

        // Worker B attempts to complete → 409
        var completeBody = new HashMap<String, Object>();
        completeBody.put("workerId", workerB);
        completeBody.put("result", "stolen");
        assertThat(client.completeTaskRaw(taskId, completeBody).statusCode()).isEqualTo(409);

        // Worker B attempts to fail → 409
        var failBody = new HashMap<String, Object>();
        failBody.put("workerId", workerB);
        failBody.put("error", "stolen");
        assertThat(client.failTaskRaw(taskId, failBody).statusCode()).isEqualTo(409);

        // Worker A completes successfully
        client.completeTask(taskId, workerA, "done");

        var task = client.getTask(taskId);
        assertThat(task.status()).isEqualTo("DONE");
    }
}
