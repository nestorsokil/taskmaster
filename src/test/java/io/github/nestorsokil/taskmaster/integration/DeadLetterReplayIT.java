package io.github.nestorsokil.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dead-letter replay: single task replay, bulk replay, guards, and edge cases.
 */
@Tag("integration")
class DeadLetterReplayIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "replay-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Helper: submit a task with maxAttempts=1, claim and fail it so it goes DEAD immediately.
     */
    private UUID submitAndKill(String queue, String workerId) {
        var submitted = client.submitTask(queue, Map.of("data", "test"), 0, 1);
        client.claimTasks(workerId, queue, 1);
        client.failTask(submitted.taskId(), workerId, "deliberate failure");
        assertThat(client.getTask(submitted.taskId()).status()).isEqualTo("DEAD");
        return submitted.taskId();
    }

    /**
     * Replay a single DEAD task: status returns to PENDING, attempts reset to 0,
     * last_error preserved, finished_at cleared.
     */
    @Test
    void replaySingleTask() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var taskId = submitAndKill(queue, workerId);
        var dead = client.getTask(taskId);
        assertThat(dead.lastError()).isEqualTo("deliberate failure");

        // Replay
        var replayed = client.replayTask(taskId);
        assertThat(replayed.status()).isEqualTo("PENDING");
        assertThat(replayed.attempts()).isZero();
        assertThat(replayed.lastError()).isEqualTo("deliberate failure"); // preserved
        assertThat(replayed.finishedAt()).isNull();
        assertThat(replayed.createdAt()).isEqualTo(dead.createdAt()); // original creation time
    }

    /**
     * Replayed task can be claimed and completed normally.
     */
    @Test
    void replayedTaskCanBeClaimedAndCompleted() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var taskId = submitAndKill(queue, workerId);
        client.replayTask(taskId);

        // Claim and complete
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        assertThat(claimed.tasks().getFirst().taskId()).isEqualTo(taskId);
        assertThat(claimed.tasks().getFirst().attempts()).isEqualTo(1);

        client.completeTask(taskId, workerId, "success after replay");
        var done = client.getTask(taskId);
        assertThat(done.status()).isEqualTo("DONE");
        assertThat(done.result()).isEqualTo("success after replay");
    }

    /**
     * Replaying a non-DEAD task returns 409 Conflict.
     */
    @Test
    void replayNonDeadTaskReturns409() {
        var queue = uniqueQueue();
        var submitted = client.submitTask(queue, Map.of("data", "test"));

        // Task is PENDING — replay should fail
        var response = client.replayTaskRaw(submitted.taskId(), null);
        assertThat(response.statusCode()).isEqualTo(409);
    }

    /**
     * Replaying a non-existent task returns 404.
     */
    @Test
    void replayMissingTaskReturns404() {
        var response = client.replayTaskRaw(UUID.randomUUID(), null);
        assertThat(response.statusCode()).isEqualTo(404);
    }

    /**
     * Replay with maxAttempts override: claim and fail once, task should stay PENDING
     * (proving maxAttempts=5 took effect instead of the original 1).
     */
    @Test
    void replayWithMaxAttemptsOverride() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var taskId = submitAndKill(queue, workerId);
        client.replayTask(taskId, 5, null);

        // Claim and fail once — with maxAttempts=5, task stays PENDING
        client.claimTasks(workerId, queue, 1);
        client.failTask(taskId, workerId, "still retrying");

        var task = client.getTask(taskId);
        assertThat(task.status()).isEqualTo("PENDING");
        assertThat(task.attempts()).isEqualTo(1);
    }

    /**
     * Bulk replay moves all DEAD tasks in a queue back to PENDING.
     */
    @Test
    void bulkReplay() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Kill 3 tasks
        submitAndKill(queue, workerId);
        submitAndKill(queue, workerId);
        submitAndKill(queue, workerId);

        var result = client.bulkReplay(queue);
        assertThat(result.replayed()).isEqualTo(3);

        // All should be PENDING now
        var tasks = client.listTasks(queue, "PENDING", 10);
        assertThat(tasks).hasSize(3);
    }

    /**
     * Bulk replay with no matching tasks returns replayed=0.
     */
    @Test
    void bulkReplayEmptyQueue() {
        var queue = uniqueQueue();
        var result = client.bulkReplay(queue);
        assertThat(result.replayed()).isZero();
    }

    /**
     * Bulk replay with deadSince filter only replays recently dead tasks.
     */
    @Test
    void bulkReplayWithDeadSinceFilter() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Kill a task, note the time
        submitAndKill(queue, workerId);

        var cutoff = Instant.now().plusSeconds(2);

        // Wait, then kill another
        try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        submitAndKill(queue, workerId);

        // Bulk replay only tasks dead after cutoff — should get 1
        var result = client.bulkReplay(queue, cutoff, null);
        assertThat(result.replayed()).isEqualTo(1);

        // 1 still DEAD, 1 now PENDING
        assertThat(client.listTasks(queue, "DEAD", 10)).hasSize(1);
        assertThat(client.listTasks(queue, "PENDING", 10)).hasSize(1);
    }

    /**
     * tasks.replayed metric is emitted after replay.
     */
    @Test
    void replayEmitsMetric() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double before = client.getMetric("tasks.replayed", "queue", queue);
        var taskId = submitAndKill(queue, workerId);
        client.replayTask(taskId);
        double after = client.getMetric("tasks.replayed", "queue", queue);

        assertThat(after - before).isEqualTo(1.0);
    }
}
