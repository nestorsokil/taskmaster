package com.example.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that Micrometer counters are incremented through the full task and worker lifecycle.
 * Reads counter values via the Spring Boot Actuator {@code /actuator/metrics} endpoint.
 */
@Tag("integration")
class MetricsIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "metrics-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Submit, claim, complete, and fail tasks — verify the corresponding counters increase.
     */
    @Test
    void taskLifecycleCounters() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double submittedBefore = client.getMetric("tasks.submitted", "queue", queue);
        double claimedBefore = client.getMetric("tasks.claimed", "queue", queue);
        double completedBefore = client.getMetric("tasks.completed", "queue", queue);
        double failedBefore = client.getMetric("tasks.failed", "queue", queue);

        // Submit 2 tasks
        var t1 = client.submitTask(queue, Map.of("i", 1));
        var t2 = client.submitTask(queue, Map.of("i", 2), 0, 1);

        assertThat(client.getMetric("tasks.submitted", "queue", queue))
                .isEqualTo(submittedBefore + 2);

        // Claim both
        var claimed = client.claimTasks(workerId, queue, 2);
        assertThat(claimed.tasks()).hasSize(2);

        assertThat(client.getMetric("tasks.claimed", "queue", queue))
                .isEqualTo(claimedBefore + 2);

        // Complete one, fail the other
        client.completeTask(t1.taskId(), workerId, "ok");
        client.failTask(t2.taskId(), workerId, "boom");

        assertThat(client.getMetric("tasks.completed", "queue", queue))
                .isEqualTo(completedBefore + 1);
        assertThat(client.getMetric("tasks.failed", "queue", queue))
                .isEqualTo(failedBefore + 1);
    }

    /**
     * Verify the worker registration counter increments.
     */
    @Test
    void workerRegistrationCounter() {
        var queue = uniqueQueue();

        double before = client.getMetric("workers.registered", "queue", queue);

        client.registerWorker("w-" + UUID.randomUUID(), queue);
        client.registerWorker("w-" + UUID.randomUUID(), queue);

        assertThat(client.getMetric("workers.registered", "queue", queue))
                .isEqualTo(before + 2);
    }

    /**
     * Verify that failing a task with maxAttempts=1 increments the dead-lettered counter.
     */
    @Test
    void deadLetteredCounter() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double before = client.getMetric("tasks.dead_lettered", "queue", queue);

        var task = client.submitTask(queue, Map.of("data", "doomed"), 0, 1);
        client.claimTasks(workerId, queue, 1);
        client.failTask(task.taskId(), workerId, "fatal");

        assertThat(client.getMetric("tasks.dead_lettered", "queue", queue))
                .isGreaterThan(before);
    }

    /**
     * Verify that claiming tasks records queue wait time (claimedAt - createdAt).
     */
    @Test
    void queueWaitTimeRecordedOnClaim() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double countBefore = client.getMetricStatistic("tasks.queue.wait", "COUNT", "queue", queue);

        client.submitTask(queue, Map.of("i", 1));
        client.submitTask(queue, Map.of("i", 2));
        client.claimTasks(workerId, queue, 2);

        assertThat(client.getMetricStatistic("tasks.queue.wait", "COUNT", "queue", queue))
                .isEqualTo(countBefore + 2);
        assertThat(client.getMetricStatistic("tasks.queue.wait", "TOTAL_TIME", "queue", queue))
                .isGreaterThan(0.0);
    }

    /**
     * Verify that completing a task records execution duration (finishedAt - claimedAt)
     * and end-to-end latency (finishedAt - createdAt).
     */
    @Test
    void executionAndEndToEndRecordedOnComplete() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double execCountBefore = client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue);
        double e2eCountBefore = client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue);

        var task = client.submitTask(queue, Map.of("data", "work"));
        client.claimTasks(workerId, queue, 1);
        client.completeTask(task.taskId(), workerId, "done");

        assertThat(client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue))
                .isEqualTo(execCountBefore + 1);
        assertThat(client.getMetricStatistic("tasks.execution.duration", "TOTAL_TIME", "queue", queue))
                .isGreaterThan(0.0);

        assertThat(client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue))
                .isEqualTo(e2eCountBefore + 1);
        assertThat(client.getMetricStatistic("tasks.end_to_end.duration", "TOTAL_TIME", "queue", queue))
                .isGreaterThan(0.0);
    }

    /**
     * Verify that failing a task records execution duration, and that dead-lettering
     * (terminal state) also records end-to-end latency.
     */
    @Test
    void executionAndEndToEndRecordedOnFailAndDeadLetter() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double execCountBefore = client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue);
        double e2eCountBefore = client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue);

        // maxAttempts=1 so failure goes straight to DEAD
        var task = client.submitTask(queue, Map.of("data", "doomed"), 0, 1);
        client.claimTasks(workerId, queue, 1);
        client.failTask(task.taskId(), workerId, "crash");

        assertThat(client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue))
                .isEqualTo(execCountBefore + 1);

        // Dead-lettered → terminal state → end-to-end recorded
        assertThat(client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue))
                .isEqualTo(e2eCountBefore + 1);
        assertThat(client.getMetricStatistic("tasks.end_to_end.duration", "TOTAL_TIME", "queue", queue))
                .isGreaterThan(0.0);
    }

    /**
     * Verify that failing a task that will be retried (not dead-lettered) records
     * execution duration but NOT end-to-end latency (task is not in a terminal state).
     */
    @Test
    void noEndToEndOnRetryableFail() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        double e2eCountBefore = client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue);
        double execCountBefore = client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue);

        // maxAttempts=3 (default) so first failure → retry, not DEAD
        var task = client.submitTask(queue, Map.of("data", "retriable"));
        client.claimTasks(workerId, queue, 1);
        client.failTask(task.taskId(), workerId, "transient error");

        // Execution duration should still be recorded
        assertThat(client.getMetricStatistic("tasks.execution.duration", "COUNT", "queue", queue))
                .isEqualTo(execCountBefore + 1);

        // End-to-end should NOT be recorded (task is requeued, not terminal)
        assertThat(client.getMetricStatistic("tasks.end_to_end.duration", "COUNT", "queue", queue))
                .isEqualTo(e2eCountBefore);
    }
}
