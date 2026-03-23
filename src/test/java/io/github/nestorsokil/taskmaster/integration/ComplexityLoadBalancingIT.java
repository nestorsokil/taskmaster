package io.github.nestorsokil.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for complexity-based load balancing.
 *
 * <p>Workers have a total complexity budget (maxConcurrency). Tasks carry a
 * complexity weight. A claim only succeeds if the task's complexity fits within
 * the worker's remaining budget (capacity - currentLoad). Heavy tasks are skipped
 * in favour of lighter tasks behind them (greedy skip).
 */
@Tag("integration")
class ComplexityLoadBalancingIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String q() {
        return "complexity-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Default complexity (1) and default capacity (4) → identical to old behaviour:
     * a worker can claim up to maxTasks tasks.
     */
    @Test
    void defaultComplexityPreservesExistingBehaviour() {
        var queue = q();
        var workerId = "w-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        for (int i = 0; i < 4; i++) {
            client.submitTask(queue, Map.of("i", i));
        }

        var claimed = client.claimTasks(workerId, queue, 4);
        assertThat(claimed.tasks()).hasSize(4);
        assertThat(claimed.tasks()).allMatch(t -> t.complexity() == 1);
    }

    /**
     * Worker with capacity 4 claims tasks whose complexities sum to ≤ 4.
     * A task with complexity 3 and a task with complexity 1 are claimed (total 4).
     * The task with complexity 2 (which would push total to 5) is not claimed.
     */
    @Test
    void workerRespectsComplexityBudget() {
        var queue = q();
        var workerId = "w-" + UUID.randomUUID();
        client.registerWorker(workerId, queue, 4, null); // capacity = 4

        var heavy = client.submitTaskWithComplexity(queue, Map.of("t", "heavy"), 3); // complexity 3
        client.submitTaskWithComplexity(queue, Map.of("t", "medium"), 2); // complexity 2 — will be skipped
        var light = client.submitTaskWithComplexity(queue, Map.of("t", "light"), 1); // complexity 1

        var claimed = client.claimTasks(workerId, queue, 10);
        // Greedy order: heavy (3, fits, remaining=1), medium (2 > 1, skip), light (1, fits, remaining=0)
        var claimedIds = claimed.tasks().stream().map(t -> t.taskId()).toList();
        assertThat(claimedIds).containsExactlyInAnyOrder(heavy.taskId(), light.taskId());
        assertThat(claimed.tasks().stream().mapToInt(t -> t.complexity()).sum()).isEqualTo(4);
    }

    /**
     * Worker at full capacity (currentLoad == capacity) gets an empty response
     * even if PENDING tasks exist.
     */
    @Test
    void workerAtCapacityReceivesNoTasks() {
        var queue = q();
        var workerId = "w-" + UUID.randomUUID();
        client.registerWorker(workerId, queue, 2, null); // capacity = 2

        // Claim two complexity-1 tasks to fill the budget completely
        client.submitTask(queue, Map.of("fill", 1));
        client.submitTask(queue, Map.of("fill", 2));
        var firstClaim = client.claimTasks(workerId, queue, 2);
        assertThat(firstClaim.tasks()).hasSize(2);

        // Submit more work — but the worker is full
        client.submitTask(queue, Map.of("extra", 1));

        var secondClaim = client.claimTasks(workerId, queue, 10);
        assertThat(secondClaim.tasks()).isEmpty();
    }

    /**
     * After completing a task the worker's load drops and it can claim again.
     */
    @Test
    void loadDropsAfterCompletion() {
        var queue = q();
        var workerId = "w-" + UUID.randomUUID();
        client.registerWorker(workerId, queue, 2, null); // capacity = 2

        client.submitTask(queue, Map.of("a", 1));
        client.submitTask(queue, Map.of("b", 1));
        client.submitTask(queue, Map.of("c", 1));

        // Fill the worker
        var first = client.claimTasks(workerId, queue, 2);
        assertThat(first.tasks()).hasSize(2);

        // No budget left
        assertThat(client.claimTasks(workerId, queue, 1).tasks()).isEmpty();

        // Complete one task → budget opens up
        client.completeTask(first.tasks().get(0).taskId(), workerId, "ok");

        var second = client.claimTasks(workerId, queue, 1);
        assertThat(second.tasks()).hasSize(1);
    }

    /**
     * Complexity is exposed in ClaimedTask and in the GET /tasks/{id} response.
     */
    @Test
    void complexityIsIncludedInResponses() {
        var queue = q();
        var workerId = "w-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTaskWithComplexity(queue, Map.of("x", 1), 3);

        // GET /tasks/{id} includes complexity
        var taskDetail = client.getTask(submitted.taskId());
        assertThat(taskDetail.complexity()).isEqualTo(3);

        // Claimed task also carries complexity
        var claimed = client.claimTasks(workerId, queue, 1);
        assertThat(claimed.tasks()).hasSize(1);
        assertThat(claimed.tasks().get(0).complexity()).isEqualTo(3);
    }

    /**
     * A task whose complexity exceeds every registered worker's capacity is still
     * accepted (HTTP 202) — a capable worker may register later.
     */
    @Test
    void taskAcceptedEvenIfNoWorkerHasSufficientCapacity() {
        var queue = q();
        // Register a worker with capacity 2
        client.registerWorker("small-worker-" + UUID.randomUUID(), queue, 2, null);

        // Submit a task with complexity 10 — no current worker can handle it
        var submitted = client.submitTaskWithComplexity(queue, Map.of("heavy", true), 10);
        assertThat(submitted.taskId()).isNotNull();
        assertThat(submitted.status()).isEqualTo("PENDING");
    }

    /**
     * Complexity validation rejects complexity < 1.
     */
    @Test
    void submittingComplexityZeroIsRejected() {
        var body = new java.util.HashMap<String, Object>();
        body.put("queueName", q());
        body.put("payload", Map.of("x", 1));
        body.put("complexity", 0);
        assertThat(client.submitTaskRaw(body).statusCode()).isEqualTo(400);
    }

    /**
     * workers.current_load gauge is emitted and tracks the running load.
     */
    @Test
    void workerCurrentLoadGaugeIsUpdated() {
        var queue = q();
        var workerId = "w-gauge-" + UUID.randomUUID();
        client.registerWorker(workerId, queue, 10, null);

        client.submitTaskWithComplexity(queue, Map.of("g", 1), 3);
        client.submitTaskWithComplexity(queue, Map.of("g", 2), 2);

        client.claimTasks(workerId, queue, 2);

        double load = client.getMetric("workers.current_load", "worker_id", workerId);
        assertThat(load).isEqualTo(5.0); // 3 + 2
    }
}
