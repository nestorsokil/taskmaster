package com.example.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task submission validation, claim validation, and not-found.
 */
@Tag("integration")
class ValidationIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "validation-" + UUID.randomUUID().toString().substring(0, 8);
    }

    // ---- Task Submission Validation ----

    @Test
    void submitWithMissingQueueName() {
        var body = new HashMap<String, Object>();
        body.put("payload", Map.of("key", "value"));
        assertThat(client.submitTaskRaw(body).statusCode()).isEqualTo(400);
    }

    @Test
    void submitWithMissingPayload() {
        var body = new HashMap<String, Object>();
        body.put("queueName", "some-queue");
        assertThat(client.submitTaskRaw(body).statusCode()).isEqualTo(400);
    }

    @Test
    void submitWithMaxAttemptsZero() {
        var body = new HashMap<String, Object>();
        body.put("queueName", "some-queue");
        body.put("payload", Map.of("key", "value"));
        body.put("maxAttempts", 0);
        assertThat(client.submitTaskRaw(body).statusCode()).isEqualTo(400);
    }

    @Test
    void submitWithOnlyRequiredFieldsAppliesDefaults() {
        var queue = uniqueQueue();
        var submitted = client.submitTask(queue, Map.of("key", "value"));
        assertThat(submitted.status()).isEqualTo("PENDING");

        var task = client.getTask(submitted.taskId());
        assertThat(task.queueName()).isEqualTo(queue);
        assertThat(task.status()).isEqualTo("PENDING");
        assertThat(task.attempts()).isZero();
    }

    // ---- Claim Validation ----

    @Test
    void claimWithMaxTasksZero() {
        var body = new HashMap<String, Object>();
        body.put("workerId", "w");
        body.put("queueName", "q");
        body.put("maxTasks", 0);
        assertThat(client.claimTasksRaw(body).statusCode()).isEqualTo(400);
    }

    @Test
    void claimWithMaxTasksTooLarge() {
        var body = new HashMap<String, Object>();
        body.put("workerId", "w");
        body.put("queueName", "q");
        body.put("maxTasks", 101);
        assertThat(client.claimTasksRaw(body).statusCode()).isEqualTo(400);
    }

    @Test
    void claimWithMissingWorkerId() {
        var body = new HashMap<String, Object>();
        body.put("queueName", "q");
        body.put("maxTasks", 1);
        assertThat(client.claimTasksRaw(body).statusCode()).isEqualTo(400);
    }

    // ---- Get Task Not Found ----

    @Test
    void getTaskNotFound() {
        var randomId = UUID.randomUUID();
        assertThat(client.getTaskRaw(randomId).statusCode()).isEqualTo(404);
    }
}
