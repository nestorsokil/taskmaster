package com.example.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Task deadline enforcement via the DeadlineReaper.
 */
@Tag("integration")
class DeadlineIT {

    private final TaskmasterClient client = new TaskmasterClient();

    /**
     * Submit a task with a near-future deadline, do not claim it.
     * After the deadline passes and the DeadlineReaper runs, the task should be DEAD.
     *
     * <p>The DeadlineReaper runs every 30 seconds by default, so the total wait
     * is deadline duration + up to one reaper cycle.
     */
    @Test
    void expiredDeadlineMovesTaskToDead() {
        var queue = "deadline-" + UUID.randomUUID().toString().substring(0, 8);
        var deadline = Instant.now().plusSeconds(5);

        var submitted = client.submitTask(queue, Map.of("urgent", true), 0, null, deadline);

        // Wait for the deadline to pass plus a DeadlineReaper cycle
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    var task = client.getTask(submitted.taskId());
                    assertThat(task.status()).isEqualTo("DEAD");
                });

        var task = client.getTask(submitted.taskId());
        assertThat(task.finishedAt()).isNotNull();
    }
}
