package io.github.nestorsokil.taskmaster.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task tags and worker capability matching during claim.
 */
@Tag("integration")
class TaskTagsIT {

    private final TaskmasterClient client = new TaskmasterClient();

    private String uniqueQueue() {
        return "tags-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * A worker with matching capabilities claims a tagged task.
     */
    @Test
    void workerWithMatchingTagsClaimsTask() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue, List.of("gpu", "region:eu"));
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu"));

        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).hasSize(1);
    }

    /**
     * A worker without the required tags cannot claim the tagged task.
     */
    @Test
    void workerWithoutMatchingTagsCannotClaim() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue, List.of("region:eu"));
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu"));

        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).isEmpty();
    }

    /**
     * A worker with no tags can only claim tasks with no tags.
     */
    @Test
    void untaggedWorkerClaimsOnlyUntaggedTasks() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue);
        client.submitTask(queue, Map.of("job", "simple"));
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu"));

        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).hasSize(1);
    }

    /**
     * A capable worker can claim both tagged and untagged tasks.
     */
    @Test
    void capableWorkerClaimsBothTaggedAndUntagged() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue, List.of("gpu"));
        client.submitTask(queue, Map.of("job", "simple"));
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu"));

        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).hasSize(2);
    }

    /**
     * Task tags must be a strict subset. A worker with ["gpu"] cannot claim
     * a task tagged ["gpu", "region:us"].
     */
    @Test
    void partialMatchDoesNotSuffice() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue, List.of("gpu"));
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu", "region:us"));

        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).isEmpty();
    }

    /**
     * Tags are returned in the task response.
     */
    @Test
    void tagsVisibleInTaskResponse() {
        var queue = uniqueQueue();
        var tags = List.of("gpu", "region:eu");

        var submitted = client.submitTask(queue, Map.of("job", "train"), tags);
        var task = client.getTask(submitted.taskId());

        assertThat(task.tags()).containsExactlyInAnyOrderElementsOf(tags);
    }

    /**
     * Tags are returned in the worker list response.
     */
    @Test
    void tagsVisibleInWorkerResponse() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        var tags = List.of("gpu", "high-mem");

        client.registerWorker(workerId, queue, tags);

        var workers = client.listWorkers();
        var worker = workers.stream()
                .filter(w -> w.workerId().equals(workerId))
                .findFirst()
                .orElseThrow();
        assertThat(worker.tags()).containsExactlyInAnyOrderElementsOf(tags);
    }

    /**
     * Re-registering a worker with new tags updates its capabilities.
     */
    @Test
    void reRegisterUpdatesWorkerTags() {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();

        client.registerWorker(workerId, queue, List.of("region:eu"));

        // Task needs gpu — worker can't claim it yet
        client.submitTask(queue, Map.of("job", "train"), List.of("gpu"));
        var claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).isEmpty();

        // Re-register with gpu capability
        client.registerWorker(workerId, queue, List.of("gpu", "region:eu"));

        claimed = client.claimTasks(workerId, queue, 10);
        assertThat(claimed.tasks()).hasSize(1);
    }
}
