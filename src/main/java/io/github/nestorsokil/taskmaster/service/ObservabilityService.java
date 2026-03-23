package io.github.nestorsokil.taskmaster.service;

import io.github.nestorsokil.taskmaster.domain.Task;
import io.github.nestorsokil.taskmaster.domain.Worker;
import io.github.nestorsokil.taskmaster.repository.TaskRepository;
import io.github.nestorsokil.taskmaster.repository.WorkerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ObservabilityService {

    private final TaskRepository taskRepository;
    private final WorkerRepository workerRepository;

    /**
     * Returns filtered tasks for {@code GET /tasks}.
     * Either parameter may be {@code null} to skip that filter.
     */
    public List<Task> getTasks(String queueName, String status, int limit) {
        return taskRepository.findFiltered(queueName, status, limit);
    }

    /**
     * Returns per-queue counts augmented with the number of ACTIVE workers.
     *
     * <p>The two underlying queries — task stats and worker list — are independent
     * and fanned out in parallel on virtual threads via a per-call executor.
     * The try-with-resources on the executor ensures both threads are joined
     * before the method returns (same structured lifetime as StructuredTaskScope).
     */
    public List<QueueSummary> getQueueSummaries() {
        // try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
        //     var statsTask   = scope.fork(taskRepository::getQueueStats);
        //     var workersTask = scope.fork(workerRepository::findAll);
        //
        //     scope.join().throwIfFailed();
        //
        //     var workerCounts = workersTask.get().stream()
        //             .filter(w -> "ACTIVE".equals(w.status()))
        //             .collect(Collectors.groupingBy(Worker::queueName, Collectors.counting()));
        //
        //     return statsTask.get().stream()
        //             .map(s -> new QueueSummary(
        //                     s.queueName(), s.pending(), s.running(), s.failed(), s.dead(),
        //                     workerCounts.getOrDefault(s.queueName(), 0L)
        //             ))
        //             .toList();
        //
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        //     throw new RuntimeException("Interrupted while fetching queue summaries", e);
        // } catch (ExecutionException e) {
        //     throw new RuntimeException("Failed to fetch queue summaries", e.getCause());
        // }

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var statsFuture   = CompletableFuture.supplyAsync(taskRepository::getQueueStats, executor);
            var workersFuture = CompletableFuture.supplyAsync(workerRepository::findActive, executor);

            Map<String, Long> workerCounts = workersFuture.get().stream()
                    .collect(Collectors.groupingBy(Worker::queueName, Collectors.counting()));
            return statsFuture.get().stream()
                    .map(stats -> new QueueSummary(
                            stats.queue_name(), stats.pending(), stats.running(), stats.failed(), stats.dead(),
                            workerCounts.getOrDefault(stats.queue_name(), 0L)
                    ))
                    .toList();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching queue summaries", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to fetch queue summaries", e.getCause());
        }
    }

    /**
     * Returns all registered workers for {@code GET /workers}.
     */
    public List<Worker> getWorkers() {
        return workerRepository.findAll();
    }

    /**
     * Per-queue aggregated view combining task counts and active worker count.
     * Returned by {@code GET /queues}.
     */
    public record QueueSummary(
            String queueName,
            long pending,
            long running,
            long failed,
            long dead,
            long activeWorkers
    ) {}
}
