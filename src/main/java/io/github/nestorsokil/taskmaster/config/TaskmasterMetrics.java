package io.github.nestorsokil.taskmaster.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Centralised Prometheus counters for task and worker lifecycle events.
 *
 * <p>Every counter is tagged with {@code queue} where applicable so dashboards
 * can slice by queue.  Counters are resolved lazily via
 * {@link MeterRegistry#counter} (Micrometer caches them internally), so there
 * is no need to pre-register every queue.
 */
@Component
public class TaskmasterMetrics {

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, AtomicInteger> workerLoadGauges = new ConcurrentHashMap<>();

    public TaskmasterMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Updates the {@code workers.current_load} gauge for a worker, registering it on
     * first call. The gauge reflects the sum of complexity of all RUNNING tasks owned
     * by that worker. Called after each claim, complete, and fail operation.
     */
    public void setWorkerLoad(String workerId, String queue, int load) {
        var key = workerId + "|" + queue;
        var gauge = workerLoadGauges.computeIfAbsent(key, k -> {
            var value = new AtomicInteger(0);
            registry.gauge("workers.current_load",
                    io.micrometer.core.instrument.Tags.of("worker_id", workerId, "queue", queue),
                    value, AtomicInteger::get);
            return value;
        });
        gauge.set(load);
    }

    public void taskSubmitted(String queue) {
        counter("tasks.submitted", queue).increment();
    }

    public void tasksClaimed(String queue, int count) {
        if (count > 0) {
            counter("tasks.claimed", queue).increment(count);
        }
    }

    public void taskCompleted(String queue) {
        counter("tasks.completed", queue).increment();
    }

    public void taskFailed(String queue) {
        counter("tasks.failed", queue).increment();
    }

    public void tasksReplayed(String queue, int count) {
        if (count > 0) {
            counter("tasks.replayed", queue).increment(count);
        }
    }

    public void taskDeadLettered(String queue, String reason) {
        registry.counter("tasks.dead_lettered", "queue", queue, "reason", reason).increment();
    }

    public void taskDeadLetteredBatch(String reason, int count) {
        if (count > 0) {
            registry.counter("tasks.dead_lettered", "reason", reason).increment(count);
        }
    }

    public void tasksRequeued(String reason, int count) {
        if (count > 0) {
            registry.counter("tasks.requeued", "reason", reason).increment(count);
        }
    }

    public void workerRegistered(String queue) {
        counter("workers.registered", queue).increment();
    }

    public void workersDied(int count) {
        if (count > 0) {
            registry.counter("workers.died").increment(count);
        }
    }

    public void tasksCleaned(int count) {
        if (count > 0) {
            registry.counter("tasks.cleaned").increment(count);
        }
    }

    public void workersCleaned(int count) {
        if (count > 0) {
            registry.counter("workers.cleaned").increment(count);
        }
    }

    public void webhookDelivered(String queue, String status) {
        registry.counter("webhooks.delivered", "queue", queue, "status", status).increment();
    }

    public void webhookDeliveryFailed(String queue) {
        registry.counter("webhooks.delivery_failed", "queue", queue).increment();
    }

    /** Records claimedAt - createdAt for a task, showing if workers are keeping up. */
    public void recordQueueWaitTime(String queue, Duration duration) {
        timer("tasks.queue.wait", queue).record(duration);
    }

    /** Records finishedAt - claimedAt for a task, showing how long workers take. */
    public void recordExecutionDuration(String queue, Duration duration) {
        timer("tasks.execution.duration", queue).record(duration);
    }

    /** Records finishedAt - createdAt for terminal states (DONE/DEAD), including retries. */
    public void recordEndToEndDuration(String queue, Duration duration) {
        timer("tasks.end_to_end.duration", queue).record(duration);
    }

    private Counter counter(String name, String queue) {
        return registry.counter(name, "queue", queue);
    }

    private Timer timer(String name, String queue) {
        return registry.timer(name, "queue", queue);
    }
}
