package com.example.taskmaster.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Typed binding for the {@code taskmaster.*} configuration namespace.
 */
@ConfigurationProperties(prefix = "taskmaster")
public record TaskmasterProperties(Heartbeat heartbeat, Reaper reaper, Retry retry, Retention retention) {

    public record Heartbeat(
            /** Seconds of silence before a worker transitions ACTIVE → STALE. */
            long staleThresholdSeconds,
            /** Seconds of silence before a worker transitions to DEAD and its tasks are requeued. */
            long deadThresholdSeconds
    ) {}

    public record Reaper(
            /** How often (ms) the HeartbeatReaper runs. */
            long intervalMs
    ) {}

    public record Retry(
            /** Base multiplier (seconds) for exponential backoff: 2^attempts * baseDelaySeconds. */
            double baseDelaySeconds
    ) {}

    public record Retention(
            /** How long terminal tasks (DONE/DEAD) are kept after finishing. Zero or negative disables cleanup. */
            Duration ttl,
            /** How often (ms) the retention reaper runs. */
            long intervalMs,
            /** Max rows deleted per batch within a single reaper cycle. */
            int batchSize
    ) {}
}
