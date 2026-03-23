package io.github.nestorsokil.taskmaster.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
 * Pins the scheduler to a single virtual thread per task execution.
 *
 * <p>Spring's default scheduler uses a one-thread pool, which is fine for our two
 * lightweight reapers. Using a {@link ThreadPoolTaskScheduler} backed by virtual
 * threads (via {@code setVirtualThreads(true)}) means each scheduled invocation
 * runs on its own virtual thread — no blocking on I/O inside the reaper will stall
 * the next scheduled tick.
 */
@Configuration
public class SchedulingConfig implements SchedulingConfigurer {

    @Override
    public void configureTasks(@NonNull ScheduledTaskRegistrar taskRegistrar) {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setVirtualThreads(true);
        scheduler.setThreadNamePrefix("reaper-");
        scheduler.initialize();
        taskRegistrar.setTaskScheduler(scheduler);
    }
}
