package com.example.taskmaster.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires up the AOP aspects that power {@code @Observed} and {@code @Timed}.
 *
 * <p>{@link ObservedAspect} intercepts every method annotated with
 * {@code @Observed} (or on a class annotated with it, like {@link com.example.taskmaster.service.TaskService})
 * and records a Micrometer observation — giving automatic timing, error counting,
 * and (if a tracer is on the classpath) distributed trace propagation.
 *
 * <p>{@link TimedAspect} similarly handles {@code @Timed} annotations should any
 * be added in the future, without requiring additional configuration.
 */
@Configuration
public class MicrometerConfig {

    @Bean
    ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
        return new ObservedAspect(observationRegistry);
    }

    @Bean
    TimedAspect timedAspect(MeterRegistry meterRegistry) {
        return new TimedAspect(meterRegistry);
    }
}
