package io.github.nestorsokil.taskmaster.config;

/**
 * Carrier for request-scoped values propagated via {@link ScopedValue}.
 *
 * <p>Unlike {@code ThreadLocal}, {@code ScopedValue} is:
 * <ul>
 *   <li>Immutable once bound — no accidental overwrites mid-request.</li>
 *   <li>Automatically inherited by child threads opened inside a
 *       {@code StructuredTaskScope} — so correlation IDs flow into
 *       every parallel subtask without explicit passing.</li>
 *   <li>Freed deterministically when the binding scope exits — no leaks
 *       when virtual threads are recycled.</li>
 * </ul>
 *
 * <p>Bound once per request by {@link io.github.nestorsokil.taskmaster.api.CorrelationIdFilter}.
 */
public final class RequestContext {

    /** Correlation ID for the current request, echoed back as {@code X-Correlation-Id}. */
    public static final ScopedValue<String> CORRELATION_ID = ScopedValue.newInstance();

    private RequestContext() {}
}
