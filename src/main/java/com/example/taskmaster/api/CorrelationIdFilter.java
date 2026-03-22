package com.example.taskmaster.api;

import com.example.taskmaster.config.RequestContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Binds a correlation ID to the current virtual thread via {@link ScopedValue} for
 * the full lifetime of each HTTP request.
 *
 * <p>The ID is taken from the incoming {@code X-Correlation-Id} header when present
 * (so callers can trace a request end-to-end across services), or generated fresh
 * as a UUID otherwise. It is echoed back on the response header.
 *
 * <p>Because every Tomcat request handler runs on a virtual thread
 * ({@code spring.threads.virtual.enabled=true}), and because {@link ScopedValue}
 * propagates into any child threads opened inside a {@link java.util.concurrent.StructuredTaskScope},
 * the correlation ID is automatically available in every parallel subtask spawned
 * during the request — for example in {@code ObservabilityService.getQueueSummaries()}.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain) throws ServletException, IOException {
        String correlationId = Optional.ofNullable(request.getHeader(HEADER))
                .orElse(UUID.randomUUID().toString());

        response.setHeader(HEADER, correlationId);

        try {
            // ScopedValue.where(...).call() propagates the binding to the current
            // virtual thread and every StructuredTaskScope child forked from it.
            ScopedValue.where(RequestContext.CORRELATION_ID, correlationId).call(() -> {
                chain.doFilter(request, response);
                return null;
            });
        } catch (ServletException | IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
