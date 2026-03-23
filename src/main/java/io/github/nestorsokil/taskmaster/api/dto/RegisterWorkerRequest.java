package io.github.nestorsokil.taskmaster.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Request body for {@code POST /workers/v1/register}.
 *
 * @param workerId       stable, worker-supplied ID (e.g. pod name or hostname)
 * @param queueName      queue this worker will consume from
 * @param maxConcurrency max tasks the worker is willing to hold at once; omit to use default (4)
 */
public record RegisterWorkerRequest(
        @NotBlank String workerId,
        @NotBlank String queueName,
        @Min(1) Integer maxConcurrency,
        @Size(max = 16) List<@Size(max = 64) String> tags
) {
    public RegisterWorkerRequest {
        if (maxConcurrency == null) {
            maxConcurrency = 4;
        }
        if (tags == null) {
            tags = List.of();
        }
    }
}
