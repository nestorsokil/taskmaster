package io.github.nestorsokil.taskmaster.api;

import io.github.nestorsokil.taskmaster.api.dto.QueueSummaryResponse;
import io.github.nestorsokil.taskmaster.service.ObservabilityService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/queues/v1")
@RequiredArgsConstructor
public class QueueController {

    private final ObservabilityService observabilityService;

    /** GET /queues/v1 — per-queue task counts and active worker count. */
    @GetMapping
    public List<QueueSummaryResponse> listQueues() {
        return observabilityService.getQueueSummaries()
                .stream()
                .map(QueueSummaryResponse::from)
                .toList();
    }
}
