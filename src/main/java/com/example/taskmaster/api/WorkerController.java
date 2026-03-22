package com.example.taskmaster.api;

import com.example.taskmaster.api.dto.RegisterWorkerRequest;
import com.example.taskmaster.api.dto.WorkerResponse;
import com.example.taskmaster.config.TaskmasterMetrics;
import com.example.taskmaster.domain.Tags;
import com.example.taskmaster.repository.WorkerRepository;
import com.example.taskmaster.service.ObservabilityService;
import lombok.RequiredArgsConstructor;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/workers/v1")
@RequiredArgsConstructor
public class WorkerController {

    private final WorkerRepository workerRepository;
    private final ObservabilityService observabilityService;
    private final TaskmasterMetrics metrics;

    @PostMapping("/register")
    public void register(@Valid @RequestBody RegisterWorkerRequest request) {
        workerRepository.upsert(request.workerId(), request.queueName(), request.maxConcurrency(), new Tags(request.tags()));
        metrics.workerRegistered(request.queueName());
    }

    @PostMapping("/{workerId}/heartbeat")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void heartbeat(@PathVariable String workerId) {
        int updated = workerRepository.updateHeartbeat(workerId);
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Worker not registered: " + workerId);
        }
    }

    @GetMapping
    public List<WorkerResponse> listWorkers() {
        return observabilityService.getWorkers()
                .stream()
                .map(WorkerResponse::from)
                .toList();
    }
}
