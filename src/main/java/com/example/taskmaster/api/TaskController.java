package com.example.taskmaster.api;

import com.example.taskmaster.api.dto.*;
import com.example.taskmaster.domain.Tags;
import com.example.taskmaster.service.ClaimService;
import com.example.taskmaster.service.ObservabilityService;
import com.example.taskmaster.service.TaskService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/tasks/v1")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;
    private final ClaimService claimService;
    private final ObservabilityService observabilityService;
    private final ObjectMapper objectMapper;

    // submit a new task, returns 202 Accepted.
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SubmitTaskResponse submit(@Valid @RequestBody SubmitTaskRequest request)
            throws JsonProcessingException {
        var task = taskService.submit(
                request.queueName(),
                objectMapper.writeValueAsString(request.payload()),
                request.priority(),
                request.maxAttempts(),
                request.deadline(),
                new Tags(request.tags()),
                request.callbackUrl()
        );
        return new SubmitTaskResponse(task.id(), task.status());
    }

    // fetch a single task by ID.
    @GetMapping("/{taskId}")
    public TaskResponse getTask(@PathVariable UUID taskId) {
        return TaskResponse.from(taskService.getTask(taskId));
    }

    // filtered task list for observability.
    @GetMapping
    public List<TaskResponse> listTasks(
            @RequestParam(required = false) String queue,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "50") int limit
    ) {
        return observabilityService.getTasks(queue, status, limit)
                .stream()
                .map(TaskResponse::from)
                .toList();
    }

    // atomically claim tasks for a worker
    @PostMapping("/claim")
    public ClaimTasksResponse claim(@Valid @RequestBody ClaimTasksRequest request)
            throws JsonProcessingException {
        @SuppressWarnings("null")
        var claimed = claimService.claim(
                request.workerId(),
                request.queueName(),
                request.maxTasks()
        );
        var tasks = new ArrayList<ClaimTasksResponse.ClaimedTask>();
        for (var task : claimed) {
            var payload = objectMapper.readTree(task.payload());
            tasks.add(new ClaimTasksResponse.ClaimedTask(task.id(), payload, task.attempts()));
        }
        return new ClaimTasksResponse(tasks);
    }

    // mark a task DONE.
    @PostMapping("/{taskId}/complete")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void complete(@PathVariable UUID taskId, @Valid @RequestBody CompleteTaskRequest request) {
        taskService.complete(taskId, request.workerId(), request.result());
    }

    // report a task failure; triggers retry or dead-letter logic.
    @PostMapping("/{taskId}/fail")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void fail(@PathVariable UUID taskId, @Valid @RequestBody FailTaskRequest request) {
        taskService.fail(taskId, request.workerId(), request.error());
    }
}
