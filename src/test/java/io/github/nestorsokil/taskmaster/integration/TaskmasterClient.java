package io.github.nestorsokil.taskmaster.integration;

import io.github.nestorsokil.taskmaster.api.dto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.common.mapper.TypeRef;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;

/**
 * Black-box HTTP client for the Taskmaster API.
 * Wraps all REST calls so tests read like plain English.
 * Uses the project's own DTO records for type-safe requests and responses.
 */
public final class TaskmasterClient {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final RestAssuredConfig CONFIG = RestAssuredConfig.config()
            .objectMapperConfig(ObjectMapperConfig.objectMapperConfig()
                    .jackson2ObjectMapperFactory((type, s) -> MAPPER));

    private final String baseUrl;

    public TaskmasterClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public TaskmasterClient() {
        this(System.getenv().getOrDefault("TASKMASTER_URL", "http://localhost:8080"));
    }

    // ---- task operations ----

    public SubmitTaskResponse submitTask(String queue, Object payload) {
        return submitTask(queue, payload, 0, null, null, null);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, int priority) {
        return submitTask(queue, payload, priority, null, null, null);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, int priority, Integer maxAttempts) {
        return submitTask(queue, payload, priority, maxAttempts, null, null);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, int priority, Integer maxAttempts, Instant deadline) {
        return submitTask(queue, payload, priority, maxAttempts, deadline, null);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, int priority, Integer maxAttempts, Instant deadline, List<String> tags) {
        return submitTask(queue, payload, priority, maxAttempts, deadline, tags, null);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, int priority, Integer maxAttempts, Instant deadline, List<String> tags, String callbackUrl) {
        var body = new SubmitTaskRequest(queue, MAPPER.valueToTree(payload), priority, maxAttempts, deadline, tags, callbackUrl);
        return request()
                .body(body)
                .when().post("/tasks/v1")
                .then().statusCode(202)
                .extract().as(SubmitTaskResponse.class);
    }

    public SubmitTaskResponse submitTask(String queue, Object payload, List<String> tags) {
        return submitTask(queue, payload, 0, null, null, tags);
    }

    public Response submitTaskRaw(Map<String, Object> body) {
        return request().body(body).when().post("/tasks/v1");
    }

    public TaskResponse getTask(UUID taskId) {
        return request()
                .when().get("/tasks/v1/{id}", taskId)
                .then().statusCode(200)
                .extract().as(TaskResponse.class);
    }

    public Response getTaskRaw(UUID taskId) {
        return request().when().get("/tasks/v1/{id}", taskId);
    }

    public List<TaskResponse> listTasks(String queue, String status, Integer limit) {
        var spec = request();
        if (queue != null) spec = spec.queryParam("queue", queue);
        if (status != null) spec = spec.queryParam("status", status);
        if (limit != null) spec = spec.queryParam("limit", limit);
        return spec
                .when().get("/tasks/v1")
                .then().statusCode(200)
                .extract().as(new TypeRef<>() {});
    }

    // ---- claim operations ----

    public ClaimTasksResponse claimTasks(String workerId, String queue, int maxTasks) {
        var body = new ClaimTasksRequest(workerId, queue, maxTasks);
        return request()
                .body(body)
                .when().post("/tasks/v1/claim")
                .then().statusCode(200)
                .extract().as(ClaimTasksResponse.class);
    }

    public Response claimTasksRaw(Map<String, Object> body) {
        return request().body(body).when().post("/tasks/v1/claim");
    }

    // ---- complete / fail ----

    public void completeTask(UUID taskId, String workerId, String result) {
        var body = new CompleteTaskRequest(workerId, result);
        request().body(body)
                .when().post("/tasks/v1/{id}/complete", taskId)
                .then().statusCode(204);
    }

    public Response completeTaskRaw(UUID taskId, Map<String, Object> body) {
        return request().body(body).when().post("/tasks/v1/{id}/complete", taskId);
    }

    public void failTask(UUID taskId, String workerId, String error) {
        var body = new FailTaskRequest(workerId, error);
        request().body(body)
                .when().post("/tasks/v1/{id}/fail", taskId)
                .then().statusCode(204);
    }

    public Response failTaskRaw(UUID taskId, Map<String, Object> body) {
        return request().body(body).when().post("/tasks/v1/{id}/fail", taskId);
    }

    // ---- replay operations ----

    public TaskResponse replayTask(UUID taskId) {
        return replayTask(taskId, null, null);
    }

    public TaskResponse replayTask(UUID taskId, Integer maxAttempts, Instant deadline) {
        var body = new java.util.HashMap<String, Object>();
        if (maxAttempts != null) body.put("maxAttempts", maxAttempts);
        if (deadline != null) body.put("deadline", deadline.toString());
        return request()
                .body(body)
                .when().post("/tasks/v1/{id}/replay", taskId)
                .then().statusCode(200)
                .extract().as(TaskResponse.class);
    }

    public Response replayTaskRaw(UUID taskId, Map<String, Object> body) {
        return request().body(body != null ? body : Map.of()).when().post("/tasks/v1/{id}/replay", taskId);
    }

    public BulkReplayResponse bulkReplay(String queueName) {
        return bulkReplay(queueName, null, null);
    }

    public BulkReplayResponse bulkReplay(String queueName, Instant deadSince, Integer maxAttempts) {
        var body = new java.util.HashMap<String, Object>();
        body.put("queueName", queueName);
        if (deadSince != null) body.put("deadSince", deadSince.toString());
        if (maxAttempts != null) body.put("maxAttempts", maxAttempts);
        return request()
                .body(body)
                .when().post("/tasks/v1/replay")
                .then().statusCode(200)
                .extract().as(BulkReplayResponse.class);
    }

    public Response bulkReplayRaw(Map<String, Object> body) {
        return request().body(body).when().post("/tasks/v1/replay");
    }

    // ---- worker operations ----

    public void registerWorker(String workerId, String queue) {
        registerWorker(workerId, queue, null);
    }

    public void registerWorker(String workerId, String queue, List<String> tags) {
        var body = new RegisterWorkerRequest(workerId, queue, null, tags);
        request().body(body)
                .when().post("/workers/v1/register")
                .then().statusCode(200);
    }

    public void heartbeat(String workerId) {
        request()
                .when().post("/workers/v1/{id}/heartbeat", workerId)
                .then().statusCode(204);
    }

    public Response heartbeatRaw(String workerId) {
        return request().when().post("/workers/v1/{id}/heartbeat", workerId);
    }

    public List<WorkerResponse> listWorkers() {
        return request()
                .when().get("/workers/v1")
                .then().statusCode(200)
                .extract().as(new TypeRef<>() {});
    }

    // ---- queue stats ----

    public List<QueueSummaryResponse> getQueueStats() {
        return request()
                .when().get("/queues/v1")
                .then().statusCode(200)
                .extract().as(new TypeRef<>() {});
    }

    // ---- metrics (Actuator) ----

    /**
     * Returns the total value of a Micrometer counter from the Actuator metrics endpoint.
     * Optionally filters by a tag (e.g. {@code "queue", "my-queue"}).
     *
     * @return the counter value, or 0.0 if the metric does not exist yet
     */
    public double getMetric(String name, String... tags) {
        var spec = request();
        var path = new StringBuilder("/actuator/metrics/").append(name);
        if (tags.length >= 2) {
            path.append("?tag=").append(tags[0]).append(":").append(tags[1]);
        }
        var response = spec.when().get(path.toString());
        if (response.statusCode() == 404) {
            return 0.0;
        }
        return response.then().statusCode(200)
                .extract().jsonPath().getDouble("measurements[0].value");
    }

    /**
     * Returns a specific statistic (e.g. "COUNT", "TOTAL_TIME") from a timer metric.
     * Falls back to 0.0 if the metric does not exist yet.
     */
    public double getMetricStatistic(String name, String statistic, String... tags) {
        var spec = request();
        var path = new StringBuilder("/actuator/metrics/").append(name);
        if (tags.length >= 2) {
            path.append("?tag=").append(tags[0]).append(":").append(tags[1]);
        }
        var response = spec.when().get(path.toString());
        if (response.statusCode() == 404) {
            return 0.0;
        }
        var measurements = response.then().statusCode(200)
                .extract().jsonPath().getList("measurements", Map.class);
        return measurements.stream()
                .filter(m -> statistic.equals(m.get("statistic")))
                .mapToDouble(m -> ((Number) m.get("value")).doubleValue())
                .findFirst()
                .orElse(0.0);
    }

    // ---- internal helpers ----

    private RequestSpecification request() {
        return given()
                .config(CONFIG)
                .baseUri(baseUrl)
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON);
    }
}
