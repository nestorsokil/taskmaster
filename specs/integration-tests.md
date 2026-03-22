# Integration Tests

## Problem

There are no automated tests for Taskmaster. The service needs black-box integration tests that exercise the full API over HTTP, verifying end-to-end behavior including database interactions, scheduled reapers, and error handling.

## Goal

Build an integration test suite in Java that:
- Tests exclusively via HTTP (no internal class imports from the main source)
- Is runnable against any Taskmaster instance (local, CI, remote) by pointing at a base URL
- Is readable and well-structured with a reasonable abstraction layer over raw HTTP calls
- Covers the complete current API surface and key system behaviors (retries, reapers, concurrency)

## Test Infrastructure

### HTTP Client Abstraction

A `TaskmasterClient` helper class wraps all HTTP calls to the Taskmaster API. Test methods read like plain English by calling methods such as `client.submitTask(...)`, `client.claimTasks(...)`, `client.completeTask(...)`, etc. This class handles JSON serialization, HTTP status assertions, and response parsing internally. Tests never construct raw HTTP requests.

### Configuration

The test suite reads the target base URL from an environment variable (e.g., `TASKMASTER_URL`), defaulting to `http://localhost:8080` for local development. No Spring context is loaded. Tests are plain JUnit 5 classes that can run against any environment.

### Dependencies

Add REST Assured as a test dependency for ergonomic HTTP calls and assertions. Use AssertJ for fluent assertions (already available via spring-boot-starter-test). Use Awaitility for polling assertions on async/eventual behavior (already available).

### Docker Compose

A `docker-compose.yml` at the project root provides PostgreSQL for local and CI runs. The Taskmaster application itself runs on the host (via `mvn spring-boot:run` or as a JAR), not inside Docker. This keeps the feedback loop fast during development. The compose file exposes Postgres on port 5432 with credentials matching `application.yml` defaults.

### Database Cleanup

Each test class cleans up its data in a `@BeforeEach` or `@AfterEach` method by calling a utility that truncates the tasks and workers tables (via a direct JDBC connection or a dedicated test-support endpoint). This ensures test isolation without restarting the service.

Alternatively, each test class uses unique queue names (e.g., prefixed with the test class name) to avoid cross-test interference, making cleanup optional for most cases.

### Build Integration

Integration tests live in `src/test/java` under a dedicated package (e.g., `com.example.taskmaster.integration`). They are tagged with a JUnit 5 tag (e.g., `@Tag("integration")`) so they can be included or excluded from Maven runs. The Maven Failsafe plugin runs them in the `verify` phase, separate from unit tests.

## Test Coverage

### 1. Task Lifecycle (Happy Path)

- Submit a task and verify the response contains a task ID and status PENDING
- Retrieve the submitted task by ID and verify all fields match (queue, priority, status, payload)
- Register a worker, claim the task, verify the claimed task contains the correct payload and attempts = 1
- Complete the task with a result string, verify 204 response
- Retrieve the task again and verify status is DONE, result matches, finishedAt is set

### 2. Task Lifecycle (Failure and Retry)

- Submit a task with maxAttempts = 3
- Register a worker, claim the task
- Fail the task with an error message, verify 204 response
- Retrieve the task and verify status is PENDING (requeued for retry), lastError is set, attempts = 1
- Wait for the backoff period, claim the task again, verify attempts = 2
- Fail the task again
- Wait for the backoff period, claim the task again, verify attempts = 3
- Fail the task a third time
- Retrieve the task and verify status is DEAD, lastError is set

### 3. Claim Atomicity

- Submit 10 tasks to a queue
- Register two workers on the same queue
- Both workers claim with maxTasks = 10 concurrently (parallel HTTP calls)
- Verify each task was claimed by exactly one worker (no duplicates across the two responses)
- Verify all 10 tasks are accounted for across both claim responses

### 4. Priority Ordering

- Submit 3 tasks to the same queue with priorities 1, 5, and 10
- Register a worker and claim with maxTasks = 3
- Verify tasks are returned in priority order: 10, 5, 1

### 5. FIFO Within Same Priority

- Submit 3 tasks to the same queue with equal priority, slight delay between each submission
- Register a worker and claim with maxTasks = 3
- Verify tasks are returned in submission order (earliest first)

### 6. Claim Returns Empty When No Work Available

- Register a worker
- Claim tasks from a queue with no pending tasks
- Verify response contains an empty task list (not a 404 or error)

### 7. Worker Registration (Idempotent)

- Register a worker with a given ID and queue
- Register the same worker again with the same parameters
- Verify no error and the worker list shows exactly one entry for that ID

### 8. Worker Heartbeat

- Register a worker
- Send a heartbeat, verify 204 response
- Send a heartbeat for a non-existent worker ID, verify 404 response

### 9. Worker Death and Task Requeue

- Submit a task, register a worker, claim the task
- Stop sending heartbeats and wait for the dead threshold (120 seconds + reaper interval)
- Verify the worker status transitions to DEAD
- Verify the task is requeued to PENDING (or DEAD if max attempts exhausted)
- Register a new worker, claim the task, verify it is claimable

Note: this test requires waiting for the reaper cycle. Use Awaitility with a generous timeout. For faster CI, the Taskmaster instance under test should be configured with shorter reaper thresholds.

### 10. Ownership Guard on Complete/Fail

- Submit a task, register worker A, claim the task
- Attempt to complete the task with worker B's ID, verify 409 Conflict response
- Attempt to fail the task with worker B's ID, verify 409 Conflict response
- Complete the task with worker A's ID, verify 204 success

### 11. Task Submission Validation

- Submit a task with missing queueName, verify 400 Bad Request
- Submit a task with missing payload, verify 400 Bad Request
- Submit a task with maxAttempts = 0, verify 400 Bad Request
- Submit a valid task with only required fields (no priority, no deadline, no maxAttempts), verify defaults are applied

### 12. Claim Validation

- Claim with maxTasks = 0, verify 400 Bad Request
- Claim with maxTasks = 101, verify 400 Bad Request
- Claim with missing workerId, verify 400 Bad Request

### 13. Queue Stats

- Submit multiple tasks to two different queues, complete some, fail some
- Call the queue stats endpoint
- Verify per-queue counts for pending, running, failed, and dead statuses

### 14. Filtered Task Listing

- Submit tasks to multiple queues with varying statuses
- List tasks filtered by queue, verify only tasks from that queue are returned
- List tasks filtered by status, verify only tasks with that status are returned
- List tasks with a limit, verify the response does not exceed the limit
- Verify tasks are returned in reverse chronological order (most recent first)

### 15. Task Deadline

- Submit a task with a deadline set to a few seconds in the future
- Do not claim the task
- Wait for the deadline to pass plus a DeadlineReaper cycle (30 seconds)
- Retrieve the task and verify it has been moved to DEAD

### 16. Get Task Not Found

- Request a task by a random UUID that does not exist
- Verify 404 response

### 17. Multiple Queue Isolation

- Submit a task to queue A
- Register a worker on queue B and attempt to claim
- Verify the claim returns empty (worker on queue B cannot see queue A tasks)

## Non-Goals

- These tests do not test internal implementation details (no Spring context, no repository calls, no mocking).
- These tests do not test performance or load characteristics.
- These tests do not cover features from other specs (webhook callbacks, tags, TTL, complexity, dead letter replay). Each feature spec should define its own test scenarios, and those tests should be added to this suite as the features are implemented.
