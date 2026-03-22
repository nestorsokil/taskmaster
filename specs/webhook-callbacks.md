# Webhook Callbacks

## Problem

Producers that submit tasks have no way to know when a task completes without polling `GET /tasks/v1/{id}`. This adds latency, wastes resources, and forces producers to maintain polling loops.

## Goal

Allow producers to specify a callback URL at submission time. Taskmaster will POST task results to that URL when the task reaches a terminal state (DONE or DEAD).

## Behavior

### Submission

A new optional field `callbackUrl` is accepted on task submission. When present, Taskmaster stores it alongside the task. If absent, no callback is attempted (backwards-compatible).

### Callback Trigger

When a task transitions to DONE or DEAD, Taskmaster sends an HTTP POST to the stored callback URL. The request body contains the task ID, final status, result (if DONE), last error (if DEAD), queue name, and the number of attempts used.

### Callback Delivery

- Callbacks are delivered asynchronously. The worker's complete/fail response must not block on callback delivery.
- The callback POST includes an HMAC signature header computed from a shared secret so the receiver can verify authenticity. The secret is configured per Taskmaster instance (not per task).
- Taskmaster retries failed callback deliveries (non-2xx response or connection timeout) up to 3 times with exponential backoff (5s, 20s, 60s).
- After all retries are exhausted, the failure is logged at WARN level and a metric `webhooks.delivery_failed` is incremented with a `queue` tag. The task itself is not affected (its status remains DONE or DEAD).
- A configurable HTTP timeout (default 10 seconds) applies to each delivery attempt.

### Observability

- Metric: `webhooks.delivered` (counter, tagged by queue and status)
- Metric: `webhooks.delivery_failed` (counter, tagged by queue)
- Log: WARN on final delivery failure with task ID, callback URL, and HTTP status or error

## Data Changes

- New column on the tasks table: `callback_url` (nullable text)

## API Changes

- `POST /tasks/v1` request body gains an optional `callbackUrl` string field
- No changes to any other endpoints
- `GET /tasks/v1/{id}` response includes `callbackUrl` when set

## Edge Cases

- If the callback URL is unreachable for all retries, the task's terminal status is unaffected. The callback is best-effort.
- If Taskmaster restarts while a callback retry is pending, the callback may be lost. This is acceptable for v1. A future enhancement could persist pending callbacks to a table.
- Callbacks for tasks that reach DEAD via the DeadlineReaper should also fire.
- Callbacks for tasks that reach DEAD via HeartbeatReaper (worker death, max attempts exhausted) should also fire.
