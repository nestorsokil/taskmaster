# Dead Letter Replay

## Problem

Tasks that reach the DEAD state (max attempts exhausted or deadline expired) are stuck permanently. Operators must manually inspect failures, but there is no mechanism to retry a dead task after the underlying issue has been fixed. The only option today is to submit a new task with the same payload, losing the original task's history.

## Goal

Allow operators to resurrect dead tasks back to PENDING so they can be claimed and retried. Support both single-task replay and bulk replay by queue.

## Behavior

### Single Task Replay

A new endpoint accepts a task ID and moves it from DEAD back to PENDING. The task's attempt counter is reset to zero and max attempts is reset to its original value (or a new value if provided). The task re-enters the normal claim flow as if freshly submitted.

The original task ID is preserved so operators can correlate the retry with the original failure history. The `last_error` field is preserved (not cleared) so the previous failure reason remains visible.

### Bulk Replay

A separate endpoint replays all DEAD tasks in a given queue. It returns the count of tasks replayed. An optional filter by age allows replaying only recently dead-lettered tasks (e.g., "replay all DEAD tasks in queue X that died in the last hour").

### Guards

- Only tasks in DEAD status can be replayed. Attempting to replay a task in any other status returns a 409 Conflict.
- Replayed tasks go back to PENDING with `next_attempt_at` set to null (immediately eligible for claiming).
- The `finished_at` timestamp is cleared. A new `created_at` is NOT set; the original creation time is preserved.

## API Changes

### Single replay

- `POST /tasks/v1/{id}/replay`
- Request body (all fields optional):
  - `maxAttempts`: override the max attempts for the retried task. If omitted, reset to the original value.
- Response: 200 OK with the updated task summary
- Error: 404 if task not found, 409 if task is not in DEAD status

### Bulk replay

- `POST /tasks/v1/replay`
- Request body:
  - `queueName` (required): which queue to replay
  - `deadSince` (optional): only replay tasks whose `finished_at` is after this timestamp
  - `maxAttempts` (optional): override max attempts for all replayed tasks
- Response: 200 OK with `{ "replayed": <count> }`

## Data Changes

None. Uses existing columns. The replay operation is an UPDATE setting status to PENDING, resetting attempts to 0, clearing worker_id, claimed_at, finished_at, and next_attempt_at.

## Observability

- Metric: `tasks.replayed` (counter, tagged by queue)
- Log: INFO with task ID (single) or count and queue (bulk) on each replay

## Edge Cases

- Replaying a task that has a webhook callback URL: the callback will fire again if the task reaches a terminal state after replay. This is expected and correct.
- Replaying a task with an expired deadline: the DeadlineReaper will immediately move it back to DEAD on the next cycle. Operators should clear or extend the deadline. The replay endpoint should accept an optional `deadline` override for this reason.
- Bulk replay with no matching tasks returns `{ "replayed": 0 }`, not an error.
- Concurrent replays of the same task: the UPDATE uses a WHERE status = 'DEAD' guard, so only one replay succeeds. The second attempt returns 409.
