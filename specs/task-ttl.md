# Task TTL and Auto-Cleanup

## Problem

Completed (DONE) and dead-lettered (DEAD) tasks accumulate in the database indefinitely. Over time this degrades query performance, increases storage costs, and makes observability endpoints noisy.

## Goal

Automatically remove terminal tasks after a configurable retention period. Keep the database lean without manual intervention.

## Behavior

### Retention Policy

A global retention period is configured via application properties (e.g., `taskmaster.retention.ttl`). Default: 7 days. This applies to all tasks in a terminal state (DONE or DEAD) based on their `finished_at` timestamp.

### Cleanup Reaper

A new scheduled job runs periodically (default: every 10 minutes). On each run it deletes tasks where:
- status is DONE or DEAD
- `finished_at` is older than `now() - retention TTL`

Deletion is done in batches (default batch size: 500) to avoid long-running transactions and lock contention. The reaper loops until fewer rows than the batch size are deleted, then sleeps until the next cycle.

### Observability

- Metric: `tasks.cleaned` (counter, tagged by status: DONE or DEAD)
- Log: INFO with count of deleted tasks per reaper cycle (skip logging if count is zero)

## Configuration

| Property | Default | Description |
|---|---|---|
| `taskmaster.retention.ttl` | `7d` | How long terminal tasks are kept after finishing |
| `taskmaster.retention.interval-ms` | `600000` | How often the cleanup reaper runs (10 min) |
| `taskmaster.retention.batch-size` | `500` | Max rows deleted per batch within a cycle |

## Data Changes

None. Uses existing `finished_at` and `status` columns. An index on `(status, finished_at)` may be added if cleanup queries are slow on large tables.

## API Changes

None. This is purely an internal background process. Optionally, the queue stats endpoint could be extended in the future to report cleanup counts, but that is not in scope for this spec.

## Edge Cases

- Tasks still in PENDING, RUNNING, or FAILED state are never cleaned up regardless of age.
- If a task transitions to DONE/DEAD but has a webhook callback pending, the callback should fire before the TTL clock matters (callback retries complete within minutes; TTL is measured in days). No coordination needed.
- If retention TTL is set to zero or negative, cleanup is disabled.
- The reaper must be safe to run concurrently with other reapers (heartbeat, deadline). Since it only touches terminal tasks and they only touch non-terminal tasks, there is no conflict.
