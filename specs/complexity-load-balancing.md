# Complexity-Based Load Balancing

## Problem

All tasks are treated as equal cost. A worker that happens to claim several expensive tasks becomes overloaded while other workers sit idle. The existing `maxConcurrency` limit treats every task as "1 slot" regardless of actual cost.

## Goal

Allow producers to assign a complexity weight to tasks and let Taskmaster distribute work more evenly by considering each worker's current load (sum of complexity of its running tasks) relative to its declared capacity.

## Behavior

### Task Complexity

A new optional field `complexity` is accepted on task submission. It is a positive integer representing relative cost (default: 1). Taskmaster does not interpret the unit; it is up to the producer to define a consistent scale.

### Worker Capacity

The existing `maxConcurrency` field is reinterpreted as `capacity`: the total complexity budget a worker can handle concurrently. For backwards compatibility, the default remains 4, meaning a worker can handle 4 units of complexity. Workers using the old API with default-complexity tasks (complexity=1) see identical behavior to today (4 tasks = 4 units).

### Claim Logic

When a worker calls the claim endpoint, Taskmaster calculates the worker's current load: the sum of `complexity` for all tasks currently RUNNING and assigned to that worker. The number of tasks returned is limited so that the worker's load does not exceed its capacity.

Specifically: if a worker has capacity C and current load L, it can accept tasks whose cumulative complexity does not exceed C - L. If C - L is zero or negative, an empty list is returned even if PENDING tasks exist.

Tasks are still ordered by priority (descending) then creation time (ascending). Tasks are claimed greedily in order: if the next task's complexity would exceed remaining budget, skip it and try the next one. This avoids a situation where a single heavy task blocks all lighter tasks behind it.

### API Field Naming

The submit endpoint gains a `complexity` field. The register endpoint field remains named `maxConcurrency` in the API for backwards compatibility but is documented as "total complexity budget." A future API version may rename it to `capacity`.

## Data Changes

- New column on the tasks table: `complexity` (integer, not null, default 1)

## API Changes

- `POST /tasks/v1` request body gains an optional `complexity` field (integer, default 1, minimum 1)
- `POST /tasks/v1/claim` response items include the `complexity` value alongside existing fields
- `GET /tasks/v1/{id}` response includes `complexity`
- No changes to the worker registration API shape (the `maxConcurrency` field takes on expanded meaning)

## Observability

- Metric: `workers.current_load` (gauge, tagged by worker ID and queue) tracking the sum of complexity of running tasks per worker
- The queue stats endpoint could be extended to report total pending complexity, but this is not required for v1

## Edge Cases

- A task with complexity greater than any worker's capacity will never be claimed. This should be validated at submission time: if all currently registered workers on the target queue have capacity smaller than the task's complexity, return a warning (but still accept the task, since a capable worker may register later).
- If no workers are registered on the queue, complexity validation is skipped and the task is accepted normally.
- When a worker dies and its tasks are requeued, the load score for that worker drops to zero naturally (no running tasks). No special handling needed.
- Default complexity of 1 and default capacity of 4 means existing behavior is preserved exactly for users who do not opt into this feature.
