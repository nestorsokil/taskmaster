# Task Tags and Worker Capabilities

## Problem

All workers on a queue are treated identically. There is no way to express that certain tasks require specific worker capabilities (e.g., GPU, high memory, a particular region, or access to a specific external service). The only routing mechanism is the queue name, which forces users to create separate queues for every capability dimension.

## Goal

Allow tasks to declare required tags and workers to declare their capabilities. A worker can only claim a task if it has all the tags the task requires. Tags provide soft affinity within a queue, complementing hard queue-based partitioning.

## Behavior

### Task Tags

When submitting a task, the producer may include a list of string tags representing requirements (e.g., `["gpu", "region:eu"]`). Tags are opaque to Taskmaster; it does not interpret their meaning. If no tags are provided, the task has no special requirements and any worker on the queue can claim it.

### Worker Capabilities

When registering, a worker may include a list of string tags representing its capabilities. A worker with no tags can only claim tasks that also have no tags.

### Claim Matching

During claim, a task is eligible for a worker only if every tag on the task is present in the worker's capability set. In other words, the task's tags must be a subset of the worker's capabilities. A worker with capabilities `["gpu", "region:eu", "high-mem"]` can claim a task tagged `["gpu", "region:eu"]` but not one tagged `["gpu", "region:us"]`.

### Tag Format

Tags are plain lowercase strings. Colons are allowed for namespacing (e.g., `region:eu`) but Taskmaster does not parse or enforce any structure. Maximum tag length: 64 characters. Maximum tags per task or worker: 16.

## Data Changes

- New column on the tasks table: `tags` (text array, default empty array)
- New column on the workers table: `tags` (text array, default empty array)
- Index on tasks tags column to support containment queries during claim

## API Changes

- `POST /tasks/v1` request body gains an optional `tags` field (list of strings)
- `POST /workers/v1/register` request body gains an optional `tags` field (list of strings)
- Claim query is updated to add a tag subset check
- `GET /tasks/v1/{id}` and worker list responses include tags

## Edge Cases

- A task with tags submitted to a queue where no worker has matching capabilities will sit PENDING until a capable worker registers (or until its deadline expires, if set).
- Updating a worker's tags requires re-registering. Since register is already idempotent/upsert, the worker calls register again with the new tag set.
- Tags are case-sensitive. Normalization (e.g., lowercasing) is the producer's responsibility.
