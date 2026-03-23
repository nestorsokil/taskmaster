# Taskmaster

A generic **task coordination service** — a durable task queue with worker liveness tracking. Works over a PostgreSQL database. Microservices submit tasks via REST; separate worker processes poll to claim and execute them. Taskmaster owns no business logic: it tracks state, enforces exclusive ownership, and handles retries.

---

## Architecture

```mermaid
graph LR
    A[Producer Service] -- POST /tasks/v1 --> TM[Taskmaster]
    W1[Worker A] -- register / heartbeat / claim / complete --> TM
    W2[Worker B] -- register / heartbeat / claim / complete --> TM
    TM -- read/write --> DB[(PostgreSQL)]
    TM -- metrics --> M[Micrometer]
```

Producers and workers are completely decoupled — they only talk to Taskmaster over HTTP. Workers are stateless and horizontally scalable.

---

## Task lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING : submitted

    PENDING --> RUNNING : claimed by worker
    PENDING --> DEAD    : deadline exceeded

    RUNNING --> DONE    : worker reports success
    RUNNING --> PENDING : worker reports failure\n(retries remaining, backoff)
    RUNNING --> DEAD    : worker reports failure\n(max attempts exhausted)
    RUNNING --> PENDING : worker died (requeued by reaper)

    DONE --> [*]
    DEAD --> [*]
```

Tasks carry a **priority** (higher = claimed first) and a configurable **max attempts** (default 3) before being permanently dead-lettered. When a worker reports failure, the task goes back to PENDING with exponential backoff (`2^attempts × baseDelay`, capped at 5 minutes) if retries remain, or straight to DEAD if attempts are exhausted. Tasks may also carry an optional **deadline** — if still PENDING after that instant, the deadline reaper moves them to DEAD.

---

## Worker lifecycle

```mermaid
stateDiagram-v2
    [*]    --> ACTIVE : registers on startup

    ACTIVE --> STALE  : no heartbeat for > 30 s
    STALE  --> ACTIVE : heartbeat resumes
    ACTIVE --> DEAD   : no heartbeat for > 2 min
    STALE  --> DEAD   : no heartbeat for > 2 min

    DEAD --> [*] : running tasks requeued
```

Workers send a heartbeat every 10 seconds. A background reaper sweeps every 15 seconds, demoting silent workers and requeuing any tasks they were holding.

---

## Claim flow

```mermaid
sequenceDiagram
    participant W as Worker
    participant TM as Taskmaster
    participant DB as PostgreSQL

    W  ->> TM : POST /workers/v1/register
    TM ->> DB : upsert worker row

    loop every 10 s
        W  ->> TM : POST /workers/v1/{id}/heartbeat
        TM ->> DB : UPDATE last_heartbeat = now()
    end

    loop poll for work
        W  ->> TM : POST /tasks/v1/claim
        TM ->> DB : SELECT … FOR UPDATE SKIP LOCKED\nUPDATE status = RUNNING
        TM -->> W : claimed tasks + payloads

        alt success
            W  ->> TM : POST /tasks/v1/{id}/complete
            TM ->> DB : UPDATE status = DONE
        else failure
            W  ->> TM : POST /tasks/v1/{id}/fail
            TM ->> DB : UPDATE status = PENDING (backoff) / DEAD
        end
    end
```

Claiming is atomic (`FOR UPDATE SKIP LOCKED`) — two workers polling simultaneously will never receive the same task.

---

## Tags & capabilities

Tasks can carry **tags** (up to 16 strings) representing required capabilities. Workers register with their own set of tags advertising what they can handle. During a claim, Taskmaster only matches a task to a worker when the task's tags are a **subset** of the worker's tags (PostgreSQL `<@` array containment). Untagged tasks match any worker.

---

## Background jobs

| Job | Interval | Responsibility |
|---|---|---|
| **Heartbeat Reaper** | 15 s | Marks silent workers STALE → DEAD; requeues their tasks |
| **Deadline Reaper** | 30 s | Moves PENDING tasks past their deadline to DEAD |
| **Retention Reaper** | 10 min | Deletes terminal tasks (DONE/DEAD) and dead workers older than the configured TTL; runs in bounded batches |

---

## API surface

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks/v1` | Submit a new task |
| `GET` | `/tasks/v1/{id}` | Fetch task status |
| `GET` | `/tasks/v1` | List tasks (filterable by queue, status, limit) |
| `POST` | `/tasks/v1/claim` | Atomically claim up to N tasks |
| `POST` | `/tasks/v1/{id}/complete` | Mark a task done |
| `POST` | `/tasks/v1/{id}/fail` | Report failure; triggers retry or dead-letter |
| `POST` | `/workers/v1/register` | Register a worker (idempotent) |
| `POST` | `/workers/v1/{id}/heartbeat` | Refresh worker liveness |
| `GET` | `/workers/v1` | List all workers |
| `GET` | `/queues/v1` | Per-queue task counts and active worker count |

All error responses follow [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457).

All requests may carry an `X-Correlation-Id` header; Taskmaster propagates it through virtual-thread-scoped context via `ScopedValue` for end-to-end tracing.

---

## Configuration

| Property | Default | Description |
|---|---|---|
| `taskmaster.heartbeat.stale-threshold-seconds` | `30` | Seconds of silence before a worker is marked STALE |
| `taskmaster.heartbeat.dead-threshold-seconds` | `120` | Seconds of silence before a worker is marked DEAD |
| `taskmaster.reaper.interval-ms` | `15000` | How often the heartbeat reaper runs |
| `taskmaster.retry.base-delay-seconds` | `1` | Base multiplier for exponential backoff (`2^attempts × base`) |
| `taskmaster.retention.ttl` | `7d` | How long terminal tasks and dead workers are kept; zero disables cleanup |
| `taskmaster.retention.interval-ms` | `600000` | How often the retention reaper runs |
| `taskmaster.retention.batch-size` | `500` | Max rows deleted per batch within a single retention cycle |

---

## Metrics

Taskmaster exposes Prometheus metrics via `/actuator/prometheus`. All task metrics are tagged with `queue` for per-queue filtering.

### Counters

| Metric | Tags | Description |
|---|---|---|
| `tasks.submitted` | `queue` | Tasks submitted |
| `tasks.claimed` | `queue` | Tasks claimed by workers |
| `tasks.completed` | `queue` | Tasks completed successfully |
| `tasks.failed` | `queue` | Fail calls received (before retry/dead-letter decision) |
| `tasks.dead_lettered` | `queue`, `reason` | Tasks moved to terminal DEAD state (`exhausted`, `worker_dead`, `deadline`) |
| `tasks.requeued` | `reason` | Tasks returned to PENDING for retry |
| `tasks.cleaned` | — | Terminal tasks removed by retention reaper |
| `workers.registered` | `queue` | Worker registrations |
| `workers.died` | — | Workers marked DEAD |
| `workers.cleaned` | — | Dead workers removed by retention reaper |

### Timers

| Metric | Tags | Description |
|---|---|---|
| `tasks.queue.wait` | `queue` | Time from submission to claim (claimedAt − createdAt) |
| `tasks.execution.duration` | `queue` | Time from claim to completion/failure (finishedAt − claimedAt) |
| `tasks.end_to_end.duration` | `queue` | Total time from submission to terminal state, including retries |

---

## Docker

```bash
# Start everything (Taskmaster, PostgreSQL, Prometheus, Grafana)
docker compose up --build

# Taskmaster API:   http://localhost:8080
# Prometheus:       http://localhost:9090
# Grafana:          http://localhost:3000  (admin / admin)
```

A pre-provisioned Grafana dashboard ("Taskmaster Overview") is available on startup with panels for task throughput, dead letters, worker events, and latency breakdowns.

---

## Implementation notes

- **Java 25** with preview features enabled (sealed interfaces for `TaskStatus`)
- **Virtual threads** — all Tomcat request handlers and scheduled reapers run on virtual threads
- **Spring Data JDBC** (no JPA) — lightweight, explicit SQL with no lazy-loading surprises
- **Flyway** for schema migrations
- **Atomic claiming** — a single `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE` query ensures two workers polling simultaneously never receive the same task

## Code formatting

Taskmaster uses [OpenRewrite](https://docs.openrewrite.org/) to enforce Java formatting rules,
including braces around single-line control statements.

Format code locally with:

```bash
mvn rewrite:run
```

To enable the repo-managed pre-commit hook, point Git at the tracked hook directory:

```bash
git config core.hooksPath .githooks
```

The hook runs `mvn rewrite:run` for staged Java files, then re-stages the formatted files before the commit completes.
