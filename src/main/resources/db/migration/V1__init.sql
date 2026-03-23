CREATE TABLE workers (
  id               TEXT        PRIMARY KEY,
  queue_name       TEXT        NOT NULL,
  max_concurrency  INT         NOT NULL DEFAULT 4,
  tags             TEXT[]      NOT NULL DEFAULT '{}',
  registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat   TIMESTAMPTZ NOT NULL DEFAULT now(),
  status           TEXT        NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE tasks (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_name       TEXT        NOT NULL,
  payload          JSONB       NOT NULL,
  priority         INT         NOT NULL DEFAULT 0,
  status           TEXT        NOT NULL DEFAULT 'PENDING',
  worker_id        TEXT        REFERENCES workers(id),
  attempts         INT         NOT NULL DEFAULT 0,
  max_attempts     INT         NOT NULL DEFAULT 3,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,
  next_attempt_at  TIMESTAMPTZ,
  result           TEXT,
  last_error       TEXT,
  deadline         TIMESTAMPTZ,
  callback_url     TEXT,
  tags             TEXT[]      NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_tasks_claimable
  ON tasks (queue_name, priority DESC, created_at)
  WHERE status = 'PENDING';

CREATE INDEX idx_tasks_running_worker
  ON tasks (worker_id)
  WHERE status = 'RUNNING';

CREATE INDEX idx_workers_heartbeat
  ON workers (last_heartbeat)
  WHERE status = 'ACTIVE';

CREATE INDEX idx_tasks_deadline
  ON tasks (deadline)
  WHERE status = 'PENDING' AND deadline IS NOT NULL;

CREATE INDEX idx_tasks_tags
  ON tasks USING GIN (tags)
  WHERE status = 'PENDING';
