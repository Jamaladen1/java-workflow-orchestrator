CREATE TABLE workflow_definitions (
    id          UUID PRIMARY KEY,
    name        TEXT        NOT NULL,
    version     INTEGER     NOT NULL,
    definition  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_definition_name_version UNIQUE (name, version)
);

CREATE TABLE workflow_runs (
    id                      UUID PRIMARY KEY,
    workflow_definition_id  UUID        NOT NULL REFERENCES workflow_definitions (id),
    status                  TEXT        NOT NULL
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at              TIMESTAMPTZ,
    finished_at             TIMESTAMPTZ
);
CREATE INDEX idx_runs_status ON workflow_runs (status);
CREATE INDEX idx_runs_definition ON workflow_runs (workflow_definition_id, created_at DESC);

CREATE TABLE task_runs (
    id               UUID PRIMARY KEY,
    workflow_run_id  UUID        NOT NULL REFERENCES workflow_runs (id) ON DELETE CASCADE,
    task_key         TEXT        NOT NULL,
    task_type        TEXT        NOT NULL,
    config           JSONB       NOT NULL,
    depends_on       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status           TEXT        NOT NULL
        CHECK (status IN ('pending', 'ready', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
    attempts         INTEGER     NOT NULL DEFAULT 0,
    max_attempts     INTEGER     NOT NULL DEFAULT 3,
    result           JSONB,
    last_error       TEXT,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    next_run_at      TIMESTAMPTZ,
    CONSTRAINT uq_task_run_key UNIQUE (workflow_run_id, task_key)
);
CREATE INDEX idx_task_runs_ready ON task_runs (status, next_run_at) WHERE status = 'ready';

CREATE TABLE task_events (
    id               BIGSERIAL PRIMARY KEY,
    workflow_run_id  UUID        NOT NULL REFERENCES workflow_runs (id) ON DELETE CASCADE,
    task_run_id      UUID        REFERENCES task_runs (id) ON DELETE CASCADE,
    type             TEXT        NOT NULL,
    detail           JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_task_events_run ON task_events (workflow_run_id, id);
