CREATE TABLE IF NOT EXISTS request_ledger (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    task_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    provider_request_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    operation TEXT NOT NULL,
    prompt TEXT NOT NULL,
    submission_state TEXT NOT NULL,
    execution_state TEXT NOT NULL,
    provider_task_id TEXT,
    output_ref TEXT,
    failure_code TEXT,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    submit_lease_owner TEXT,
    submit_lease_expires_at TIMESTAMPTZ,
    unknown_lease_owner TEXT,
    unknown_lease_expires_at TIMESTAMPTZ,
    poll_lease_owner TEXT,
    poll_lease_expires_at TIMESTAMPTZ,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS tasks_submit_claim_idx
    ON tasks (submission_state, submit_lease_expires_at, created_at);
CREATE INDEX IF NOT EXISTS tasks_unknown_claim_idx
    ON tasks (submission_state, unknown_lease_expires_at, created_at);
CREATE INDEX IF NOT EXISTS tasks_poll_claim_idx
    ON tasks (submission_state, execution_state, poll_lease_expires_at, created_at);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    tenant_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_claim_idx
    ON outbox_events (status, lease_expires_at, created_at);

