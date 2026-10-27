package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	schema, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(748126149201)`); err != nil {
		return fmt.Errorf("lock migration: %w", err)
	}
	if _, err := tx.Exec(ctx, string(schema)); err != nil {
		return fmt.Errorf("apply migration: %w", err)
	}
	return commit(ctx, tx)
}

func (s *Store) CreateTask(ctx context.Context, candidate task.Task) (task.Task, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return task.Task{}, false, fmt.Errorf("begin create task: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var ledgerTaskID, ledgerDigest string
	err = tx.QueryRow(ctx, `
        INSERT INTO request_ledger (tenant_id, idempotency_key, request_digest, task_id, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
        RETURNING task_id, request_digest`,
		candidate.TenantID, candidate.IdempotencyKey, candidate.RequestDigest, candidate.ID, candidate.CreatedAt,
	).Scan(&ledgerTaskID, &ledgerDigest)
	inserted := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
            SELECT task_id, request_digest FROM request_ledger
            WHERE tenant_id = $1 AND idempotency_key = $2`, candidate.TenantID, candidate.IdempotencyKey,
		).Scan(&ledgerTaskID, &ledgerDigest)
	}
	if err != nil {
		return task.Task{}, false, fmt.Errorf("write request ledger: %w", err)
	}
	if !inserted {
		if ledgerDigest != candidate.RequestDigest {
			return task.Task{}, false, controlplane.ErrIdempotencyConflict
		}
		existing, err := getTaskByID(ctx, tx, ledgerTaskID)
		if err != nil {
			return task.Task{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return task.Task{}, false, fmt.Errorf("commit replay: %w", err)
		}
		return existing, true, nil
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO tasks (
            id, tenant_id, idempotency_key, request_digest, provider_request_key,
            provider, model, operation, prompt, submission_state, execution_state,
            provider_task_id, output_ref, failure_code, version, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
            NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), $15, $16, $17
        )`,
		candidate.ID, candidate.TenantID, candidate.IdempotencyKey, candidate.RequestDigest, candidate.ProviderRequestKey,
		candidate.Spec.Provider, candidate.Spec.Model, candidate.Spec.Operation, candidate.Spec.Prompt,
		candidate.SubmissionState, candidate.ExecutionState, candidate.ProviderTaskID, candidate.OutputRef,
		candidate.FailureCode, candidate.Version, candidate.CreatedAt, candidate.UpdatedAt,
	)
	if err != nil {
		return task.Task{}, false, fmt.Errorf("insert task: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return task.Task{}, false, fmt.Errorf("commit create task: %w", err)
	}
	return candidate, false, nil
}

func (s *Store) GetTask(ctx context.Context, tenantID, taskID string) (task.Task, error) {
	record, err := scanTask(s.pool.QueryRow(ctx, taskSelect+` WHERE id = $1 AND tenant_id = $2`, taskID, tenantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return task.Task{}, controlplane.ErrNotFound
	}
	if err != nil {
		return task.Task{}, fmt.Errorf("get task: %w", err)
	}
	return record, nil
}

func (s *Store) ClaimSubmission(ctx context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.SubmissionClaim, bool, error) {
	token := controlplane.NewClaimToken(owner)
	query := `WITH candidate AS (
        SELECT id FROM tasks
        WHERE submission_state = 'PENDING'
        ORDER BY created_at, id
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE tasks t SET submission_state = 'SUBMITTING', submit_lease_owner = $1,
        submit_lease_expires_at = $3, version = version + 1, updated_at = $2
    FROM candidate c WHERE t.id = c.id
    RETURNING ` + taskColumns("t")
	record, err := scanTask(s.pool.QueryRow(ctx, query, token, now, now.Add(ttl)))
	if errors.Is(err, pgx.ErrNoRows) {
		return controlplane.SubmissionClaim{}, false, nil
	}
	if err != nil {
		return controlplane.SubmissionClaim{}, false, fmt.Errorf("claim submission: %w", err)
	}
	return controlplane.SubmissionClaim{Task: record, LeaseOwner: token}, true, nil
}

func (s *Store) CompleteSubmission(ctx context.Context, completion controlplane.SubmissionCompletion) error {
	var submissionState task.SubmissionState
	executionState := task.ExecutionNone
	switch completion.Outcome {
	case controlplane.SubmissionAccepted:
		if completion.ProviderTaskID == "" {
			return controlplane.ErrInvalidRequest
		}
		submissionState, executionState = task.SubmissionConfirmed, task.ExecutionQueued
	case controlplane.SubmissionDenied:
		submissionState = task.SubmissionRejected
	case controlplane.SubmissionAmbiguous:
		submissionState = task.SubmissionUnknown
	default:
		return controlplane.ErrInvalidRequest
	}
	tag, err := s.pool.Exec(ctx, `
        UPDATE tasks SET submission_state = $1, execution_state = $2,
            provider_task_id = NULLIF($3, ''), failure_code = NULLIF($4, ''),
            submit_lease_owner = NULL, submit_lease_expires_at = NULL,
            version = version + 1, updated_at = NOW()
        WHERE id = $5 AND submission_state = 'SUBMITTING' AND submit_lease_owner = $6`,
		submissionState, executionState, completion.ProviderTaskID, completion.FailureCode,
		completion.TaskID, completion.LeaseOwner,
	)
	if err != nil {
		return fmt.Errorf("complete submission: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.ErrLeaseLost
	}
	return nil
}

func (s *Store) ClaimUnknown(ctx context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.SubmissionClaim, bool, error) {
	token := controlplane.NewClaimToken(owner)
	query := `WITH candidate AS (
        SELECT id FROM tasks
        WHERE (submission_state = 'UNKNOWN'
          AND (unknown_lease_expires_at IS NULL OR unknown_lease_expires_at <= $2))
           OR (submission_state = 'SUBMITTING' AND submit_lease_expires_at <= $2)
        ORDER BY updated_at, id
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE tasks t SET
        submission_state = 'UNKNOWN',
        submit_lease_owner = NULL,
        submit_lease_expires_at = NULL,
        unknown_lease_owner = $1,
        unknown_lease_expires_at = $3,
        version = version + CASE WHEN t.submission_state = 'SUBMITTING' THEN 1 ELSE 0 END,
        updated_at = CASE WHEN t.submission_state = 'SUBMITTING' THEN $2 ELSE t.updated_at END
    FROM candidate c WHERE t.id = c.id
    RETURNING ` + taskColumns("t")
	record, err := scanTask(s.pool.QueryRow(ctx, query, token, now, now.Add(ttl)))
	if errors.Is(err, pgx.ErrNoRows) {
		return controlplane.SubmissionClaim{}, false, nil
	}
	if err != nil {
		return controlplane.SubmissionClaim{}, false, fmt.Errorf("claim unknown submission: %w", err)
	}
	return controlplane.SubmissionClaim{Task: record, LeaseOwner: token}, true, nil
}

func (s *Store) ResolveUnknown(ctx context.Context, resolution controlplane.UnknownResolution) error {
	var state task.SubmissionState
	execution := task.ExecutionNone
	switch resolution.Outcome {
	case controlplane.ReconciliationFound:
		if resolution.ProviderTaskID == "" {
			return controlplane.ErrInvalidRequest
		}
		state, execution = task.SubmissionConfirmed, task.ExecutionQueued
	case controlplane.ReconciliationAbsent:
		state = task.SubmissionPending
	case controlplane.ReconciliationInconclusive:
		state = task.SubmissionUnknown
	default:
		return controlplane.ErrInvalidRequest
	}
	tag, err := s.pool.Exec(ctx, `
        UPDATE tasks SET submission_state = $1, execution_state = $2,
            provider_task_id = CASE WHEN $3 = '' THEN provider_task_id ELSE $3 END,
            unknown_lease_owner = NULL, unknown_lease_expires_at = NULL,
            version = version + 1, updated_at = NOW()
        WHERE id = $4 AND submission_state = 'UNKNOWN' AND unknown_lease_owner = $5`,
		state, execution, resolution.ProviderTaskID, resolution.TaskID, resolution.LeaseOwner,
	)
	if err != nil {
		return fmt.Errorf("resolve unknown submission: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.ErrLeaseLost
	}
	return nil
}

func (s *Store) ClaimPoll(ctx context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.PollClaim, bool, error) {
	token := controlplane.NewClaimToken(owner)
	query := `WITH candidate AS (
        SELECT id FROM tasks
        WHERE submission_state = 'CONFIRMED'
          AND execution_state IN ('QUEUED', 'RUNNING')
          AND (poll_lease_expires_at IS NULL OR poll_lease_expires_at <= $2)
          AND poll_available_at <= $2
        ORDER BY poll_available_at, updated_at, id
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE tasks t SET poll_lease_owner = $1, poll_lease_expires_at = $3
    FROM candidate c WHERE t.id = c.id
    RETURNING ` + taskColumns("t")
	record, err := scanTask(s.pool.QueryRow(ctx, query, token, now, now.Add(ttl)))
	if errors.Is(err, pgx.ErrNoRows) {
		return controlplane.PollClaim{}, false, nil
	}
	if err != nil {
		return controlplane.PollClaim{}, false, fmt.Errorf("claim poll: %w", err)
	}
	return controlplane.PollClaim{Task: record, LeaseOwner: token}, true, nil
}

func (s *Store) CompletePoll(ctx context.Context, completion controlplane.PollCompletion) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin poll completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, err := scanTask(tx.QueryRow(ctx, taskSelect+`
        WHERE id = $1 AND submission_state = 'CONFIRMED' AND poll_lease_owner = $2 FOR UPDATE`,
		completion.TaskID, completion.LeaseOwner))
	if errors.Is(err, pgx.ErrNoRows) {
		return controlplane.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("lock polled task: %w", err)
	}
	if completion.Outcome == controlplane.PollObservationRetry {
		if _, err := tx.Exec(ctx, `UPDATE tasks SET poll_lease_owner = NULL, poll_lease_expires_at = NULL,
            poll_available_at = $2 WHERE id = $1`, completion.TaskID, completion.NextPollAt); err != nil {
			return fmt.Errorf("release poll lease: %w", err)
		}
		return commit(ctx, tx)
	}

	var nextState task.ExecutionState
	switch completion.Outcome {
	case controlplane.PollObservedQueued:
		nextState = task.ExecutionQueued
	case controlplane.PollObservedRunning:
		nextState = task.ExecutionRunning
	case controlplane.PollObservedSucceeded:
		nextState, record.OutputRef = task.ExecutionSucceeded, completion.OutputRef
	case controlplane.PollObservedFailed:
		nextState, record.FailureCode = task.ExecutionFailed, completion.FailureCode
	default:
		return controlplane.ErrInvalidRequest
	}
	if !record.ExecutionState.CanTransitionTo(nextState) {
		if _, err := tx.Exec(ctx, `UPDATE tasks SET poll_lease_owner = NULL, poll_lease_expires_at = NULL,
            poll_available_at = $2 WHERE id = $1`, completion.TaskID, completion.NextPollAt); err != nil {
			return fmt.Errorf("release regressive poll observation: %w", err)
		}
		return commit(ctx, tx)
	}
	record.ExecutionState = nextState
	record.UpdatedAt = time.Now().UTC()
	tag, err := tx.Exec(ctx, `
        UPDATE tasks SET execution_state = $1, output_ref = NULLIF($2, ''), failure_code = NULLIF($3, ''),
            poll_lease_owner = NULL, poll_lease_expires_at = NULL, poll_available_at = $7,
            version = version + 1, updated_at = $4
        WHERE id = $5 AND poll_lease_owner = $6`, record.ExecutionState, record.OutputRef,
		record.FailureCode, record.UpdatedAt, record.ID, completion.LeaseOwner, completion.NextPollAt)
	if err != nil {
		return fmt.Errorf("update polled task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.ErrLeaseLost
	}
	if record.ExecutionState == task.ExecutionSucceeded || record.ExecutionState == task.ExecutionFailed {
		event := controlplane.TerminalEvent(record)
		if _, err := tx.Exec(ctx, `
            INSERT INTO outbox_events (event_id, event_type, task_id, tenant_id, payload, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id) DO NOTHING`, event.ID, event.Type, event.TaskID, event.TenantID, event.Payload, event.CreatedAt); err != nil {
			return fmt.Errorf("insert terminal event: %w", err)
		}
	}
	return commit(ctx, tx)
}

func (s *Store) ClaimOutbox(ctx context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.OutboxClaim, bool, error) {
	token := controlplane.NewClaimToken(owner)
	row := s.pool.QueryRow(ctx, `WITH candidate AS (
        SELECT event_id FROM outbox_events
        WHERE available_at <= $2
          AND (status = 'PENDING' OR (status = 'SENDING' AND lease_expires_at <= $2))
        ORDER BY available_at, created_at, event_id
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE outbox_events o SET status = 'SENDING', lease_owner = $1,
        lease_expires_at = $3, attempts = attempts + 1
    FROM candidate c WHERE o.event_id = c.event_id
    RETURNING o.event_id, o.event_type, o.task_id, o.tenant_id, o.payload, o.created_at, o.attempts`,
		token, now, now.Add(ttl))
	var claim controlplane.OutboxClaim
	claim.LeaseOwner = token
	if err := row.Scan(&claim.Event.ID, &claim.Event.Type, &claim.Event.TaskID, &claim.Event.TenantID,
		&claim.Event.Payload, &claim.Event.CreatedAt, &claim.Attempts); errors.Is(err, pgx.ErrNoRows) {
		return controlplane.OutboxClaim{}, false, nil
	} else if err != nil {
		return controlplane.OutboxClaim{}, false, fmt.Errorf("claim outbox: %w", err)
	}
	return claim, true, nil
}

func (s *Store) MarkOutboxSent(ctx context.Context, eventID, owner string) error {
	tag, err := s.pool.Exec(ctx, `
        UPDATE outbox_events SET status = 'SENT', lease_owner = NULL, lease_expires_at = NULL, sent_at = NOW()
        WHERE event_id = $1 AND status = 'SENDING' AND lease_owner = $2`, eventID, owner)
	if err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.ErrLeaseLost
	}
	return nil
}

func (s *Store) ReleaseOutbox(ctx context.Context, eventID, owner string, retryAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
        UPDATE outbox_events SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL, available_at = $3
        WHERE event_id = $1 AND status = 'SENDING' AND lease_owner = $2`, eventID, owner, retryAt)
	if err != nil {
		return fmt.Errorf("release outbox: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return controlplane.ErrLeaseLost
	}
	return nil
}

const taskSelect = `SELECT ` + taskColumnList + ` FROM tasks`

const taskColumnList = `id, tenant_id, idempotency_key, request_digest, provider_request_key,
provider, model, operation, prompt, submission_state, execution_state,
COALESCE(provider_task_id, ''), COALESCE(output_ref, ''), COALESCE(failure_code, ''),
version, created_at, updated_at`

func taskColumns(alias string) string {
	return alias + `.id, ` + alias + `.tenant_id, ` + alias + `.idempotency_key, ` + alias + `.request_digest, ` + alias + `.provider_request_key,
` + alias + `.provider, ` + alias + `.model, ` + alias + `.operation, ` + alias + `.prompt, ` + alias + `.submission_state, ` + alias + `.execution_state,
COALESCE(` + alias + `.provider_task_id, ''), COALESCE(` + alias + `.output_ref, ''), COALESCE(` + alias + `.failure_code, ''),
` + alias + `.version, ` + alias + `.created_at, ` + alias + `.updated_at`
}

type rowScanner interface {
	Scan(...any) error
}

func scanTask(row rowScanner) (task.Task, error) {
	var record task.Task
	err := row.Scan(
		&record.ID, &record.TenantID, &record.IdempotencyKey, &record.RequestDigest, &record.ProviderRequestKey,
		&record.Spec.Provider, &record.Spec.Model, &record.Spec.Operation, &record.Spec.Prompt,
		&record.SubmissionState, &record.ExecutionState, &record.ProviderTaskID, &record.OutputRef,
		&record.FailureCode, &record.Version, &record.CreatedAt, &record.UpdatedAt,
	)
	return record, err
}

func getTaskByID(ctx context.Context, tx pgx.Tx, taskID string) (task.Task, error) {
	record, err := scanTask(tx.QueryRow(ctx, taskSelect+` WHERE id = $1`, taskID))
	if err != nil {
		return task.Task{}, fmt.Errorf("get replayed task: %w", err)
	}
	return record, nil
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
