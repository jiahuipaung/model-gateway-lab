package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *Store) Save(ctx context.Context, requestKey, remoteID string, shouldFail bool) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO fake_provider_tasks (request_key, remote_id, should_fail)
        VALUES ($1, $2, $3)
        ON CONFLICT (request_key) DO NOTHING`, requestKey, remoteID, shouldFail)
	if err != nil {
		return fmt.Errorf("save fake provider task: %w", err)
	}
	return nil
}

func (s *Store) Find(ctx context.Context, requestKey string) (string, bool, error) {
	var remoteID string
	err := s.pool.QueryRow(ctx, `
        SELECT remote_id FROM fake_provider_tasks WHERE request_key = $1`, requestKey).Scan(&remoteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find fake provider task: %w", err)
	}
	return remoteID, true, nil
}

func (s *Store) AdvancePoll(ctx context.Context, remoteID string) (int, bool, bool, error) {
	var pollCount int
	var shouldFail bool
	err := s.pool.QueryRow(ctx, `
        UPDATE fake_provider_tasks SET poll_count = poll_count + 1
        WHERE remote_id = $1
        RETURNING poll_count, should_fail`, remoteID).Scan(&pollCount, &shouldFail)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, fmt.Errorf("advance fake provider poll: %w", err)
	}
	return pollCount, shouldFail, true, nil
}
