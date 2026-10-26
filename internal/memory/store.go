package memory

import (
	"context"
	"sync"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

type Store struct {
	mu             sync.Mutex
	tasks          map[string]task.Task
	idempotencyIDs map[string]string
}

func NewStore() *Store {
	return &Store{
		tasks:          make(map[string]task.Task),
		idempotencyIDs: make(map[string]string),
	}
}

func (s *Store) CreateTask(_ context.Context, candidate task.Task) (task.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := candidate.TenantID + "\x00" + candidate.IdempotencyKey
	if existingID, ok := s.idempotencyIDs[key]; ok {
		existing := s.tasks[existingID]
		if existing.RequestDigest != candidate.RequestDigest {
			return task.Task{}, false, controlplane.ErrIdempotencyConflict
		}
		return existing, true, nil
	}

	s.tasks[candidate.ID] = candidate
	s.idempotencyIDs[key] = candidate.ID
	return candidate, false, nil
}

func (s *Store) GetTask(_ context.Context, tenantID, taskID string) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.tasks[taskID]
	if !ok || record.TenantID != tenantID {
		return task.Task{}, controlplane.ErrNotFound
	}
	return record, nil
}
