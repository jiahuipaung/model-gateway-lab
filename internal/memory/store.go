package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

type Store struct {
	mu             sync.Mutex
	tasks          map[string]task.Task
	idempotencyIDs map[string]string
	submitLeases   map[string]lease
	unknownLeases  map[string]lease
}

type lease struct {
	owner     string
	expiresAt time.Time
}

func NewStore() *Store {
	return &Store{
		tasks:          make(map[string]task.Task),
		idempotencyIDs: make(map[string]string),
		submitLeases:   make(map[string]lease),
		unknownLeases:  make(map[string]lease),
	}
}

func (s *Store) ClaimSubmission(_ context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.SubmissionClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.orderedTaskIDs() {
		record := s.tasks[id]
		currentLease := s.submitLeases[id]
		claimable := record.SubmissionState == task.SubmissionPending ||
			(record.SubmissionState == task.SubmissionSubmitting && !currentLease.expiresAt.After(now))
		if !claimable {
			continue
		}
		if record.SubmissionState == task.SubmissionPending {
			record.SubmissionState = task.SubmissionSubmitting
		}
		record.Version++
		record.UpdatedAt = now.UTC()
		s.tasks[id] = record
		s.submitLeases[id] = lease{owner: owner, expiresAt: now.Add(ttl)}
		return controlplane.SubmissionClaim{Task: record, LeaseOwner: owner}, true, nil
	}
	return controlplane.SubmissionClaim{}, false, nil
}

func (s *Store) CompleteSubmission(_ context.Context, completion controlplane.SubmissionCompletion) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.tasks[completion.TaskID]
	currentLease := s.submitLeases[completion.TaskID]
	if !ok || currentLease.owner == "" || currentLease.owner != completion.LeaseOwner || record.SubmissionState != task.SubmissionSubmitting {
		return controlplane.ErrLeaseLost
	}

	switch completion.Outcome {
	case controlplane.SubmissionAccepted:
		if completion.ProviderTaskID == "" {
			return controlplane.ErrInvalidRequest
		}
		record.SubmissionState = task.SubmissionConfirmed
		record.ExecutionState = task.ExecutionQueued
		record.ProviderTaskID = completion.ProviderTaskID
	case controlplane.SubmissionDenied:
		record.SubmissionState = task.SubmissionRejected
		record.FailureCode = completion.FailureCode
	case controlplane.SubmissionAmbiguous:
		record.SubmissionState = task.SubmissionUnknown
	default:
		return controlplane.ErrInvalidRequest
	}
	record.Version++
	record.UpdatedAt = time.Now().UTC()
	s.tasks[completion.TaskID] = record
	delete(s.submitLeases, completion.TaskID)
	return nil
}

func (s *Store) ClaimUnknown(_ context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.SubmissionClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.orderedTaskIDs() {
		record := s.tasks[id]
		currentLease := s.unknownLeases[id]
		if record.SubmissionState != task.SubmissionUnknown || currentLease.expiresAt.After(now) {
			continue
		}
		s.unknownLeases[id] = lease{owner: owner, expiresAt: now.Add(ttl)}
		return controlplane.SubmissionClaim{Task: record, LeaseOwner: owner}, true, nil
	}
	return controlplane.SubmissionClaim{}, false, nil
}

func (s *Store) ResolveUnknown(_ context.Context, resolution controlplane.UnknownResolution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.tasks[resolution.TaskID]
	currentLease := s.unknownLeases[resolution.TaskID]
	if !ok || currentLease.owner == "" || currentLease.owner != resolution.LeaseOwner || record.SubmissionState != task.SubmissionUnknown {
		return controlplane.ErrLeaseLost
	}

	switch resolution.Outcome {
	case controlplane.ReconciliationFound:
		if resolution.ProviderTaskID == "" {
			return controlplane.ErrInvalidRequest
		}
		record.SubmissionState = task.SubmissionConfirmed
		record.ExecutionState = task.ExecutionQueued
		record.ProviderTaskID = resolution.ProviderTaskID
	case controlplane.ReconciliationAbsent:
		record.SubmissionState = task.SubmissionPending
	case controlplane.ReconciliationInconclusive:
		// UNKNOWN remains authoritative until a later lookup proves otherwise.
	default:
		return controlplane.ErrInvalidRequest
	}
	record.Version++
	record.UpdatedAt = time.Now().UTC()
	s.tasks[resolution.TaskID] = record
	delete(s.unknownLeases, resolution.TaskID)
	return nil
}

func (s *Store) orderedTaskIDs() []string {
	ids := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.tasks[ids[i]], s.tasks[ids[j]]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return ids[i] < ids[j]
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	return ids
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
