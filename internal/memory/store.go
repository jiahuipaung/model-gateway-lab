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
	pollLeases     map[string]lease
	pollAfter      map[string]time.Time
	events         map[string]controlplane.Event
	deliveries     map[string]delivery
}

type lease struct {
	owner     string
	expiresAt time.Time
}

type delivery struct {
	state       string
	lease       lease
	attempts    int
	availableAt time.Time
}

func NewStore() *Store {
	return &Store{
		tasks:          make(map[string]task.Task),
		idempotencyIDs: make(map[string]string),
		submitLeases:   make(map[string]lease),
		unknownLeases:  make(map[string]lease),
		pollLeases:     make(map[string]lease),
		pollAfter:      make(map[string]time.Time),
		events:         make(map[string]controlplane.Event),
		deliveries:     make(map[string]delivery),
	}
}

func (s *Store) ClaimPoll(_ context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.PollClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.orderedPollTaskIDs() {
		record := s.tasks[id]
		currentLease := s.pollLeases[id]
		claimableState := record.SubmissionState == task.SubmissionConfirmed &&
			(record.ExecutionState == task.ExecutionQueued || record.ExecutionState == task.ExecutionRunning)
		if !claimableState || currentLease.expiresAt.After(now) || s.pollAfter[id].After(now) {
			continue
		}
		token := controlplane.NewClaimToken(owner)
		s.pollLeases[id] = lease{owner: token, expiresAt: now.Add(ttl)}
		return controlplane.PollClaim{Task: record, LeaseOwner: token}, true, nil
	}
	return controlplane.PollClaim{}, false, nil
}

func (s *Store) CompletePoll(_ context.Context, completion controlplane.PollCompletion) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.tasks[completion.TaskID]
	currentLease := s.pollLeases[completion.TaskID]
	if !ok || currentLease.owner == "" || currentLease.owner != completion.LeaseOwner || record.SubmissionState != task.SubmissionConfirmed {
		return controlplane.ErrLeaseLost
	}

	switch completion.Outcome {
	case controlplane.PollObservedQueued:
		if !record.ExecutionState.CanTransitionTo(task.ExecutionQueued) {
			delete(s.pollLeases, completion.TaskID)
			return nil
		}
		record.ExecutionState = task.ExecutionQueued
	case controlplane.PollObservedRunning:
		if !record.ExecutionState.CanTransitionTo(task.ExecutionRunning) {
			delete(s.pollLeases, completion.TaskID)
			return nil
		}
		record.ExecutionState = task.ExecutionRunning
	case controlplane.PollObservedSucceeded:
		if !record.ExecutionState.CanTransitionTo(task.ExecutionSucceeded) {
			delete(s.pollLeases, completion.TaskID)
			return nil
		}
		record.ExecutionState = task.ExecutionSucceeded
		record.OutputRef = completion.OutputRef
	case controlplane.PollObservedFailed:
		if !record.ExecutionState.CanTransitionTo(task.ExecutionFailed) {
			delete(s.pollLeases, completion.TaskID)
			return nil
		}
		record.ExecutionState = task.ExecutionFailed
		record.FailureCode = completion.FailureCode
	case controlplane.PollObservationRetry:
		delete(s.pollLeases, completion.TaskID)
		s.pollAfter[completion.TaskID] = completion.NextPollAt
		return nil
	default:
		return controlplane.ErrInvalidRequest
	}

	record.Version++
	record.UpdatedAt = time.Now().UTC()
	s.tasks[completion.TaskID] = record
	delete(s.pollLeases, completion.TaskID)
	if record.ExecutionState == task.ExecutionQueued || record.ExecutionState == task.ExecutionRunning {
		s.pollAfter[completion.TaskID] = completion.NextPollAt
	} else {
		delete(s.pollAfter, completion.TaskID)
	}
	if record.ExecutionState == task.ExecutionSucceeded || record.ExecutionState == task.ExecutionFailed {
		event := controlplane.TerminalEvent(record)
		s.events[event.ID] = event
		if _, exists := s.deliveries[event.ID]; !exists {
			s.deliveries[event.ID] = delivery{state: "PENDING"}
		}
	}
	return nil
}

func (s *Store) ClaimOutbox(_ context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.OutboxClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.events))
	for id := range s.events {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		d := s.deliveries[id]
		if d.state == "SENT" || d.availableAt.After(now) || (d.state == "SENDING" && d.lease.expiresAt.After(now)) {
			continue
		}
		d.state = "SENDING"
		token := controlplane.NewClaimToken(owner)
		d.lease = lease{owner: token, expiresAt: now.Add(ttl)}
		d.attempts++
		s.deliveries[id] = d
		return controlplane.OutboxClaim{Event: s.events[id], LeaseOwner: token, Attempts: d.attempts}, true, nil
	}
	return controlplane.OutboxClaim{}, false, nil
}

func (s *Store) MarkOutboxSent(_ context.Context, eventID, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[eventID]
	if !ok || d.state != "SENDING" || d.lease.owner != owner {
		return controlplane.ErrLeaseLost
	}
	d.state = "SENT"
	d.lease = lease{}
	s.deliveries[eventID] = d
	return nil
}

func (s *Store) ReleaseOutbox(_ context.Context, eventID, owner string, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[eventID]
	if !ok || d.state != "SENDING" || d.lease.owner != owner {
		return controlplane.ErrLeaseLost
	}
	d.state = "PENDING"
	d.lease = lease{}
	d.availableAt = retryAt
	s.deliveries[eventID] = d
	return nil
}

func (s *Store) ClaimSubmission(_ context.Context, owner string, now time.Time, ttl time.Duration) (controlplane.SubmissionClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.orderedTaskIDs() {
		record := s.tasks[id]
		claimable := record.SubmissionState == task.SubmissionPending
		if !claimable {
			continue
		}
		record.SubmissionState = task.SubmissionSubmitting
		record.Version++
		record.UpdatedAt = now.UTC()
		s.tasks[id] = record
		token := controlplane.NewClaimToken(owner)
		s.submitLeases[id] = lease{owner: token, expiresAt: now.Add(ttl)}
		return controlplane.SubmissionClaim{Task: record, LeaseOwner: token}, true, nil
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
		expiredSubmission := record.SubmissionState == task.SubmissionSubmitting && !s.submitLeases[id].expiresAt.After(now)
		claimableUnknown := record.SubmissionState == task.SubmissionUnknown && !currentLease.expiresAt.After(now)
		if !expiredSubmission && !claimableUnknown {
			continue
		}
		if expiredSubmission {
			record.SubmissionState = task.SubmissionUnknown
			record.Version++
			record.UpdatedAt = now.UTC()
			s.tasks[id] = record
			delete(s.submitLeases, id)
		}
		token := controlplane.NewClaimToken(owner)
		s.unknownLeases[id] = lease{owner: token, expiresAt: now.Add(ttl)}
		return controlplane.SubmissionClaim{Task: record, LeaseOwner: token}, true, nil
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

func (s *Store) orderedPollTaskIDs() []string {
	ids := s.orderedTaskIDs()
	sort.SliceStable(ids, func(i, j int) bool {
		left, right := s.pollAfter[ids[i]], s.pollAfter[ids[j]]
		if left.Equal(right) {
			return s.tasks[ids[i]].UpdatedAt.Before(s.tasks[ids[j]].UpdatedAt)
		}
		return left.Before(right)
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
