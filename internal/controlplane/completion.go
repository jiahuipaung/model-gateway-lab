package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

func TerminalEvent(record task.Task) Event {
	type payload struct {
		TaskID          string               `json:"task_id"`
		SubmissionState task.SubmissionState `json:"submission_state"`
		ExecutionState  task.ExecutionState  `json:"execution_state"`
		OutputRef       string               `json:"output_ref,omitempty"`
		FailureCode     string               `json:"failure_code,omitempty"`
	}
	body, err := json.Marshal(payload{
		TaskID: record.ID, SubmissionState: record.SubmissionState, ExecutionState: record.ExecutionState,
		OutputRef: record.OutputRef, FailureCode: record.FailureCode,
	})
	if err != nil {
		panic("terminal event payload is JSON-safe: " + err.Error())
	}
	sum := sha256.Sum256([]byte(record.ID + "\x00" + string(record.ExecutionState)))
	eventType := "task.succeeded"
	if record.ExecutionState == task.ExecutionFailed {
		eventType = "task.failed"
	}
	return Event{
		ID: "evt_" + hex.EncodeToString(sum[:16]), Type: eventType, TaskID: record.ID,
		TenantID: record.TenantID, Payload: body, CreatedAt: record.UpdatedAt,
	}
}

type Poller struct {
	repository    Repository
	providers     map[string]provider.Provider
	workerID      string
	leaseDuration time.Duration
	now           func() time.Time
}

const retryDelay = 500 * time.Millisecond

func NewPoller(repository Repository, providers map[string]provider.Provider, workerID string, leaseDuration time.Duration) *Poller {
	return &Poller{repository: repository, providers: providers, workerID: workerID, leaseDuration: leaseDuration, now: time.Now}
}

func (p *Poller) RunOnce(ctx context.Context) (bool, error) {
	now := p.now().UTC()
	claim, ok, err := p.repository.ClaimPoll(ctx, p.workerID, now, p.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}
	client, configured := p.providers[claim.Task.Spec.Provider]
	if !configured {
		return true, fmt.Errorf("provider %q is not configured", claim.Task.Spec.Provider)
	}

	result, pollErr := client.Poll(ctx, claim.Task.ProviderTaskID)
	completion := PollCompletion{TaskID: claim.Task.ID, LeaseOwner: claim.LeaseOwner, NextPollAt: now.Add(retryDelay)}
	if pollErr != nil {
		completion.Outcome = PollObservationRetry
	} else {
		switch result.Status {
		case provider.PollQueued:
			completion.Outcome = PollObservedQueued
		case provider.PollRunning:
			completion.Outcome = PollObservedRunning
		case provider.PollSucceeded:
			completion.Outcome = PollObservedSucceeded
			completion.OutputRef = result.OutputRef
		case provider.PollFailed:
			completion.Outcome = PollObservedFailed
			completion.FailureCode = result.FailureCode
		default:
			completion.Outcome = PollObservationRetry
		}
	}
	return true, p.repository.CompletePoll(ctx, completion)
}

type EventSink interface {
	Publish(context.Context, Event) error
}

type Dispatcher struct {
	repository    Repository
	sink          EventSink
	workerID      string
	leaseDuration time.Duration
	now           func() time.Time
}

func NewDispatcher(repository Repository, sink EventSink, workerID string, leaseDuration time.Duration) *Dispatcher {
	return &Dispatcher{repository: repository, sink: sink, workerID: workerID, leaseDuration: leaseDuration, now: time.Now}
}

func (d *Dispatcher) RunOnce(ctx context.Context) (bool, error) {
	now := d.now().UTC()
	claim, ok, err := d.repository.ClaimOutbox(ctx, d.workerID, now, d.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}
	if err := d.sink.Publish(ctx, claim.Event); err != nil {
		if releaseErr := d.repository.ReleaseOutbox(ctx, claim.Event.ID, claim.LeaseOwner, now.Add(retryDelay)); releaseErr != nil {
			return true, fmt.Errorf("publish failed: %v; release failed: %w", err, releaseErr)
		}
		return true, err
	}
	return true, d.repository.MarkOutboxSent(ctx, claim.Event.ID, claim.LeaseOwner)
}
