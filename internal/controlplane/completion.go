package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

type Poller struct {
	repository    Repository
	providers     map[string]provider.Provider
	workerID      string
	leaseDuration time.Duration
	now           func() time.Time
}

func NewPoller(repository Repository, providers map[string]provider.Provider, workerID string, leaseDuration time.Duration) *Poller {
	return &Poller{repository: repository, providers: providers, workerID: workerID, leaseDuration: leaseDuration, now: time.Now}
}

func (p *Poller) RunOnce(ctx context.Context) (bool, error) {
	claim, ok, err := p.repository.ClaimPoll(ctx, p.workerID, p.now().UTC(), p.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}
	client, configured := p.providers[claim.Task.Spec.Provider]
	if !configured {
		return true, fmt.Errorf("provider %q is not configured", claim.Task.Spec.Provider)
	}

	result, pollErr := client.Poll(ctx, claim.Task.ProviderTaskID)
	completion := PollCompletion{TaskID: claim.Task.ID, LeaseOwner: claim.LeaseOwner}
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
	claim, ok, err := d.repository.ClaimOutbox(ctx, d.workerID, d.now().UTC(), d.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}
	if err := d.sink.Publish(ctx, claim.Event); err != nil {
		if releaseErr := d.repository.ReleaseOutbox(ctx, claim.Event.ID, claim.LeaseOwner); releaseErr != nil {
			return true, fmt.Errorf("publish failed: %v; release failed: %w", err, releaseErr)
		}
		return true, err
	}
	return true, d.repository.MarkOutboxSent(ctx, claim.Event.ID, claim.LeaseOwner)
}
