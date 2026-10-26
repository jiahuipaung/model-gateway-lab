package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/fakeprovider"
	"github.com/jiahuipaung/model-gateway-lab/internal/memory"
	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

func TestPollerAdvancesProviderExecutionStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		poll        provider.PollResult
		wantState   task.ExecutionState
		wantOutput  string
		wantFailure string
		wantEvent   bool
	}{
		{"queued", provider.PollResult{Status: provider.PollQueued}, task.ExecutionQueued, "", "", false},
		{"running", provider.PollResult{Status: provider.PollRunning}, task.ExecutionRunning, "", "", false},
		{"succeeded", provider.PollResult{Status: provider.PollSucceeded, OutputRef: "artifact://result-1"}, task.ExecutionSucceeded, "artifact://result-1", "", true},
		{"failed", provider.PollResult{Status: provider.PollFailed, FailureCode: "generation_failed"}, task.ExecutionFailed, "", "generation_failed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store, svc, fake, taskID := confirmedTask(t)
			fake.PushPoll(tt.poll, nil)
			poller := controlplane.NewPoller(store, map[string]provider.Provider{"fake-a": fake}, "poll-1", time.Minute)

			worked, err := poller.RunOnce(ctx)
			if err != nil || !worked {
				t.Fatalf("RunOnce = %v, %v", worked, err)
			}
			got, err := svc.GetTask(ctx, "team-a", taskID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.ExecutionState != tt.wantState || got.OutputRef != tt.wantOutput || got.FailureCode != tt.wantFailure {
				t.Fatalf("task = %#v, want state/output/failure %s/%q/%q", got, tt.wantState, tt.wantOutput, tt.wantFailure)
			}

			event, ok, err := store.ClaimOutbox(ctx, "dispatch-1", time.Now().UTC(), time.Minute)
			if err != nil || ok != tt.wantEvent {
				t.Fatalf("outbox claim = %#v, ok=%v err=%v, want event=%v", event, ok, err, tt.wantEvent)
			}
		})
	}
}

func TestStalePollOwnerCannotCompleteAfterTakeover(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, _, _, taskID := confirmedTask(t)
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	if _, ok, err := store.ClaimPoll(ctx, "poll-a", now, time.Minute); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimPoll(ctx, "poll-b", now.Add(time.Minute), time.Minute); err != nil || !ok {
		t.Fatalf("takeover claim: ok=%v err=%v", ok, err)
	}
	err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID:     taskID,
		LeaseOwner: "poll-a",
		Outcome:    controlplane.PollObservedSucceeded,
		OutputRef:  "artifact://stale",
	})
	if !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v, want ErrLeaseLost", err)
	}
}

func TestOutboxRedeliveryKeepsStableEventID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, _, fake, _ := confirmedTask(t)
	fake.PushPoll(provider.PollResult{Status: provider.PollSucceeded, OutputRef: "artifact://result-1"}, nil)
	poller := controlplane.NewPoller(store, map[string]provider.Provider{"fake-a": fake}, "poll-1", time.Minute)
	if worked, err := poller.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("poll = %v, %v", worked, err)
	}

	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	first, ok, err := store.ClaimOutbox(ctx, "dispatch-a", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first event claim = %#v, %v, %v", first, ok, err)
	}
	// Simulate delivery succeeding followed by a crash before MarkOutboxSent.
	second, ok, err := store.ClaimOutbox(ctx, "dispatch-b", now.Add(time.Minute), time.Minute)
	if err != nil || !ok {
		t.Fatalf("redelivery claim = %#v, %v, %v", second, ok, err)
	}
	if second.Event.ID != first.Event.ID {
		t.Fatalf("redelivery event ID = %q, want stable %q", second.Event.ID, first.Event.ID)
	}
	if err := store.MarkOutboxSent(ctx, second.Event.ID, "dispatch-a"); !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale dispatcher mark error = %v, want ErrLeaseLost", err)
	}
	if err := store.MarkOutboxSent(ctx, second.Event.ID, "dispatch-b"); err != nil {
		t.Fatalf("current dispatcher mark: %v", err)
	}
}

func TestDispatcherReleasesFailedDeliveryForRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, _, fake, _ := confirmedTask(t)
	fake.PushPoll(provider.PollResult{Status: provider.PollFailed, FailureCode: "generation_failed"}, nil)
	poller := controlplane.NewPoller(store, map[string]provider.Provider{"fake-a": fake}, "poll-1", time.Minute)
	if worked, err := poller.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("poll = %v, %v", worked, err)
	}

	sink := &recordingSink{failuresRemaining: 1}
	dispatcher := controlplane.NewDispatcher(store, sink, "dispatch-1", time.Minute)
	if worked, err := dispatcher.RunOnce(ctx); err == nil || !worked {
		t.Fatalf("first dispatch = %v, %v, want worked with sink error", worked, err)
	}
	if worked, err := dispatcher.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("retry dispatch = %v, %v", worked, err)
	}
	if len(sink.eventIDs) != 2 || sink.eventIDs[0] != sink.eventIDs[1] {
		t.Fatalf("delivery IDs = %v, want two identical IDs", sink.eventIDs)
	}
}

type recordingSink struct {
	failuresRemaining int
	eventIDs          []string
}

func (s *recordingSink) Publish(_ context.Context, event controlplane.Event) error {
	s.eventIDs = append(s.eventIDs, event.ID)
	if s.failuresRemaining > 0 {
		s.failuresRemaining--
		return errors.New("sink unavailable")
	}
	return nil
}

func confirmedTask(t *testing.T) (*memory.Store, *controlplane.Service, *fakeprovider.Provider, string) {
	t.Helper()
	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store)
	created, err := svc.CreateTask(ctx, validCreateInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	fake := fakeprovider.New()
	fake.PushSubmit(provider.SubmitResult{ProviderTaskID: "remote-1"}, nil)
	submitter := controlplane.NewSubmissionWorker(store, map[string]provider.Provider{"fake-a": fake}, "submit-1", time.Minute)
	if worked, err := submitter.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("submit = %v, %v", worked, err)
	}
	return store, svc, fake, created.Task.ID
}
