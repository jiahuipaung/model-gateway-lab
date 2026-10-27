package controlplane_test

import (
	"context"
	"errors"
	"fmt"
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
	first, ok, err := store.ClaimPoll(ctx, "poll-worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	second, ok, err := store.ClaimPoll(ctx, "poll-worker", now.Add(time.Minute), time.Minute)
	if err != nil || !ok {
		t.Fatalf("takeover claim: ok=%v err=%v", ok, err)
	}
	if first.LeaseOwner == second.LeaseOwner {
		t.Fatalf("poll claim token was reused: %q", first.LeaseOwner)
	}
	err = store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID:     taskID,
		LeaseOwner: first.LeaseOwner,
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
	if err := store.MarkOutboxSent(ctx, second.Event.ID, first.LeaseOwner); !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale dispatcher mark error = %v, want ErrLeaseLost", err)
	}
	if err := store.MarkOutboxSent(ctx, second.Event.ID, second.LeaseOwner); err != nil {
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
	time.Sleep(600 * time.Millisecond)
	if worked, err := dispatcher.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("retry dispatch = %v, %v", worked, err)
	}
	if len(sink.eventIDs) != 2 || sink.eventIDs[0] != sink.eventIDs[1] {
		t.Fatalf("delivery IDs = %v, want two identical IDs", sink.eventIDs)
	}
}

func TestPollRetryDoesNotStarveAnotherTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "fake-a")
	fake := fakeprovider.New()
	for i := 0; i < 2; i++ {
		input := validCreateInput()
		input.IdempotencyKey = fmt.Sprintf("request-fair-%d", i)
		if _, err := svc.CreateTask(ctx, input); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		fake.PushSubmit(provider.SubmitResult{ProviderTaskID: fmt.Sprintf("remote-fair-%d", i)}, nil)
	}
	submitter := controlplane.NewSubmissionWorker(store, map[string]provider.Provider{"fake-a": fake}, "submit", time.Minute)
	for i := 0; i < 2; i++ {
		if worked, err := submitter.RunOnce(ctx); err != nil || !worked {
			t.Fatalf("submit %d: worked=%v err=%v", i, worked, err)
		}
	}

	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	first, ok, err := store.ClaimPoll(ctx, "poll", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first poll claim: %#v ok=%v err=%v", first, ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: first.Task.ID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.PollObservationRetry,
		NextPollAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("release first poll: %v", err)
	}
	second, ok, err := store.ClaimPoll(ctx, "poll", now, time.Minute)
	if err != nil || !ok || second.Task.ID == first.Task.ID {
		t.Fatalf("second task was starved: first=%s second=%#v ok=%v err=%v", first.Task.ID, second, ok, err)
	}
}

func TestRegressiveProviderObservationDoesNotMoveRunningTaskBackToQueued(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, svc, _, taskID := confirmedTask(t)
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	first, ok, err := store.ClaimPoll(ctx, "poll", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: taskID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.PollObservedRunning, NextPollAt: now,
	}); err != nil {
		t.Fatalf("record running: %v", err)
	}
	second, ok, err := store.ClaimPoll(ctx, "poll", now.Add(time.Second), time.Minute)
	if err != nil || !ok {
		t.Fatalf("second claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: taskID, LeaseOwner: second.LeaseOwner, Outcome: controlplane.PollObservedQueued, NextPollAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("ignore stale observation: %v", err)
	}
	got, err := svc.GetTask(ctx, "team-a", taskID)
	if err != nil || got.ExecutionState != task.ExecutionRunning {
		t.Fatalf("task after stale observation = %#v err=%v", got, err)
	}
}

func TestFailedOutboxDeliveryDoesNotStarveAnotherEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "fake-a")
	fake := fakeprovider.New()
	for i := 0; i < 2; i++ {
		input := validCreateInput()
		input.IdempotencyKey = fmt.Sprintf("request-event-%d", i)
		if _, err := svc.CreateTask(ctx, input); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		fake.PushSubmit(provider.SubmitResult{ProviderTaskID: fmt.Sprintf("remote-event-%d", i)}, nil)
	}
	submitter := controlplane.NewSubmissionWorker(store, map[string]provider.Provider{"fake-a": fake}, "submit", time.Minute)
	for i := 0; i < 2; i++ {
		if worked, err := submitter.RunOnce(ctx); err != nil || !worked {
			t.Fatalf("submit %d: worked=%v err=%v", i, worked, err)
		}
		fake.PushPoll(provider.PollResult{Status: provider.PollSucceeded, OutputRef: fmt.Sprintf("artifact://%d", i)}, nil)
	}
	poller := controlplane.NewPoller(store, map[string]provider.Provider{"fake-a": fake}, "poll", time.Minute)
	for i := 0; i < 2; i++ {
		if worked, err := poller.RunOnce(ctx); err != nil || !worked {
			t.Fatalf("poll %d: worked=%v err=%v", i, worked, err)
		}
	}

	now := time.Now().UTC()
	first, ok, err := store.ClaimOutbox(ctx, "dispatch", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first event: %#v ok=%v err=%v", first, ok, err)
	}
	if err := store.ReleaseOutbox(ctx, first.Event.ID, first.LeaseOwner, now.Add(time.Minute)); err != nil {
		t.Fatalf("release first event: %v", err)
	}
	second, ok, err := store.ClaimOutbox(ctx, "dispatch", now, time.Minute)
	if err != nil || !ok || second.Event.ID == first.Event.ID {
		t.Fatalf("second event was starved: first=%s second=%#v ok=%v err=%v", first.Event.ID, second, ok, err)
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
	svc := controlplane.NewService(store, "fake-a")
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
