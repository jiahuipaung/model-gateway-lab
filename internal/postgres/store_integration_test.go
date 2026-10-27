//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/fakeprovider"
	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

func TestCreateTaskIsAtomicAndIdempotent(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	record := testTask("task-00000000-0000-4000-8000-000000000001", "request-1", "digest-1", now)

	created, replayed, err := store.CreateTask(ctx, record)
	if err != nil || replayed || created.ID != record.ID {
		t.Fatalf("first create = %#v, replayed=%v err=%v", created, replayed, err)
	}
	replayedTask, replayed, err := store.CreateTask(ctx, testTask("task-00000000-0000-4000-8000-000000000002", "request-1", "digest-1", now))
	if err != nil || !replayed || replayedTask.ID != record.ID {
		t.Fatalf("replay = %#v, replayed=%v err=%v", replayedTask, replayed, err)
	}
	_, _, err = store.CreateTask(ctx, testTask("task-00000000-0000-4000-8000-000000000003", "request-1", "digest-2", now))
	if !errors.Is(err, controlplane.ErrIdempotencyConflict) {
		t.Fatalf("conflict error = %v", err)
	}

	claim, ok, err := store.ClaimSubmission(ctx, "worker-a", now, time.Minute)
	if err != nil || !ok || claim.Task.ID != record.ID {
		t.Fatalf("durable submit intent missing: claim=%#v ok=%v err=%v", claim, ok, err)
	}
}

func TestExpiredSubmissionIsReconciledAndFencedBeforeRetry(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	record := testTask("task-00000000-0000-4000-8000-000000000011", "request-11", "digest-11", now)
	if _, _, err := store.CreateTask(ctx, record); err != nil {
		t.Fatalf("create: %v", err)
	}
	first, ok, err := store.ClaimSubmission(ctx, "worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(30*time.Second), time.Minute); err != nil || ok {
		t.Fatalf("early takeover: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker", now.Add(time.Minute), time.Minute); err != nil || ok {
		t.Fatalf("expired submission was directly retried: ok=%v err=%v", ok, err)
	}
	recovery, ok, err := store.ClaimUnknown(ctx, "worker", now.Add(time.Minute), time.Minute)
	if err != nil || !ok || recovery.Task.SubmissionState != task.SubmissionUnknown {
		t.Fatalf("recovery claim = %#v ok=%v err=%v", recovery, ok, err)
	}
	if first.LeaseOwner == recovery.LeaseOwner {
		t.Fatalf("claim token was reused: %q", first.LeaseOwner)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: record.ID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-stale",
	}); !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, controlplane.UnknownResolution{
		TaskID: record.ID, LeaseOwner: recovery.LeaseOwner, Outcome: controlplane.ReconciliationAbsent,
	}); err != nil {
		t.Fatalf("resolve absent: %v", err)
	}
	second, ok, err := store.ClaimSubmission(ctx, "worker", now.Add(2*time.Minute), time.Minute)
	if err != nil || !ok || second.LeaseOwner == first.LeaseOwner {
		t.Fatalf("safe retry claim = %#v ok=%v err=%v", second, ok, err)
	}
}

func TestTerminalUpdateAndOutboxRedelivery(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	record := testTask("task-00000000-0000-4000-8000-000000000021", "request-21", "digest-21", now)
	if _, _, err := store.CreateTask(ctx, record); err != nil {
		t.Fatalf("create: %v", err)
	}
	submit, ok, err := store.ClaimSubmission(ctx, "submit", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("submit claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: record.ID, LeaseOwner: submit.LeaseOwner, Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-21",
	}); err != nil {
		t.Fatalf("submit completion: %v", err)
	}
	poll, ok, err := store.ClaimPoll(ctx, "poll", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("poll claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: record.ID, LeaseOwner: poll.LeaseOwner, Outcome: controlplane.PollObservedSucceeded,
		OutputRef: "artifact://result-21", NextPollAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("poll completion: %v", err)
	}

	got, err := store.GetTask(ctx, record.TenantID, record.ID)
	if err != nil || got.ExecutionState != task.ExecutionSucceeded {
		t.Fatalf("terminal task = %#v, err=%v", got, err)
	}
	first, ok, err := store.ClaimOutbox(ctx, "dispatch-a", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first event = %#v ok=%v err=%v", first, ok, err)
	}
	second, ok, err := store.ClaimOutbox(ctx, "dispatch-b", now.Add(time.Minute), time.Minute)
	if err != nil || !ok || second.Event.ID != first.Event.ID {
		t.Fatalf("redelivery = %#v ok=%v err=%v; first=%#v", second, ok, err, first)
	}
	if err := store.MarkOutboxSent(ctx, second.Event.ID, second.LeaseOwner); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
}

func TestPollRetryIsFairAndRegressiveObservationIsIgnored(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	for i, id := range []string{
		"task-00000000-0000-4000-8000-000000000031",
		"task-00000000-0000-4000-8000-000000000032",
	} {
		record := testTask(id, fmt.Sprintf("request-3%d", i), fmt.Sprintf("digest-3%d", i), now.Add(time.Duration(i)*time.Second))
		if _, _, err := store.CreateTask(ctx, record); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		claim, ok, err := store.ClaimSubmission(ctx, "submit", now.Add(time.Duration(i)*time.Second), time.Minute)
		if err != nil || !ok {
			t.Fatalf("submit claim %d: ok=%v err=%v", i, ok, err)
		}
		if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
			TaskID: claim.Task.ID, LeaseOwner: claim.LeaseOwner, Outcome: controlplane.SubmissionAccepted,
			ProviderTaskID: fmt.Sprintf("remote-3%d", i),
		}); err != nil {
			t.Fatalf("submit completion %d: %v", i, err)
		}
	}

	first, ok, err := store.ClaimPoll(ctx, "poll", now.Add(2*time.Second), time.Minute)
	if err != nil || !ok {
		t.Fatalf("first poll: %#v ok=%v err=%v", first, ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: first.Task.ID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.PollObservationRetry,
		NextPollAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("retry first: %v", err)
	}
	second, ok, err := store.ClaimPoll(ctx, "poll", now.Add(2*time.Second), time.Minute)
	if err != nil || !ok || second.Task.ID == first.Task.ID {
		t.Fatalf("second task was starved: first=%s second=%#v ok=%v err=%v", first.Task.ID, second, ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: second.Task.ID, LeaseOwner: second.LeaseOwner, Outcome: controlplane.PollObservedRunning,
		NextPollAt: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("record running: %v", err)
	}
	again, ok, err := store.ClaimPoll(ctx, "poll", now.Add(3*time.Second), time.Minute)
	if err != nil || !ok || again.Task.ID != second.Task.ID {
		t.Fatalf("reclaim running task: %#v ok=%v err=%v", again, ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: again.Task.ID, LeaseOwner: again.LeaseOwner, Outcome: controlplane.PollObservedQueued,
		NextPollAt: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("ignore regression: %v", err)
	}
	got, err := store.GetTask(ctx, second.Task.TenantID, second.Task.ID)
	if err != nil || got.ExecutionState != task.ExecutionRunning {
		t.Fatalf("task after regression = %#v err=%v", got, err)
	}
}

func TestScenarioProviderStateSurvivesGatewayReconstruction(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	requestKey := "mgw_0123456789abcdef0123456789abcdef"
	first := fakeprovider.NewScenarioWithState(store)
	submitted, err := first.Submit(ctx, provider.SubmitRequest{
		RequestKey: requestKey,
		Spec:       task.Spec{Model: "demo-success"},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	restarted := fakeprovider.NewScenarioWithState(store)
	found, err := restarted.Lookup(ctx, requestKey)
	if err != nil || found.Status != provider.LookupFound || found.ProviderTaskID != submitted.ProviderTaskID {
		t.Fatalf("lookup after reconstruction = %#v, %v", found, err)
	}
	if missing, err := restarted.Lookup(ctx, "mgw_absent"); err != nil || missing.Status != provider.LookupAbsent {
		t.Fatalf("missing lookup = %#v, %v", missing, err)
	}
	polled, err := restarted.Poll(ctx, submitted.ProviderTaskID)
	if err != nil || polled.Status != provider.PollRunning {
		t.Fatalf("poll after reconstruction = %#v, %v", polled, err)
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	store, err := Open(context.Background(), url)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := store.pool.Exec(context.Background(), `TRUNCATE fake_provider_tasks, outbox_events, tasks, request_ledger`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	return store
}

func testTask(id, idempotencyKey, digest string, now time.Time) task.Task {
	return task.Task{
		ID: id, TenantID: "team-a", IdempotencyKey: idempotencyKey, RequestDigest: digest,
		ProviderRequestKey: "provider-" + idempotencyKey,
		Spec:               task.Spec{Provider: "fake-a", Model: "video-basic", Operation: "text-to-video", Prompt: "test prompt"},
		SubmissionState:    task.SubmissionPending, ExecutionState: task.ExecutionNone,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
}
