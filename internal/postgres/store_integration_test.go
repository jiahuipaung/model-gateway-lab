//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
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

func TestSubmissionLeaseTakeoverRejectsStaleOwner(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	record := testTask("task-00000000-0000-4000-8000-000000000011", "request-11", "digest-11", now)
	if _, _, err := store.CreateTask(ctx, record); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-a", now, time.Minute); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(30*time.Second), time.Minute); err != nil || ok {
		t.Fatalf("early takeover: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(time.Minute), time.Minute); err != nil || !ok {
		t.Fatalf("expired takeover: ok=%v err=%v", ok, err)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: record.ID, LeaseOwner: "worker-a", Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-stale",
	}); !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v", err)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: record.ID, LeaseOwner: "worker-b", Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-current",
	}); err != nil {
		t.Fatalf("current completion: %v", err)
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
	if _, ok, err := store.ClaimSubmission(ctx, "submit", now, time.Minute); err != nil || !ok {
		t.Fatalf("submit claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: record.ID, LeaseOwner: "submit", Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-21",
	}); err != nil {
		t.Fatalf("submit completion: %v", err)
	}
	if _, ok, err := store.ClaimPoll(ctx, "poll", now, time.Minute); err != nil || !ok {
		t.Fatalf("poll claim: ok=%v err=%v", ok, err)
	}
	if err := store.CompletePoll(ctx, controlplane.PollCompletion{
		TaskID: record.ID, LeaseOwner: "poll", Outcome: controlplane.PollObservedSucceeded, OutputRef: "artifact://result-21",
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
	if err := store.MarkOutboxSent(ctx, second.Event.ID, "dispatch-b"); err != nil {
		t.Fatalf("mark sent: %v", err)
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
	if _, err := store.pool.Exec(context.Background(), `TRUNCATE outbox_events, tasks, request_ledger`); err != nil {
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
