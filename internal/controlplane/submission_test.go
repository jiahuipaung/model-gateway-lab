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

func TestExpiredSubmissionMustReconcileBeforeResubmission(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "fake-a")
	created, err := svc.CreateTask(ctx, validCreateInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

	first, ok, err := store.ClaimSubmission(ctx, "worker-a", now, time.Minute)
	if err != nil || !ok || first.Task.ID != created.Task.ID {
		t.Fatalf("first claim = %#v, %v, %v", first, ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(30*time.Second), time.Minute); err != nil || ok {
		t.Fatalf("claim before lease expiry: ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(time.Minute), time.Minute); err != nil || ok {
		t.Fatalf("expired submission must not be resubmitted: ok=%v err=%v", ok, err)
	}

	recovery, ok, err := store.ClaimUnknown(ctx, "reconcile-a", now.Add(time.Minute), time.Minute)
	if err != nil || !ok || recovery.Task.ID != created.Task.ID || recovery.Task.SubmissionState != task.SubmissionUnknown {
		t.Fatalf("recovery claim = %#v, %v, %v", recovery, ok, err)
	}

	err = store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: created.Task.ID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "remote-old",
	})
	if !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v, want ErrLeaseLost", err)
	}
	if err := store.ResolveUnknown(ctx, controlplane.UnknownResolution{
		TaskID: created.Task.ID, LeaseOwner: recovery.LeaseOwner, Outcome: controlplane.ReconciliationAbsent,
	}); err != nil {
		t.Fatalf("prove absent: %v", err)
	}
	if _, ok, err := store.ClaimSubmission(ctx, "worker-b", now.Add(2*time.Minute), time.Minute); err != nil || !ok {
		t.Fatalf("resubmit after proven absence: ok=%v err=%v", ok, err)
	}
}

func TestClaimTokensFenceSameLogicalWorkerAfterTakeover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "fake-a")
	created, err := svc.CreateTask(ctx, validCreateInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	first, ok, err := store.ClaimSubmission(ctx, "submit-worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim: %#v ok=%v err=%v", first, ok, err)
	}
	recovery, ok, err := store.ClaimUnknown(ctx, "reconcile-worker", now.Add(time.Minute), time.Minute)
	if err != nil || !ok {
		t.Fatalf("recovery claim: %#v ok=%v err=%v", recovery, ok, err)
	}
	if first.LeaseOwner == recovery.LeaseOwner {
		t.Fatalf("claim token was reused: %q", first.LeaseOwner)
	}
	if err := store.CompleteSubmission(ctx, controlplane.SubmissionCompletion{
		TaskID: created.Task.ID, LeaseOwner: first.LeaseOwner, Outcome: controlplane.SubmissionAccepted, ProviderTaskID: "stale",
	}); !errors.Is(err, controlplane.ErrLeaseLost) {
		t.Fatalf("stale completion error = %v, want ErrLeaseLost", err)
	}
}

func TestSubmissionWorkerMapsProviderOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		result    provider.SubmitResult
		err       error
		wantState task.SubmissionState
		wantExec  task.ExecutionState
	}{
		{"accepted", provider.SubmitResult{ProviderTaskID: "remote-1"}, nil, task.SubmissionConfirmed, task.ExecutionQueued},
		{"definite rejection", provider.SubmitResult{}, &provider.DefiniteError{Code: "invalid_input"}, task.SubmissionRejected, task.ExecutionNone},
		{"unknown outcome", provider.SubmitResult{}, &provider.UnknownOutcomeError{Cause: errors.New("response lost")}, task.SubmissionUnknown, task.ExecutionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := memory.NewStore()
			svc := controlplane.NewService(store, "fake-a")
			created, err := svc.CreateTask(ctx, validCreateInput())
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			fake := fakeprovider.New()
			fake.PushSubmit(tt.result, tt.err)
			worker := controlplane.NewSubmissionWorker(store, map[string]provider.Provider{"fake-a": fake}, "submit-1", time.Minute)

			worked, err := worker.RunOnce(ctx)
			if err != nil || !worked {
				t.Fatalf("RunOnce = %v, %v", worked, err)
			}
			got, err := svc.GetTask(ctx, "team-a", created.Task.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.SubmissionState != tt.wantState || got.ExecutionState != tt.wantExec {
				t.Fatalf("states = %s/%s, want %s/%s", got.SubmissionState, got.ExecutionState, tt.wantState, tt.wantExec)
			}
		})
	}
}

func TestSubmissionWorkerRejectsPersistedTaskWhenProviderConfigurationDrifts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "retired-provider")
	input := validCreateInput()
	input.Spec.Provider = "retired-provider"
	created, err := svc.CreateTask(ctx, input)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	worker := controlplane.NewSubmissionWorker(store, nil, "submit", time.Minute)
	if worked, err := worker.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("RunOnce = %v, %v", worked, err)
	}
	got, err := svc.GetTask(ctx, input.TenantID, created.Task.ID)
	if err != nil || got.SubmissionState != task.SubmissionRejected || got.FailureCode != "provider_not_configured" {
		t.Fatalf("task = %#v, err=%v", got, err)
	}
}

func TestReconcilerDistinguishesFoundAbsentAndInconclusive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lookup    provider.LookupResult
		lookupErr error
		wantState task.SubmissionState
		wantExec  task.ExecutionState
	}{
		{"found", provider.LookupResult{Status: provider.LookupFound, ProviderTaskID: "remote-1"}, nil, task.SubmissionConfirmed, task.ExecutionQueued},
		{"proven absent", provider.LookupResult{Status: provider.LookupAbsent}, nil, task.SubmissionPending, task.ExecutionNone},
		{"inconclusive", provider.LookupResult{Status: provider.LookupInconclusive}, nil, task.SubmissionUnknown, task.ExecutionNone},
		{"temporary lookup error", provider.LookupResult{}, errors.New("lookup unavailable"), task.SubmissionUnknown, task.ExecutionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := memory.NewStore()
			svc := controlplane.NewService(store, "fake-a")
			created, err := svc.CreateTask(ctx, validCreateInput())
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			fake := fakeprovider.New()
			fake.PushSubmit(provider.SubmitResult{}, &provider.UnknownOutcomeError{Cause: errors.New("response lost")})
			fake.PushLookup(tt.lookup, tt.lookupErr)
			submitter := controlplane.NewSubmissionWorker(store, map[string]provider.Provider{"fake-a": fake}, "submit-1", time.Minute)
			if worked, err := submitter.RunOnce(ctx); err != nil || !worked {
				t.Fatalf("submit RunOnce = %v, %v", worked, err)
			}

			reconciler := controlplane.NewReconciler(store, map[string]provider.Provider{"fake-a": fake}, "reconcile-1", time.Minute)
			if worked, err := reconciler.RunOnce(ctx); err != nil || !worked {
				t.Fatalf("reconcile RunOnce = %v, %v", worked, err)
			}
			got, err := svc.GetTask(ctx, "team-a", created.Task.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.SubmissionState != tt.wantState || got.ExecutionState != tt.wantExec {
				t.Fatalf("states = %s/%s, want %s/%s", got.SubmissionState, got.ExecutionState, tt.wantState, tt.wantExec)
			}
		})
	}
}
