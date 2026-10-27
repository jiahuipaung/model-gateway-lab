package fakeprovider_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jiahuipaung/model-gateway-lab/internal/fakeprovider"
	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

func TestScenarioProviderModelsNormalAndLostResponseSubmissions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := fakeprovider.NewScenario()

	normal, err := fake.Submit(ctx, provider.SubmitRequest{RequestKey: "normal-1", Spec: task.Spec{Model: "demo-success"}})
	if err != nil || normal.ProviderTaskID == "" {
		t.Fatalf("normal submit = %#v, %v", normal, err)
	}
	_, err = fake.Submit(ctx, provider.SubmitRequest{RequestKey: "lost-1", Spec: task.Spec{Model: "demo-lost-response"}})
	var unknown *provider.UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("lost response error = %v, want UnknownOutcomeError", err)
	}
	found, err := fake.Lookup(ctx, "lost-1")
	if err != nil || found.Status != provider.LookupFound || found.ProviderTaskID == "" {
		t.Fatalf("lookup = %#v, %v", found, err)
	}
}

func TestScenarioProviderPollsRunningThenSucceeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := fakeprovider.NewScenario()
	submitted, err := fake.Submit(ctx, provider.SubmitRequest{RequestKey: "normal-1", Spec: task.Spec{Model: "demo-success"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	first, err := fake.Poll(ctx, submitted.ProviderTaskID)
	if err != nil || first.Status != provider.PollRunning {
		t.Fatalf("first poll = %#v, %v", first, err)
	}
	second, err := fake.Poll(ctx, submitted.ProviderTaskID)
	if err != nil || second.Status != provider.PollSucceeded || second.OutputRef == "" {
		t.Fatalf("second poll = %#v, %v", second, err)
	}
}

func TestScenarioProviderCanRecoverKnownDeterministicTasksAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	requestKey := "mgw_0123456789abcdef0123456789abcdef"
	state := fakeprovider.NewMemoryScenarioState()
	first := fakeprovider.NewScenarioWithState(state)
	submitted, err := first.Submit(ctx, provider.SubmitRequest{RequestKey: requestKey, Spec: task.Spec{Model: "demo-success"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	restarted := fakeprovider.NewScenarioWithState(state)
	found, err := restarted.Lookup(ctx, requestKey)
	if err != nil || found.Status != provider.LookupFound || found.ProviderTaskID != submitted.ProviderTaskID {
		t.Fatalf("lookup after restart = %#v, %v", found, err)
	}
	polled, err := restarted.Poll(ctx, submitted.ProviderTaskID)
	if err != nil || polled.Status != provider.PollRunning {
		t.Fatalf("poll after restart = %#v, %v", polled, err)
	}
}
