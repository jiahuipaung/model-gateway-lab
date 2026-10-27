package fakeprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

// Scenario is a deterministic provider used by the local demo. It models
// remote acceptance, lost submit responses, reconciliation, and asynchronous
// completion without external credentials.
type Scenario struct {
	state ScenarioState
}

func NewScenario() *Scenario {
	return NewScenarioWithState(NewMemoryScenarioState())
}

func NewScenarioWithState(state ScenarioState) *Scenario {
	return &Scenario{state: state}
}

func (p *Scenario) Submit(ctx context.Context, request provider.SubmitRequest) (provider.SubmitResult, error) {
	remoteID := deterministicRemoteID(request.RequestKey)
	if err := p.state.Save(ctx, request.RequestKey, remoteID, request.Spec.Model == "demo-failure"); err != nil {
		return provider.SubmitResult{}, err
	}
	if request.Spec.Model == "demo-lost-response" {
		return provider.SubmitResult{}, &provider.UnknownOutcomeError{Cause: errors.New("simulated response loss after remote acceptance")}
	}
	return provider.SubmitResult{ProviderTaskID: remoteID}, nil
}

func (p *Scenario) Lookup(ctx context.Context, requestKey string) (provider.LookupResult, error) {
	remoteID, found, err := p.state.Find(ctx, requestKey)
	if err != nil {
		return provider.LookupResult{}, err
	}
	if !found {
		return provider.LookupResult{Status: provider.LookupAbsent}, nil
	}
	return provider.LookupResult{Status: provider.LookupFound, ProviderTaskID: remoteID}, nil
}

func (p *Scenario) Poll(ctx context.Context, providerTaskID string) (provider.PollResult, error) {
	pollCount, shouldFail, found, err := p.state.AdvancePoll(ctx, providerTaskID)
	if err != nil {
		return provider.PollResult{}, err
	}
	if !found {
		return provider.PollResult{}, &provider.DefiniteError{Code: "task_not_found"}
	}
	if pollCount == 1 {
		return provider.PollResult{Status: provider.PollRunning}, nil
	}
	if shouldFail {
		return provider.PollResult{Status: provider.PollFailed, FailureCode: "simulated_generation_failure"}, nil
	}
	return provider.PollResult{Status: provider.PollSucceeded, OutputRef: "artifact://demo/" + providerTaskID}, nil
}

func deterministicRemoteID(requestKey string) string {
	sum := sha256.Sum256([]byte(requestKey))
	return "remote_" + hex.EncodeToString(sum[:8])
}
