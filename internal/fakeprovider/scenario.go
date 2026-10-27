package fakeprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

// Scenario is a deterministic provider used by the local demo. It models
// remote acceptance, lost submit responses, reconciliation, and asynchronous
// completion without external credentials.
type Scenario struct {
	mu            sync.Mutex
	requestToTask map[string]string
	pollCounts    map[string]int
	failingTask   map[string]bool
}

func NewScenario() *Scenario {
	return &Scenario{
		requestToTask: make(map[string]string),
		pollCounts:    make(map[string]int),
		failingTask:   make(map[string]bool),
	}
}

func (p *Scenario) Submit(_ context.Context, request provider.SubmitRequest) (provider.SubmitResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	remoteID := deterministicRemoteID(request.RequestKey)
	p.requestToTask[request.RequestKey] = remoteID
	if request.Spec.Model == "demo-failure" {
		p.failingTask[remoteID] = true
	}
	if request.Spec.Model == "demo-lost-response" {
		return provider.SubmitResult{}, &provider.UnknownOutcomeError{Cause: errors.New("simulated response loss after remote acceptance")}
	}
	return provider.SubmitResult{ProviderTaskID: remoteID}, nil
}

func (p *Scenario) Lookup(_ context.Context, requestKey string) (provider.LookupResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	remoteID, ok := p.requestToTask[requestKey]
	if !ok {
		return provider.LookupResult{Status: provider.LookupAbsent}, nil
	}
	return provider.LookupResult{Status: provider.LookupFound, ProviderTaskID: remoteID}, nil
}

func (p *Scenario) Poll(_ context.Context, providerTaskID string) (provider.PollResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, known := p.pollCounts[providerTaskID]; !known {
		known = false
		for _, remoteID := range p.requestToTask {
			if remoteID == providerTaskID {
				known = true
				break
			}
		}
		if !known {
			return provider.PollResult{}, &provider.DefiniteError{Code: "task_not_found"}
		}
	}
	p.pollCounts[providerTaskID]++
	if p.pollCounts[providerTaskID] == 1 {
		return provider.PollResult{Status: provider.PollRunning}, nil
	}
	if p.failingTask[providerTaskID] {
		return provider.PollResult{Status: provider.PollFailed, FailureCode: "simulated_generation_failure"}, nil
	}
	return provider.PollResult{Status: provider.PollSucceeded, OutputRef: "artifact://demo/" + providerTaskID}, nil
}

func deterministicRemoteID(requestKey string) string {
	sum := sha256.Sum256([]byte(requestKey))
	return "remote_" + hex.EncodeToString(sum[:8])
}
