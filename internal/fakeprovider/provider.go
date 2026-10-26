package fakeprovider

import (
	"context"
	"errors"
	"sync"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

var ErrNoBehavior = errors.New("fake provider has no programmed behavior")

type submitBehavior struct {
	result provider.SubmitResult
	err    error
}

type lookupBehavior struct {
	result provider.LookupResult
	err    error
}

type pollBehavior struct {
	result provider.PollResult
	err    error
}

type Provider struct {
	mu      sync.Mutex
	submits []submitBehavior
	lookups []lookupBehavior
	polls   []pollBehavior
}

func (p *Provider) PushPoll(result provider.PollResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.polls = append(p.polls, pollBehavior{result: result, err: err})
}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) PushSubmit(result provider.SubmitResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submits = append(p.submits, submitBehavior{result: result, err: err})
}

func (p *Provider) PushLookup(result provider.LookupResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lookups = append(p.lookups, lookupBehavior{result: result, err: err})
}

func (p *Provider) Submit(context.Context, provider.SubmitRequest) (provider.SubmitResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.submits) == 0 {
		return provider.SubmitResult{}, ErrNoBehavior
	}
	next := p.submits[0]
	p.submits = p.submits[1:]
	return next.result, next.err
}

func (p *Provider) Lookup(context.Context, string) (provider.LookupResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lookups) == 0 {
		return provider.LookupResult{}, ErrNoBehavior
	}
	next := p.lookups[0]
	p.lookups = p.lookups[1:]
	return next.result, next.err
}

func (p *Provider) Poll(context.Context, string) (provider.PollResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.polls) == 0 {
		return provider.PollResult{}, ErrNoBehavior
	}
	next := p.polls[0]
	p.polls = p.polls[1:]
	return next.result, next.err
}
