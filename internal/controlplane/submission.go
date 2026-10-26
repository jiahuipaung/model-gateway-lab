package controlplane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/provider"
)

type SubmissionWorker struct {
	repository    Repository
	providers     map[string]provider.Provider
	workerID      string
	leaseDuration time.Duration
	now           func() time.Time
}

func NewSubmissionWorker(repository Repository, providers map[string]provider.Provider, workerID string, leaseDuration time.Duration) *SubmissionWorker {
	return &SubmissionWorker{repository: repository, providers: providers, workerID: workerID, leaseDuration: leaseDuration, now: time.Now}
}

func (w *SubmissionWorker) RunOnce(ctx context.Context) (bool, error) {
	claim, ok, err := w.repository.ClaimSubmission(ctx, w.workerID, w.now().UTC(), w.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}

	client, ok := w.providers[claim.Task.Spec.Provider]
	if !ok {
		return true, fmt.Errorf("provider %q is not configured", claim.Task.Spec.Provider)
	}

	result, submitErr := client.Submit(ctx, provider.SubmitRequest{
		TaskID:     claim.Task.ID,
		RequestKey: claim.Task.ProviderRequestKey,
		Spec:       claim.Task.Spec,
	})
	completion := SubmissionCompletion{TaskID: claim.Task.ID, LeaseOwner: claim.LeaseOwner}
	if submitErr == nil && result.ProviderTaskID != "" {
		completion.Outcome = SubmissionAccepted
		completion.ProviderTaskID = result.ProviderTaskID
	} else {
		var definite *provider.DefiniteError
		if errors.As(submitErr, &definite) {
			completion.Outcome = SubmissionDenied
			completion.FailureCode = definite.Code
		} else {
			completion.Outcome = SubmissionAmbiguous
		}
	}
	return true, w.repository.CompleteSubmission(ctx, completion)
}

type Reconciler struct {
	repository    Repository
	providers     map[string]provider.Provider
	workerID      string
	leaseDuration time.Duration
	now           func() time.Time
}

func NewReconciler(repository Repository, providers map[string]provider.Provider, workerID string, leaseDuration time.Duration) *Reconciler {
	return &Reconciler{repository: repository, providers: providers, workerID: workerID, leaseDuration: leaseDuration, now: time.Now}
}

func (r *Reconciler) RunOnce(ctx context.Context) (bool, error) {
	claim, ok, err := r.repository.ClaimUnknown(ctx, r.workerID, r.now().UTC(), r.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}

	resolution := UnknownResolution{
		TaskID:     claim.Task.ID,
		LeaseOwner: claim.LeaseOwner,
		Outcome:    ReconciliationInconclusive,
	}
	client, configured := r.providers[claim.Task.Spec.Provider]
	if configured {
		result, lookupErr := client.Lookup(ctx, claim.Task.ProviderRequestKey)
		if lookupErr == nil {
			switch result.Status {
			case provider.LookupFound:
				resolution.Outcome = ReconciliationFound
				resolution.ProviderTaskID = result.ProviderTaskID
			case provider.LookupAbsent:
				resolution.Outcome = ReconciliationAbsent
			}
		}
	}
	return true, r.repository.ResolveUnknown(ctx, resolution)
}
