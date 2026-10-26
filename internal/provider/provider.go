package provider

import (
	"context"
	"fmt"

	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

type SubmitRequest struct {
	TaskID     string
	RequestKey string
	Spec       task.Spec
}

type SubmitResult struct {
	ProviderTaskID string
}

type LookupStatus string

const (
	LookupFound        LookupStatus = "FOUND"
	LookupAbsent       LookupStatus = "ABSENT"
	LookupInconclusive LookupStatus = "INCONCLUSIVE"
)

type LookupResult struct {
	Status         LookupStatus
	ProviderTaskID string
}

type PollStatus string

const (
	PollQueued    PollStatus = "QUEUED"
	PollRunning   PollStatus = "RUNNING"
	PollSucceeded PollStatus = "SUCCEEDED"
	PollFailed    PollStatus = "FAILED"
)

type PollResult struct {
	Status      PollStatus
	OutputRef   string
	FailureCode string
}

type Provider interface {
	Submit(context.Context, SubmitRequest) (SubmitResult, error)
	Lookup(context.Context, string) (LookupResult, error)
	Poll(context.Context, string) (PollResult, error)
}

type DefiniteError struct {
	Code string
}

func (e *DefiniteError) Error() string {
	return fmt.Sprintf("provider definitely rejected request: %s", e.Code)
}

type UnknownOutcomeError struct {
	Cause error
}

func (e *UnknownOutcomeError) Error() string {
	return "provider submission outcome is unknown: " + e.Cause.Error()
}

func (e *UnknownOutcomeError) Unwrap() error {
	return e.Cause
}
