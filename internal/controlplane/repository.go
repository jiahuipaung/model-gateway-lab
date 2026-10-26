package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

var (
	ErrInvalidRequest      = errors.New("invalid request")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrNotFound            = errors.New("task not found")
	ErrLeaseLost           = errors.New("work lease is no longer owned")
)

type Repository interface {
	CreateTask(context.Context, task.Task) (created task.Task, replayed bool, err error)
	GetTask(context.Context, string, string) (task.Task, error)
	ClaimSubmission(context.Context, string, time.Time, time.Duration) (SubmissionClaim, bool, error)
	CompleteSubmission(context.Context, SubmissionCompletion) error
	ClaimUnknown(context.Context, string, time.Time, time.Duration) (SubmissionClaim, bool, error)
	ResolveUnknown(context.Context, UnknownResolution) error
}

type SubmissionClaim struct {
	Task       task.Task
	LeaseOwner string
}

type SubmissionOutcome string

const (
	SubmissionAccepted  SubmissionOutcome = "ACCEPTED"
	SubmissionDenied    SubmissionOutcome = "DENIED"
	SubmissionAmbiguous SubmissionOutcome = "AMBIGUOUS"
)

type SubmissionCompletion struct {
	TaskID         string
	LeaseOwner     string
	Outcome        SubmissionOutcome
	ProviderTaskID string
	FailureCode    string
}

type ReconciliationOutcome string

const (
	ReconciliationFound        ReconciliationOutcome = "FOUND"
	ReconciliationAbsent       ReconciliationOutcome = "ABSENT"
	ReconciliationInconclusive ReconciliationOutcome = "INCONCLUSIVE"
)

type UnknownResolution struct {
	TaskID         string
	LeaseOwner     string
	Outcome        ReconciliationOutcome
	ProviderTaskID string
}
