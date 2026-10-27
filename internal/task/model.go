package task

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type SubmissionState string

const (
	SubmissionPending    SubmissionState = "PENDING"
	SubmissionSubmitting SubmissionState = "SUBMITTING"
	SubmissionConfirmed  SubmissionState = "CONFIRMED"
	SubmissionRejected   SubmissionState = "REJECTED"
	SubmissionUnknown    SubmissionState = "UNKNOWN"
)

func (s SubmissionState) CanTransitionTo(next SubmissionState) bool {
	switch s {
	case SubmissionPending:
		return next == SubmissionSubmitting
	case SubmissionSubmitting:
		return next == SubmissionConfirmed || next == SubmissionRejected || next == SubmissionUnknown
	case SubmissionUnknown:
		return next == SubmissionConfirmed || next == SubmissionPending
	default:
		return false
	}
}

type ExecutionState string

const (
	ExecutionNone      ExecutionState = "NONE"
	ExecutionQueued    ExecutionState = "QUEUED"
	ExecutionRunning   ExecutionState = "RUNNING"
	ExecutionSucceeded ExecutionState = "SUCCEEDED"
	ExecutionFailed    ExecutionState = "FAILED"
)

func (s ExecutionState) CanTransitionTo(next ExecutionState) bool {
	switch s {
	case ExecutionNone:
		return next == ExecutionQueued
	case ExecutionQueued:
		return next == ExecutionQueued || next == ExecutionRunning || next == ExecutionSucceeded || next == ExecutionFailed
	case ExecutionRunning:
		return next == ExecutionRunning || next == ExecutionSucceeded || next == ExecutionFailed
	default:
		return false
	}
}

type Spec struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Operation string `json:"operation"`
	Prompt    string `json:"prompt"`
}

func CanonicalDigest(spec Spec) string {
	payload, err := json.Marshal(spec)
	if err != nil {
		panic("task.Spec contains only JSON-safe fields: " + err.Error())
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

type Task struct {
	ID                 string          `json:"id"`
	TenantID           string          `json:"tenant_id"`
	IdempotencyKey     string          `json:"-"`
	RequestDigest      string          `json:"-"`
	ProviderRequestKey string          `json:"-"`
	Spec               Spec            `json:"spec"`
	SubmissionState    SubmissionState `json:"submission_state"`
	ExecutionState     ExecutionState  `json:"execution_state"`
	ProviderTaskID     string          `json:"provider_task_id,omitempty"`
	OutputRef          string          `json:"output_ref,omitempty"`
	FailureCode        string          `json:"failure_code,omitempty"`
	Version            int64           `json:"version"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}
