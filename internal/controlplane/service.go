package controlplane

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

var tenantPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

type Service struct {
	repository Repository
	now        func() time.Time
	newID      func() string
}

type CreateTaskInput struct {
	TenantID       string
	IdempotencyKey string
	Spec           task.Spec
}

type CreateTaskResult struct {
	Task     task.Task `json:"task"`
	Replayed bool      `json:"replayed"`
}

func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		now:        time.Now,
		newID:      newUUID,
	}
}

func (s *Service) CreateTask(ctx context.Context, input CreateTaskInput) (CreateTaskResult, error) {
	if err := validateCreateInput(input); err != nil {
		return CreateTaskResult{}, err
	}

	now := s.now().UTC()
	digest := task.CanonicalDigest(input.Spec)
	record := task.Task{
		ID:                 s.newID(),
		TenantID:           input.TenantID,
		IdempotencyKey:     input.IdempotencyKey,
		RequestDigest:      digest,
		ProviderRequestKey: providerRequestKey(input.TenantID, input.IdempotencyKey),
		Spec:               input.Spec,
		SubmissionState:    task.SubmissionPending,
		ExecutionState:     task.ExecutionNone,
		Version:            1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	created, replayed, err := s.repository.CreateTask(ctx, record)
	if err != nil {
		return CreateTaskResult{}, err
	}
	return CreateTaskResult{Task: created, Replayed: replayed}, nil
}

func (s *Service) GetTask(ctx context.Context, tenantID, taskID string) (task.Task, error) {
	if !tenantPattern.MatchString(tenantID) || taskID == "" {
		return task.Task{}, ErrNotFound
	}
	return s.repository.GetTask(ctx, tenantID, taskID)
}

func validateCreateInput(input CreateTaskInput) error {
	if !tenantPattern.MatchString(input.TenantID) {
		return fmt.Errorf("%w: invalid tenant", ErrInvalidRequest)
	}
	if !validIdempotencyKey(input.IdempotencyKey) {
		return fmt.Errorf("%w: invalid idempotency key", ErrInvalidRequest)
	}
	if input.Spec.Provider == "" || input.Spec.Model == "" || input.Spec.Operation == "" || input.Spec.Prompt == "" {
		return fmt.Errorf("%w: provider, model, operation, and prompt are required", ErrInvalidRequest)
	}
	return nil
}

func validIdempotencyKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func providerRequestKey(tenantID, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(tenantID + "\x00" + idempotencyKey))
	return "mgw_" + hex.EncodeToString(sum[:16])
}

func newUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}
