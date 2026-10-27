package controlplane_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/memory"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

func TestCreateTaskReplaysSameRequest(t *testing.T) {
	t.Parallel()

	svc := controlplane.NewService(memory.NewStore(), "fake-a")
	input := validCreateInput()

	first, err := svc.CreateTask(context.Background(), input)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	second, err := svc.CreateTask(context.Background(), input)
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}

	if first.Replayed {
		t.Fatal("first create must not be marked replayed")
	}
	if !second.Replayed || second.Task.ID != first.Task.ID {
		t.Fatalf("replay = %#v, want original task %q", second, first.Task.ID)
	}
}

func TestCreateTaskRejectsIdempotencyConflict(t *testing.T) {
	t.Parallel()

	svc := controlplane.NewService(memory.NewStore(), "fake-a")
	input := validCreateInput()
	if _, err := svc.CreateTask(context.Background(), input); err != nil {
		t.Fatalf("first create: %v", err)
	}
	input.Spec.Prompt = "a different request"

	_, err := svc.CreateTask(context.Background(), input)
	if !errors.Is(err, controlplane.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCreateTaskValidatesTenantAndIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tenant string
		key    string
	}{
		{"empty tenant", "", "request-1"},
		{"uppercase tenant", "Team-A", "request-1"},
		{"tenant punctuation", "team.a", "request-1"},
		{"empty key", "team-a", ""},
		{"key contains space", "team-a", "request 1"},
		{"non ascii key", "team-a", "请求-1"},
		{"oversized key", "team-a", strings.Repeat("x", 129)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := controlplane.NewService(memory.NewStore(), "fake-a")
			input := validCreateInput()
			input.TenantID = tt.tenant
			input.IdempotencyKey = tt.key

			_, err := svc.CreateTask(context.Background(), input)
			if !errors.Is(err, controlplane.ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestGetTaskHidesCrossTenantTask(t *testing.T) {
	t.Parallel()

	svc := controlplane.NewService(memory.NewStore(), "fake-a")
	created, err := svc.CreateTask(context.Background(), validCreateInput())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = svc.GetTask(context.Background(), "another-tenant", created.Task.ID)
	if !errors.Is(err, controlplane.ErrNotFound) {
		t.Fatalf("cross-tenant error = %v, want ErrNotFound", err)
	}
}

func TestCreateTaskRejectsUnconfiguredProviderBeforePersistence(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	svc := controlplane.NewService(store, "fake-a")
	input := validCreateInput()
	input.Spec.Provider = "fake-typo"

	if _, err := svc.CreateTask(context.Background(), input); !errors.Is(err, controlplane.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
	if _, err := svc.GetTask(context.Background(), input.TenantID, "anything"); !errors.Is(err, controlplane.ErrNotFound) {
		t.Fatalf("invalid request created state: %v", err)
	}
}

func validCreateInput() controlplane.CreateTaskInput {
	return controlplane.CreateTaskInput{
		TenantID:       "team-a",
		IdempotencyKey: "request-1",
		Spec: task.Spec{
			Provider:  "fake-a",
			Model:     "video-basic",
			Operation: "text-to-video",
			Prompt:    "a paper boat",
		},
	}
}
