package controlplane

import (
	"context"
	"errors"

	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

var (
	ErrInvalidRequest      = errors.New("invalid request")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrNotFound            = errors.New("task not found")
)

type Repository interface {
	CreateTask(context.Context, task.Task) (created task.Task, replayed bool, err error)
	GetTask(context.Context, string, string) (task.Task, error)
}
