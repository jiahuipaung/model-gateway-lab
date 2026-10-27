package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/task"
)

const maxRequestBody = 64 << 10

type handler struct {
	service *controlplane.Service
}

func NewHandler(service *controlplane.Service) http.Handler {
	h := &handler{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tasks", h.createTask)
	mux.HandleFunc("GET /v1/tasks/{taskID}", h.getTask)
	return mux
}

func (h *handler) createTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var spec task.Spec
	if err := decoder.Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be one valid JSON object")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain one JSON object")
		return
	}

	result, err := h.service.CreateTask(r.Context(), controlplane.CreateTaskInput{
		TenantID:       r.Header.Get("X-Tenant-ID"),
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Spec:           spec,
	})
	if err != nil {
		switch {
		case errors.Is(err, controlplane.ErrInvalidRequest):
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		case errors.Is(err, controlplane.ErrIdempotencyConflict):
			writeError(w, http.StatusConflict, "idempotency_conflict", err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal_error", "request could not be accepted")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"task":     taskResponseFrom(result.Task),
		"replayed": result.Replayed,
	})
}

func (h *handler) getTask(w http.ResponseWriter, r *http.Request) {
	record, err := h.service.GetTask(r.Context(), r.Header.Get("X-Tenant-ID"), r.PathValue("taskID"))
	if err != nil {
		if errors.Is(err, controlplane.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "task not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "task could not be read")
		return
	}
	writeJSON(w, http.StatusOK, map[string]taskResponse{"task": taskResponseFrom(record)})
}

type taskResponse struct {
	ID              string               `json:"id"`
	TenantID        string               `json:"tenant_id"`
	Provider        string               `json:"provider"`
	Model           string               `json:"model"`
	Operation       string               `json:"operation"`
	SubmissionState task.SubmissionState `json:"submission_state"`
	ExecutionState  task.ExecutionState  `json:"execution_state"`
	ProviderTaskID  string               `json:"provider_task_id,omitempty"`
	OutputRef       string               `json:"output_ref,omitempty"`
	FailureCode     string               `json:"failure_code,omitempty"`
	Version         int64                `json:"version"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
}

func taskResponseFrom(record task.Task) taskResponse {
	return taskResponse{
		ID: record.ID, TenantID: record.TenantID,
		Provider: record.Spec.Provider, Model: record.Spec.Model, Operation: record.Spec.Operation,
		SubmissionState: record.SubmissionState, ExecutionState: record.ExecutionState,
		ProviderTaskID: record.ProviderTaskID, OutputRef: record.OutputRef, FailureCode: record.FailureCode,
		Version: record.Version, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
