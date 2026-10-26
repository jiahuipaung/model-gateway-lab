package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jiahuipaung/model-gateway-lab/internal/controlplane"
	"github.com/jiahuipaung/model-gateway-lab/internal/httpapi"
	"github.com/jiahuipaung/model-gateway-lab/internal/memory"
)

func TestCreateTaskHTTPAndReplay(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	body := []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"a paper boat"}`)

	first := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}
	var firstResponse struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
		Replayed bool `json:"replayed"`
	}
	decodeResponse(t, first, &firstResponse)
	if firstResponse.Task.ID == "" || firstResponse.Replayed {
		t.Fatalf("unexpected first response: %#v", firstResponse)
	}

	replay := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", body)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("replay status = %d, body = %s", replay.Code, replay.Body.String())
	}
	var replayResponse struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
		Replayed bool `json:"replayed"`
	}
	decodeResponse(t, replay, &replayResponse)
	if !replayResponse.Replayed || replayResponse.Task.ID != firstResponse.Task.ID {
		t.Fatalf("unexpected replay response: %#v", replayResponse)
	}
}

func TestCreateTaskHTTPRejectsConflictAndUnknownFields(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"one"}`))

	conflict := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"two"}`))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, body = %s", conflict.Code, conflict.Body.String())
	}

	unknown := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-2", []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"one","secret":"no"}`))
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown-field status = %d, body = %s", unknown.Code, unknown.Body.String())
	}
}

func TestGetTaskHTTPIsTenantIsolated(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	created := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"one"}`))
	var response struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	decodeResponse(t, created, &response)

	missing := request(t, handler, http.MethodGet, "/v1/tasks/"+response.Task.ID, "team-b", "", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestCreateTaskHTTPRejectsInvalidHeaders(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	response := request(t, handler, http.MethodPost, "/v1/tasks", "Team A", "", []byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"one"}`))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func newHandler() http.Handler {
	return httpapi.NewHandler(controlplane.NewService(memory.NewStore()))
}

func request(t *testing.T, handler http.Handler, method, path, tenant, key string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenant)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}
