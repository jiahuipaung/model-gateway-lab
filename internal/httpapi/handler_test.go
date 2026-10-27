package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if strings.Contains(first.Body.String(), "a paper boat") || strings.Contains(first.Body.String(), `"prompt"`) {
		t.Fatalf("create response leaked prompt: %s", first.Body.String())
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

	replayBody := []byte(`{"prompt":"a paper boat","operation":"text-to-video","model":"video-basic","provider":"fake-a"}`)
	replay := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-1", replayBody)
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
	if strings.Contains(replay.Body.String(), `"prompt"`) {
		t.Fatalf("replay response leaked prompt: %s", replay.Body.String())
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
	validAfterRejected := request(t, handler, http.MethodPost, "/v1/tasks", "team-a", "request-2",
		[]byte(`{"provider":"fake-a","model":"video-basic","operation":"text-to-video","prompt":"one"}`))
	if validAfterRejected.Code != http.StatusAccepted || strings.Contains(validAfterRejected.Body.String(), `"replayed":true`) {
		t.Fatalf("rejected request created state: status=%d body=%s", validAfterRejected.Code, validAfterRejected.Body.String())
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
	read := request(t, handler, http.MethodGet, "/v1/tasks/"+response.Task.ID, "team-a", "", nil)
	if read.Code != http.StatusOK || strings.Contains(read.Body.String(), `"prompt"`) || strings.Contains(read.Body.String(), "one") {
		t.Fatalf("read response leaked prompt or failed: status=%d body=%s", read.Code, read.Body.String())
	}

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

func TestCreateTaskHTTPRejectsUnconfiguredProvider(t *testing.T) {
	t.Parallel()
	response := request(t, newHandler(), http.MethodPost, "/v1/tasks", "team-a", "request-unsupported",
		[]byte(`{"provider":"fake-typo","model":"video-basic","operation":"text-to-video","prompt":"one"}`))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func newHandler() http.Handler {
	return httpapi.NewHandler(controlplane.NewService(memory.NewStore(), "fake-a"))
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
