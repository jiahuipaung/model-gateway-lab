# Model Gateway Lab v1 Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable Go control plane that makes long-running AI task submission idempotent, lease-safe, reconcilable after unknown provider outcomes, and durably observable through an outbox.

**Architecture:** A small HTTP API writes the request ledger, task, and submit command transactionally. Independent workers claim work with expiring leases, call a typed Provider port, reconcile ambiguous submissions, poll confirmed tasks, and publish terminal events from an outbox. PostgreSQL is the production adapter; an in-memory adapter drives deterministic unit tests.

**Tech Stack:** Go 1.24, standard `net/http`, PostgreSQL 16, `pgx/v5`, Docker Compose, Make.

**Spec:** `docs/design/v1-control-plane.md`

## Global Constraints

- This is a clean-room implementation: no employer code, API shapes, schemas, identifiers, comments, fixtures, configuration, or Git history may be copied.
- v1 uses only programmable fake providers and requires no paid credentials.
- Every production behavior begins with a test that is observed failing for the intended reason.
- State transitions use typed states and conditional updates; external error handling never depends on message-string matching.
- Logs and API responses must not expose prompts, credentials, or provider response bodies.
- v1 does not claim exactly-once delivery.

## Review Focus

- Empty, oversized, or non-ASCII idempotency keys must be rejected without creating state; Task 2 tests this.
- Canonically equivalent JSON requests must replay while materially different requests conflict; Task 2 tests this.
- A stale lease owner must not complete a command after takeover; Task 3 tests this.
- Reconciliation must distinguish provider “absent” from temporary lookup failure; Task 3 tests this.
- Cross-tenant reads must return the same result as missing tasks; Task 2 tests this.

---

### Task 1: Domain contract and provider outcomes

**Files:**
- Create: `go.mod`
- Create: `internal/task/model.go`
- Create: `internal/task/model_test.go`
- Create: `internal/provider/provider.go`
- Create: `internal/provider/provider_test.go`

**Interfaces:**
- Produces: typed `task.SubmissionState`, `task.ExecutionState`, `task.Task`, `provider.Provider`, typed submission/lookup/poll outcomes, and `task.CanonicalDigest(Spec) string`.

- [ ] **Step 1:** Write table tests for legal/illegal state transitions and stable request digests.
- [ ] **Step 2:** Run `go test ./internal/task` and observe failures because the domain API is absent.
- [ ] **Step 3:** Implement the minimum immutable constants, validation methods, and digest function.
- [ ] **Step 4:** Write provider outcome classification tests and observe the missing types fail.
- [ ] **Step 5:** Implement the Provider port and typed errors/outcomes.
- [ ] **Step 6:** Run `go test ./...` and require PASS.
- [ ] **Step 7:** Commit `feat: define task and provider contracts`.

### Task 2: Idempotent intake and tenant-safe HTTP API

**Files:**
- Create: `internal/controlplane/repository.go`
- Create: `internal/controlplane/service.go`
- Create: `internal/controlplane/service_test.go`
- Create: `internal/memory/store.go`
- Create: `internal/httpapi/handler.go`
- Create: `internal/httpapi/handler_test.go`

**Interfaces:**
- Consumes: `task.Spec`, `task.Task`, and `task.CanonicalDigest` from Task 1.
- Produces: `controlplane.Repository`, `controlplane.Service.CreateTask`, `controlplane.Service.GetTask`, and `httpapi.NewHandler`.

- [ ] **Step 1:** Test same-key/same-request replay, same-key/different-request conflict, invalid key/tenant rejection, and tenant-isolated reads.
- [ ] **Step 2:** Run the focused tests and observe missing service/store failures.
- [ ] **Step 3:** Implement the Repository contract, atomic in-memory create, and service validation.
- [ ] **Step 4:** Test HTTP `202`, replay metadata, `409`, validation errors, and cross-tenant `404`.
- [ ] **Step 5:** Implement the standard-library HTTP handler with bounded JSON decoding and structured errors.
- [ ] **Step 6:** Run `go test ./...` and require PASS.
- [ ] **Step 7:** Commit `feat: add idempotent task intake api`.

### Task 3: Lease-safe submission and UNKNOWN reconciliation

**Files:**
- Create: `internal/controlplane/submission.go`
- Create: `internal/controlplane/submission_test.go`
- Create: `internal/fakeprovider/provider.go`
- Create: `internal/fakeprovider/provider_test.go`
- Modify: `internal/controlplane/repository.go`
- Modify: `internal/memory/store.go`

**Interfaces:**
- Consumes: Provider port from Task 1 and Repository/Task from Task 2.
- Produces: `controlplane.SubmissionWorker.RunOnce` and `controlplane.Reconciler.RunOnce`; lease claim and conditional completion repository methods.

- [ ] **Step 1:** Test claim exclusivity, lease-expiry takeover, and rejection of stale-owner completion.
- [ ] **Step 2:** Run focused tests and observe missing claim behavior fail.
- [ ] **Step 3:** Implement lease claim and conditional completion in the in-memory adapter.
- [ ] **Step 4:** Test accepted, definite rejection, and unknown submit outcomes using a programmable fake provider.
- [ ] **Step 5:** Implement `SubmissionWorker.RunOnce` with typed outcome handling.
- [ ] **Step 6:** Test reconciliation found, proven absent, and inconclusive lookup outcomes.
- [ ] **Step 7:** Implement `Reconciler.RunOnce`; only proven absence may requeue submission.
- [ ] **Step 8:** Run `go test ./...` and require PASS.
- [ ] **Step 9:** Commit `feat: reconcile unknown provider submissions`.

### Task 4: Polling and durable result delivery

**Files:**
- Create: `internal/controlplane/completion.go`
- Create: `internal/controlplane/completion_test.go`
- Modify: `internal/controlplane/repository.go`
- Modify: `internal/memory/store.go`
- Modify: `internal/fakeprovider/provider.go`

**Interfaces:**
- Consumes: confirmed tasks, Provider.Poll, and lease semantics.
- Produces: `controlplane.Poller.RunOnce`, `controlplane.Dispatcher.RunOnce`, `controlplane.EventSink`, and stable terminal event IDs.

- [ ] **Step 1:** Test queued/running/terminal polling transitions and stale poll-lease rejection.
- [ ] **Step 2:** Implement poll claims and conditional transitions; commit terminal state and event together.
- [ ] **Step 3:** Test at-least-once redelivery where send succeeds but mark-sent fails; assert identical event IDs.
- [ ] **Step 4:** Implement outbox claim, retry release, and conditional sent marking.
- [ ] **Step 5:** Run `go test ./...` and require PASS.
- [ ] **Step 6:** Commit `feat: deliver terminal task events through outbox`.

### Task 5: PostgreSQL source of truth

**Files:**
- Create: `internal/postgres/migrations/001_init.sql`
- Create: `internal/postgres/store.go`
- Create: `internal/postgres/store_integration_test.go`
- Create: `docker-compose.yml`
- Modify: `go.mod`
- Modify: `internal/controlplane/repository.go`

**Interfaces:**
- Consumes: the complete Repository contract from Tasks 2–4.
- Produces: `postgres.Open`, `postgres.Migrate`, and a Repository implementation using transactions, unique constraints, conditional versions, and `FOR UPDATE SKIP LOCKED` lease claims.

- [ ] **Step 1:** Add integration tests for atomic create, idempotency conflict, exclusive claim, lease takeover, stale-owner rejection, terminal+outbox atomicity, and stable event redelivery.
- [ ] **Step 2:** Start PostgreSQL and run the integration tests; observe failure because the adapter/schema is absent.
- [ ] **Step 3:** Add the migration and minimal pgx adapter method by method, rerunning each focused test to green.
- [ ] **Step 4:** Run `go test ./...` and the tagged PostgreSQL integration suite; require PASS.
- [ ] **Step 5:** Commit `feat: persist control plane state in postgres`.

### Task 6: Runnable composition and public evidence

**Files:**
- Create: `cmd/gateway/main.go`
- Create: `cmd/gateway/main_test.go`
- Create: `Makefile`
- Create: `scripts/demo.sh`
- Create: `docs/design/failure-windows.md`
- Modify: `README.md`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: HTTP handler, workers, fake provider, and PostgreSQL adapter.
- Produces: one-command local startup, deterministic demo, and an external-facing explanation tied to tests.

- [ ] **Step 1:** Test configuration validation and graceful composition without real provider credentials.
- [ ] **Step 2:** Implement the API/worker process and bounded shutdown.
- [ ] **Step 3:** Add Make targets for unit tests, integration tests, startup, demo, and verification.
- [ ] **Step 4:** Run the normal and lost-response demo; capture expected state convergence in documentation.
- [ ] **Step 5:** Rewrite README as a public portfolio entry with architecture, guarantees, non-goals, quick start, test evidence, and clean-room provenance.
- [ ] **Step 6:** Run `gofmt`, `go vet ./...`, `go test ./...`, PostgreSQL integration tests, and the demo.
- [ ] **Step 7:** Scan tracked files for secrets, internal hostnames, company-specific identifiers, and accidental source provenance leaks.
- [ ] **Step 8:** Commit `docs: publish model gateway lab v1`.

