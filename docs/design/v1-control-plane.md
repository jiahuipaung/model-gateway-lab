# Reliable Async Model Task Control Plane — v1 Specification

## 1. Problem

Long-running image and video generation cannot be treated like a normal request/response proxy. A provider may accept and charge for a task while the client sees a timeout. A worker may crash after a remote side effect but before recording the provider task ID. A result notification may be delivered even if the local “sent” update fails.

The v1 control plane makes those ambiguity windows explicit and proves that the local state eventually converges without blindly repeating external side effects.

## 2. Clean-room boundary

This repository is independently designed and implemented from an empty Git history.

It may use general ideas—idempotency keys, leases, reconciliation, state machines, and transactional outbox—but must not copy employer code, API shapes, DTOs, schemas, topic names, configuration, comments, fixtures, or Git history. Provider behavior is represented only by new programmable fakes.

## 3. System boundary

```text
POST /v1/tasks
  -> idempotency ledger + task with durable submit intent in one transaction
  -> submission worker claims the intent with a lease
  -> provider returns accepted, definite rejection, or unknown outcome
  -> reconciler resolves unknown submissions by provider request key
  -> poller advances confirmed tasks to a terminal execution state
  -> terminal state + outbox event are committed atomically
  -> dispatcher performs at-least-once delivery with a stable event ID
```

PostgreSQL is the production source of truth. In-memory adapters exist only for fast deterministic tests. Provider state is an external observation, not the local authority.

## 4. Public API

### Create task

`POST /v1/tasks`

Required headers:

- `Idempotency-Key`: 1–128 visible ASCII characters.
- `X-Tenant-ID`: 1–64 lowercase letters, digits, `_` or `-`.

JSON body:

```json
{
  "provider": "fake-a",
  "model": "video-basic",
  "operation": "text-to-video",
  "prompt": "a paper boat crossing a quiet lake"
}
```

Returns `202 Accepted` with the task representation. A replay of the same key and canonical request returns the original task with `replayed: true`. The same key with a different canonical request returns `409 Conflict`.

### Read task

`GET /v1/tasks/{task_id}` requires `X-Tenant-ID`. A task owned by another tenant is indistinguishable from a missing task and returns `404 Not Found`.

## 5. State model

Submission state:

```text
PENDING -> SUBMITTING -> CONFIRMED | REJECTED | UNKNOWN
UNKNOWN -> CONFIRMED | PENDING
```

Execution state:

```text
NONE -> QUEUED -> RUNNING -> SUCCEEDED | FAILED
```

Delivery state is represented by outbox rows:

```text
PENDING -> SENDING -> SENT
```

Each transition is conditional on the current state and version. A stale worker must not overwrite newer state.

## 6. Core invariants

1. `(tenant_id, idempotency_key)` is unique and stores a SHA-256 digest of the canonical request.
2. Creating the request ledger and task with its durable submit intent is one database transaction.
3. A command or task claim records `lease_owner` and `lease_expires_at`; only the current owner may complete it.
4. A submission timeout after bytes may have reached the provider is `UNKNOWN`, never a definite failure.
5. Reconciliation uses a stable `provider_request_key`. “Found” confirms the task; “proven absent” requeues it; “inconclusive” keeps it unknown.
6. A terminal task update and its outbox event are one transaction.
7. Event IDs are deterministic per task terminal transition. Redelivery reuses the same event ID.
8. Tenant ownership is checked on every read.

## 7. Provider contract

```go
type Provider interface {
    Submit(context.Context, SubmitRequest) (SubmitResult, error)
    Lookup(context.Context, string) (LookupResult, error)
    Poll(context.Context, string) (PollResult, error)
}
```

Provider errors are typed as `DefiniteError`, `UnknownOutcomeError`, or temporary observation errors. The control plane decides transitions from error semantics, never from string matching.

## 8. Failure scenarios required in v1

- duplicate create with the same body;
- idempotency-key conflict with a different body;
- worker crash followed by lease expiry and takeover;
- provider acceptance followed by a lost submit response;
- reconciliation found / absent / inconclusive;
- duplicate result delivery after “send succeeded, mark sent failed”;
- stale worker completion after another worker owns the lease;
- cross-tenant task lookup.

## 9. Non-goals

- real provider credentials or production provider SDKs;
- multi-provider cost/latency routing and failover;
- quota reservation or billing ledger;
- artifact download/object storage;
- cancellation and UI;
- exactly-once delivery claims.

These can become later milestones only after the v1 reliability contract is executable.

## 10. Acceptance

The release is complete when:

1. `go test ./...` passes without external credentials;
2. PostgreSQL integration tests exercise the transactional and lease invariants;
3. one command starts PostgreSQL, API, workers, and the fake provider behavior;
4. a demo script shows normal completion and unknown-result reconciliation;
5. public documentation distinguishes implemented behavior, design rationale, and future work;
6. a repository scan finds no employer names, internal endpoints, secrets, or copied source artifacts outside the provenance statement.

