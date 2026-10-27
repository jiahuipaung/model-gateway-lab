# Failure Windows and Executable Guarantees

This document explains what can fail between local persistence and an external provider side effect, how v1 models that failure, and which test makes the claim executable.

## 1. Intake and idempotency

| Failure window | Unsafe behavior | v1 behavior | Evidence |
|---|---|---|---|
| Client retries the same request | Create two paid generation jobs | `(tenant_id, idempotency_key)` replays the original task when the request digest matches | `TestCreateTaskReplaysSameRequest`, `TestCreateTaskIsAtomicAndIdempotent` |
| Client reuses a key for different content | Silently return the wrong prior result | Reject with `409 idempotency_conflict` | `TestCreateTaskRejectsIdempotencyConflict`, `TestCreateTaskHTTPRejectsConflictAndUnknownFields` |
| Process exits during intake | Persist only part of the request state | Request ledger and task with durable submit intent are committed in one transaction | `TestCreateTaskIsAtomicAndIdempotent` |

The task row carries the durable submit intent through `PENDING` and `SUBMITTING`; a separate broker is not required for this first release.

## 2. Worker ownership

| Failure window | Unsafe behavior | v1 behavior | Evidence |
|---|---|---|---|
| Two workers scan the same pending task | Both call the provider | PostgreSQL claims one row with `FOR UPDATE SKIP LOCKED` and records a unique attempt token plus expiry | `TestCreateTaskIsAtomicAndIdempotent` |
| Submit worker crashes after a possible remote side effect | Blind retry creates duplicate paid work | Expiry moves the task to `UNKNOWN`; only a proven `ABSENT` result returns it to `PENDING` | `TestExpiredSubmissionMustReconcileBeforeResubmission`, `TestExpiredSubmissionIsReconciledAndFencedBeforeRetry` |
| Two processes share the same logical worker name | A stale completion matches the new owner string | Every acquisition receives a new random fencing token | `TestClaimTokensFenceSameLogicalWorkerAfterTakeover`, `TestStalePollOwnerCannotCompleteAfterTakeover` |
| Old worker finishes after takeover | Old result overwrites new state | Completion is conditional on the current attempt token | `TestStalePollOwnerCannotCompleteAfterTakeover` |

Leases provide local ownership, not exactly-once external effects. Fencing prevents stale local completion; reconciliation is what closes the ambiguous remote-submit window.

## 3. Provider submission ambiguity

```text
local SUBMITTING -- request reaches provider --> provider task exists
        |                                      |
        +---- response is lost ----------------+
                       |
                    UNKNOWN
```

A read timeout does not prove the provider rejected the request. Retrying immediately could generate twice and charge twice. The worker therefore records `UNKNOWN` unless it receives a typed definite rejection.

The reconciler queries by stable `provider_request_key`:

- `FOUND` → attach the original provider task and continue polling;
- `ABSENT` → return to `PENDING`, making a new submission safe;
- `INCONCLUSIVE` or lookup error → remain `UNKNOWN`.

Evidence: `TestSubmissionWorkerMapsProviderOutcomes` and `TestReconcilerDistinguishesFoundAbsentAndInconclusive`.

The local scenario provider stores its simulated remote state separately in PostgreSQL. A gateway restart can therefore distinguish `FOUND` from `ABSENT` and continue polling unfinished demo tasks. Evidence: `TestScenarioProviderCanRecoverKnownDeterministicTasksAfterRestart` and `TestScenarioProviderStateSurvivesGatewayReconstruction`.

## 4. Completion and result delivery

| Failure window | Unsafe behavior | v1 behavior | Evidence |
|---|---|---|---|
| Task reaches terminal state but event insert fails | State is complete but no notification exists | Terminal update and outbox insert share one database transaction | `TestTerminalUpdateAndOutboxRedelivery` |
| Event is delivered but process exits before marking sent | A retry produces a logically new event | Redelivery reuses deterministic `event_id` | `TestOutboxRedeliveryKeepsStableEventID` |
| Sink is temporarily unavailable | Event is lost | Dispatcher releases the claim and retries later | `TestDispatcherReleasesFailedDeliveryForRetry` |
| One task or event repeatedly fails | It remains the oldest item and starves all later work | Retry eligibility times rotate failed work behind currently eligible items | `TestPollRetryDoesNotStarveAnotherTask`, `TestFailedOutboxDeliveryDoesNotStarveAnotherEvent`, `TestPollRetryIsFairAndRegressiveObservationIsIgnored` |
| Provider returns a stale `QUEUED` observation after `RUNNING` | Local lifecycle moves backwards | Completion checks the domain transition under the repository lock | `TestRegressiveProviderObservationDoesNotMoveRunningTaskBackToQueued`, `TestPollRetryIsFairAndRegressiveObservationIsIgnored` |

Delivery is intentionally **at least once**. Consumers must deduplicate by `event_id`; this project does not claim exactly-once delivery.

`make verify` uses a dedicated test database and never truncates the demo database. API tests also assert that create, replay, and read responses omit prompts.

## 5. What remains outside v1

- provider-specific rate limits and circuit breakers;
- cost/latency-aware routing and safe cross-provider failover;
- quota reservation, settlement, and billing reconciliation;
- artifact download and object storage recovery;
- cancellation races and callbacks to untrusted URLs.

These are future milestones, not implied capabilities of the current code.
