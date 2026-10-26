# Model Gateway Lab

A clean-room Go implementation of a reliable control plane for long-running AI generation tasks.

The project focuses on the failure cases hidden behind a simple “call a model API” flow: client retries, worker crashes, provider timeouts after acceptance, duplicate delivery, and eventual reconciliation. Its first release deliberately uses programmable fake providers so every failure window can be reproduced without paid credentials.

## What v1 proves

- the same idempotency key and payload creates one task;
- reusing a key with a different payload is rejected;
- expired worker leases allow another worker to continue safely;
- “accepted remotely but response lost” becomes `UNKNOWN`, not an unsafe retry;
- reconciliation can converge an unknown submission;
- terminal results are emitted through a durable outbox with stable event IDs.

The detailed design is in [docs/design/v1-control-plane.md](docs/design/v1-control-plane.md).

## Provenance

This is an independent clean-room project derived from general distributed-systems problems observed in model-platform work. It does not contain employer source code, internal APIs, data models, configuration, names, or Git history. Historical work and this repository must be evaluated as separate evidence.

## Status

The v1 implementation is in progress. The repository starts with a written contract and test-first execution plan so public claims remain tied to executable evidence.

## License

[MIT](LICENSE)

