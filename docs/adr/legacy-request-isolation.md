# Legacy request isolation in the household gateway

Ordinary calls must not reserve an entire backend connection or consume the
interactive-continuation owner's quota. Responses, cancellation and progress
already have request-local correlation in the SDK.

## Selected design

The long-lived discovery client advertises an empty client-capability profile by
default. This profile does not promise sampling, roots or elicitation on a shared
connection. Explicit caller profiles are still supported by `ClientFactory`.

`createLegacyOutboundConnection` enrolls the adapter with a live lookup of the
factory's advertised-profile evidence. Both SDK implementations participate,
including a modern SDK client that negotiated the legacy protocol. A successful
public capability mutation invalidates the evidence; unknown and interactive
legacy peers retain exclusive ownership.

Factory-proven noninteractive peers use request-local scopes and SDK request IDs
rather than a connection-wide lease. They do not allocate an interactive
continuation owner. Request cancellation reaches only its own SDK request, and
progress is forwarded only through the SDK's correlated progress callback.
Uncorrelated raw progress is never attributed by guessing an active request.

Callback-capable legacy callers use the existing fresh private-session path,
including ordinary legacy inbound clients without canonical schema projection.
The private peer advertises the caller's capabilities. Provider generation,
operation identity, schema equality and existing callback-isolation checks remain
in force before dispatch. Ordinary requests continue on the shared noninteractive
peer while private callbacks wait. Modern negotiated protocols retain their
existing request-local interaction mechanism.

This is not tool-name classification: neither `readOnlyHint` nor a tool's name
proves whether its implementation can request sampling or user input. Negotiated
client capabilities determine the channel's interaction contract.

## Verification

Run the focused unit tests and the compiled real-HTTP acceptance:

    pnpm exec vitest run src/sdk/legacy/client/runtime/legacyInteractionLease.test.ts src/sdk/legacy/client/runtime/clientFactoryProfile.test.ts src/sdk/legacy/server/protocol/requestLocalConcurrency.test.ts src/sdk/legacy/server/protocol/privateInteractionConnection.test.ts src/sdk/legacy/server/protocol/requestInteractionScope.test.ts
    pnpm build
    node --test test/e2e/legacy-request-concurrency.test.mjs

Set `GATEWAY_ENTRY` to an exact compiled baseline or published artifact entry to
run the same HTTP assertions against it. The harness uses fresh loopback-only
backends, configuration and home directories under `TMPDIR`; it never uses
household credentials or production backends. It retains gateway logs, backend
dispatch counts, negotiated profiles, actual colliding caller progress tokens,
configuration integrity and process-exit evidence.

Acceptance covers a held call versus an independent call on the same client, 40
ordinary overlapping calls beyond the interactive-owner quota, per-request
cancellation, colliding progress-token destinations, two concurrent legacy roots
callbacks with distinct callers, and session-loss recovery while an unrelated
backend remains usable. A fixture-only accepted mutation is counted and must not
be replayed during recovery. No automatic mutation replay was added.

Household deployment remains host-managed: immutable artifact selection changes
only the two person gateway services. It is not an Omphalos application release.
Keep router, tunnel, credentials, backend configurations and observer sampler
unchanged. Record exact public catalog membership and default-identity reads
before and after activation; a fixture pass alone does not establish live
household acceptance.
