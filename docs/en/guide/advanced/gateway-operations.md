---
title: Inspect gateway operations
description: Query unfinished MCP work, inspect caller and backend RPC outcomes, and export bounded metadata to Clio.
---

# Inspect gateway operations

Use the read-only operation API to inspect work before its caller finishes. The journal works at `--log-level warn` and does not depend on Clio availability. It does not change routing, admission, cancellation, or retry policy.

## Query the running gateway

Run the script from a source checkout, or use the script included with the household release. Supply your gateway's HTTP origin and an existing credential JSON file. The file supports `token`, `bearer`, or `access_token`. A path is passed on the command line, never the token value.

```sh
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --active
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --limit 100 --json
```

Replace port `3000` with the running gateway's listening port and replace the credential-file placeholder. Omit `--credential-file` only when the target already has authentication disabled. The script does not disable authentication. Use an origin without `/mcp`, a query, or URL credentials.

The list reports local coverage, unfinished or uncertain operations, pending caller handlers, callbacks, private connections, export status, and native backend loading/retry state. Backend readiness comes from the existing `/health/mcp` endpoint. An unavailable health read appears as unavailable, never as an empty healthy inventory.

Copy an operation ID from the list to inspect its bounded timeline:

```sh
node scripts/gateway-observability.mjs --url http://127.0.0.1:3000 --credential-file /path/to/gateway-credential.json --operation OPERATION_ID
```

Unknown or expired IDs return a nonzero exit. Authentication, HTTP, malformed-response, and invalid-option failures also return nonzero. Exit zero means the query succeeded, not that every backend or observed operation is healthy. Requests are GET-only and do not dispatch MCP tools.

## Interpret separate lifetimes

`callerOutcome` records the gateway handler's known outcome. Client-side waiting can end before the gateway receives cancellation or detects a disconnect. `backendRequest` describes observed MCP RPC evidence:

- `not_dispatched` means no backend dispatch has been recorded.
- `in_flight` means a tracked RPC has no terminal response yet.
- `terminal_response_observed` means its terminal RPC response was observed.
- `unknown` means the gateway cannot establish terminal RPC evidence.

A terminal RPC response is not proof that external effects or detached business work ended. Cancellation alone does not prove backend termination. Pending callbacks and private closes remain visible after the caller finishes. Multiple tracked RPCs must all receive terminal evidence before their operation can settle.

`summary.active` counts unfinished or uncertain observed operations. `summary.callerActive` counts pending caller handlers. `elapsedMs` measures the caller handler; `ageMs` keeps advancing while observed backend or private-close work remains unsettled and freezes when it settles. Counters accumulate for the current process; they are not rolling-window success rates. A new process has a new `instanceId` and no earlier in-memory records.

Coverage applies to the methods listed in `coverage.methods`: legacy tool calls, resource reads, prompt gets, completions, and gateway-dispatch operations. It does not claim every discovery request or notification is recorded. The existing household sampler supplies separate catalog and functional-read evidence. `coverage.complete` reports local observation loss, not backend health or confirmed historical export.

The journal keeps at most 512 unfinished observations and 2,048 settled observations, with 32 timeline points per operation. History rotation increments `historyEvicted` without claiming observation failure. Observation overflow increments `dropped` and leaves serving behavior unchanged. An unknown or evicted record is never proof that work ended.

## Enable existing-owner export

Set all three variables in the gateway's supervisor configuration. For a systemd-managed gateway, add these environment entries to its existing service override:

```ini
[Service]
Environment=MCP_GATEWAY_OBSERVABILITY_OMPHALOS_URL=https://omphalos.example.net
Environment=MCP_GATEWAY_OBSERVABILITY_SERVICE=gateway-operations
Environment=MCP_GATEWAY_OBSERVABILITY_TIER=production
```

Replace the example origin with your existing Omphalos origin. It is not the gateway's listening URL. Supply a stable service label and choose `production` or `staging` explicitly. Reload the supervisor configuration and restart only the intended gateway through your existing deployment procedure. For a direct shell launch, use `export` assignments before starting the new gateway process instead. Changing your shell environment does not reconfigure an already-running or supervisor-managed process.

An incomplete, non-origin, or invalid configuration leaves export disabled and reports `invalid_config`; it does not refuse gateway startup.

The exporter sends safe operation events through `ingest_logs`. It never forwards console output, tool arguments/results, callback contents, credentials, raw session/progress tokens, or client trace metadata. Configured backend references, opaque operation/session IDs, fixed lifecycle categories, and timings remain available.

The queue holds 256 events and exports batches of at most 32 once per second. One request can be active at a time. Its deadline is five seconds, and its response parser stops at 4,096 bytes. Queue overflow, rejected or uncertain batches, and timeouts increment export drops. Failed batches are not replayed. Export never waits in a tool handler and has no gateway-owned durable outbox.

The API's `export` object exposes enablement, queue depth, an active batch, drops, acknowledged batches, and a bounded failure category. Acknowledgement means the owner accepted a batch. It does not prove storage. Confirm an exact operation through the existing `query_logs` capability using the configured service, operation UUID in the regex pattern, explicit time bounds, and a bounded result limit. Returned `line` values contain safe event JSON. Clio owns historical retention.

This feature adds neither an OTLP collector nor spans. It does not create a second incident or messaging owner. Production household incident and notification integration must be verified separately through Phylax and Kerux.
