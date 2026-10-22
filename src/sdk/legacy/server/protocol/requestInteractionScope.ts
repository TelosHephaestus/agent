import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';

import { type InboundConnection, type OutboundConnection, ServerStatus } from '@src/core/types/index.js';
import { hasInteractionCapability } from '@src/gateway/interactions/interactionCapabilities.js';
import { InteractionOwner } from '@src/gateway/interactions/interactionOwner.js';
import { currentNativeInteractionRound } from '@src/gateway/interactions/interactionRoute.js';
import {
  validateInteractionRequest,
  validateInteractionResponse,
} from '@src/gateway/interactions/validateInteractionResponse.js';
import type { GatewayInteractionRequest } from '@src/gateway/ports/outboundEraAdapter.js';
import { currentOperation, updateOperation } from '@src/observability/operations/operationJournal.js';
import { captureTraceContext, injectTraceContext, stripBaggage } from '@src/observability/tracing/context.js';
import {
  hasNonInteractiveProfile,
  supportsRequestLocalInteractions,
  withLegacyInteractionLease,
} from '@src/sdk/legacy/client/runtime/legacyInteractionLease.js';
import { setOutboundRequestHandler } from '@src/sdk/legacy/client/runtime/legacyOutboundConnection.js';
import { getLegacyInboundServer } from '@src/sdk/legacy/server/runtime/legacyInboundConnection.js';
import type { RequestHandlerExtra } from '@src/sdk/legacy/shared/protocol.js';
import {
  type ClientCapabilities,
  CreateMessageRequestSchema,
  CreateMessageResultSchema,
  ElicitRequestSchema,
  ElicitResultSchema,
  ListRootsRequestSchema,
  ListRootsResultSchema,
  McpError,
  PingRequestSchema,
  ServerNotification,
  ServerRequest,
} from '@src/sdk/legacy/types.js';

type Extra = RequestHandlerExtra<ServerRequest, ServerNotification>;
interface Scope {
  readonly inbound: InboundConnection;
  readonly adapter: OutboundConnection['adapter'];
  readonly extra: Extra;
  readonly callbacks: { pending: number };
  readonly operationId?: string;
  readonly runTrace: ReturnType<typeof captureTraceContext>;
  readonly abort: AbortController;
  readonly assertCurrent?: () => void;
  readonly requestLocal: boolean;
}
const legacyOwners = new InteractionOwner();
const active = new WeakMap<OutboundConnection, Scope>();
const requestContext = new AsyncLocalStorage<Scope>();
const requestScopes = new WeakMap<OutboundConnection, Set<Scope>>();
const installed = new WeakMap<OutboundConnection, OutboundConnection['adapter']>();
const profiles = new WeakMap<OutboundConnection, ClientCapabilities>();
const notificationOwners = new WeakMap<OutboundConnection, Set<WeakRef<InboundConnection>>>();
const privateConnections = new WeakMap<InboundConnection, Set<OutboundConnection>>();

export function ownedPrivateInteractionConnections(
  inbound: InboundConnection,
  capability: keyof ClientCapabilities,
): readonly OutboundConnection[] {
  return Array.from(privateConnections.get(inbound) ?? []).filter(
    (connection) => profiles.get(connection)?.[capability] !== undefined,
  );
}

function ownsScope(connection: OutboundConnection, scope: Scope): boolean {
  if (scope.requestLocal) return requestScopes.get(connection)?.has(scope) === true;
  return active.get(connection) === scope;
}

function callbackScope(connection: OutboundConnection): Scope | undefined {
  if (connection.adapter.protocol?.era !== 'modern') return active.get(connection);
  const scope = requestContext.getStore();
  return scope && ownsScope(connection, scope) ? scope : undefined;
}

export function registerLegacyNotificationOwner(connection: OutboundConnection, inbound: InboundConnection): void {
  if (inbound.requestOnly) return;
  let owners = notificationOwners.get(connection);
  if (!owners) notificationOwners.set(connection, (owners = new Set()));
  for (const ref of owners) {
    const owner = ref.deref();
    if (!owner || owner.status === ServerStatus.Disconnected) owners.delete(ref);
    else if (owner === inbound) return;
  }
  owners.add(new WeakRef(inbound));
}

export function unregisterLegacyNotificationOwner(
  connections: Iterable<OutboundConnection>,
  inbound: InboundConnection,
): void {
  for (const connection of connections) {
    const owners = notificationOwners.get(connection);
    if (!owners) continue;
    for (const ref of owners) if (!ref.deref() || ref.deref() === inbound) owners.delete(ref);
    if (!owners.size) notificationOwners.delete(connection);
  }
}

export function setRequestInteractionProfile(connection: OutboundConnection, capabilities: ClientCapabilities): void {
  profiles.set(connection, capabilities);
}

/** Share noninteractive peers; reserve callback-capable legacy peers exclusively. */
export async function withRequestInteractionScope<T>(
  connection: OutboundConnection,
  inbound: InboundConnection,
  extra: Extra,
  operation: () => Promise<T>,
  sourceProviderId?: string,
  assertCurrent?: () => void,
): Promise<T> {
  const requestLocal = supportsRequestLocalInteractions(connection.adapter);
  if (!requestLocal && (active.has(connection) || requestScopes.get(connection)?.size))
    throw new McpError(-32000, 'interaction_capacity_exceeded');
  if (extra.signal.aborted) throw new McpError(-32000, 'interaction_lost');
  const auth = extra.authInfo;
  if (auth && (typeof auth.clientId !== 'string' || !auth.clientId || typeof auth.token !== 'string' || !auth.token))
    throw new McpError(-32000, 'interaction_lost');
  const principal = auth
    ? createHash('sha256')
        .update(JSON.stringify([auth.clientId, auth.token]))
        .digest('hex')
    : `legacy:${inbound.connectionId}`;
  // The trusted modern bridge already owns a process/owner reservation across its rounds.
  const admission =
    currentNativeInteractionRound() || hasNonInteractiveProfile(connection.adapter)
      ? undefined
      : legacyOwners.start(
          {
            principal,
            request: String(extra.requestId),
            route: connection.adapter.connectionId,
            provider: sourceProviderId ?? connection.adapter.connectionId,
            generation: connection.adapter.connectionId,
            inbound: 'legacy',
            outbound: connection.adapter.protocol?.era ?? 'legacy',
          },
          Date.now() + 600_000,
        );
  const abort = new AbortController();
  const scopedExtra = {
    ...extra,
    signal: AbortSignal.any([extra.signal, abort.signal, ...(admission ? [admission.signal] : [])]),
  };
  try {
    if (installed.get(connection) !== connection.adapter) {
      const installedAdapter = connection.adapter;
      setOutboundRequestHandler(connection, PingRequestSchema, async () => ({}));
      const schemas = [
        [CreateMessageRequestSchema, CreateMessageResultSchema, 'sampling'],
        [ElicitRequestSchema, ElicitResultSchema, 'elicitation'],
        [ListRootsRequestSchema, ListRootsResultSchema, 'roots'],
      ] as const;
      for (const [schema, resultSchema, capability] of schemas) {
        const profile = profiles.get(connection);
        if (profile && !profile[capability]) continue;
        try {
          setOutboundRequestHandler(connection, schema, async (request: ServerRequest) => {
            const scope = callbackScope(connection);
            if (
              !scope ||
              scope.adapter !== installedAdapter ||
              connection.adapter !== installedAdapter ||
              scope.extra.signal.aborted
            )
              throw new McpError(-32000, 'interaction_lost');
            const assertRoute = () => {
              try {
                scope.assertCurrent?.();
              } catch {
                scope.abort.abort();
                throw new McpError(-32000, 'interaction_lost');
              }
            };
            assertRoute();
            if (
              !hasInteractionCapability(
                getLegacyInboundServer(scope.inbound).getClientCapabilities(),
                request as GatewayInteractionRequest,
              )
            )
              throw new McpError(-32000, 'interaction_capability_required');
            const assertCapability = () => {
              const capabilities = getLegacyInboundServer(scope.inbound).getClientCapabilities();
              if (!hasInteractionCapability(capabilities, request as GatewayInteractionRequest)) {
                scope.abort.abort();
                throw new McpError(-32000, 'interaction_capability_required');
              }
            };
            if (scope.callbacks.pending >= 32) {
              scope.abort.abort();
              throw new McpError(-32000, 'interaction_capacity_exceeded');
            }
            scope.callbacks.pending++;
            updateOperation(scope.operationId, { outstandingCallbacks: scope.callbacks.pending });
            try {
              const input = request as unknown as GatewayInteractionRequest;
              const binding = {
                principal: 'request-scoped-provider',
                request: String(scope.extra.requestId),
                route: scope.adapter.connectionId,
                generation: scope.adapter.connectionId,
                inbound: 'legacy',
                outbound: 'legacy',
              };
              await validateInteractionRequest(input, binding, scope.extra.signal);
              scope.extra.signal.throwIfAborted();
              assertCapability();
              assertRoute();
              const response = await scope.runTrace(
                () =>
                  scope.extra.sendRequest(
                    { ...request, params: injectTraceContext(request.params) } as ServerRequest,
                    resultSchema,
                    {
                      signal: scope.extra.signal,
                    },
                  ),
                scope.extra.signal,
                request.params,
              );
              await validateInteractionResponse(input, response, binding, scope.extra.signal);
              assertCapability();
              assertRoute();
              if (
                scope.extra.signal.aborted ||
                !ownsScope(connection, scope) ||
                connection.adapter !== installedAdapter ||
                scope.adapter !== installedAdapter
              )
                throw new McpError(-32000, 'interaction_lost');
              return stripBaggage(response);
            } finally {
              scope.callbacks.pending--;
              updateOperation(scope.operationId, { outstandingCallbacks: scope.callbacks.pending });
            }
          });
        } catch (error) {
          // Legacy SDKs reject registration for capabilities absent at initialize. Leave
          // those methods unregistered (and fail closed if requested), without blocking ordinary calls.
          if (!(
            error instanceof Error &&
            error.message ===
              `Client does not support ${capability} capability (required for ${schema.shape.method.value})`
          ))
            throw error;
        }
      }
      installed.set(connection, connection.adapter);
    }
    return await withLegacyInteractionLease(
      connection.adapter,
      async () => {
        const scope = Object.freeze({
          inbound,
          extra: scopedExtra,
          adapter: connection.adapter,
          callbacks: { pending: 0 },
          operationId: currentOperation()?.operationId,
          runTrace: captureTraceContext(),
          abort,
          assertCurrent,
          requestLocal,
        });
        if (requestLocal) {
          let scopes = requestScopes.get(connection);
          if (!scopes) requestScopes.set(connection, (scopes = new Set()));
          scopes.add(scope);
        } else active.set(connection, scope);
        if (sourceProviderId) {
          let owned = privateConnections.get(inbound);
          if (!owned) privateConnections.set(inbound, (owned = new Set()));
          owned.add(connection);
        }
        try {
          assertCurrent?.();
          return requestLocal ? await requestContext.run(scope, operation) : await operation();
        } finally {
          abort.abort();
          if (sourceProviderId) {
            const owned = privateConnections.get(inbound);
            owned?.delete(connection);
            if (!owned?.size) privateConnections.delete(inbound);
          }
          if (requestLocal) {
            const scopes = requestScopes.get(connection);
            scopes?.delete(scope);
            if (!scopes?.size) requestScopes.delete(connection);
          } else if (active.get(connection) === scope) active.delete(connection);
        }
      },
      scopedExtra.signal,
      sessionLogLevels.get(inbound),
      getLegacyInboundServer(inbound).getClientCapabilities(),
      assertCurrent,
    );
  } finally {
    if (admission) legacyOwners.finish(admission.id);
  }
}

const logLevels = ['debug', 'info', 'notice', 'warning', 'error', 'critical', 'alert', 'emergency'];
export const sessionLogLevels = new WeakMap<InboundConnection, string>();

export async function forwardScopedNotification(
  connection: OutboundConnection,
  notification: { method: string; params?: Record<string, unknown> },
): Promise<void> {
  // Progress is delivered only by the SDK's request-specific onprogress callback.
  if (notification.method === 'notifications/progress') return;
  const scopes = requestScopes.get(connection);
  // These raw notifications have no portable parent id. Never choose an owner
  // from an ambiguous request-local operation set.
  if (scopes && scopes.size !== 1) return;
  const scope = scopes ? scopes.values().next().value : active.get(connection);
  if (!scope) {
    if (notification.method !== 'notifications/message') return;
    const owners = Array.from(notificationOwners.get(connection) ?? []).flatMap((ref) => {
      const owner = ref.deref();
      return owner?.status === ServerStatus.Connected && !owner.requestOnly ? [owner] : [];
    });
    if (owners.length !== 1) return;
    const level = notification.params?.level;
    if (
      typeof level !== 'string' ||
      logLevels.indexOf(level) < logLevels.indexOf(sessionLogLevels.get(owners[0]) ?? 'info')
    )
      return;
    await getLegacyInboundServer(owners[0]).notification(notification as ServerNotification);
    return;
  }
  if (scope.extra.signal.aborted) return;
  if (notification.method === 'notifications/message') {
    const level = notification.params?.level;
    if (
      typeof level !== 'string' ||
      logLevels.indexOf(level) < logLevels.indexOf(sessionLogLevels.get(scope.inbound) ?? 'info')
    )
      return;
  }
  await scope.extra.sendNotification(notification as ServerNotification);
}

export function ownsActiveInteraction(connection: OutboundConnection, inbound: InboundConnection): boolean {
  let scope: Scope | undefined;
  if (requestScopes.has(connection)) {
    const scopes = requestScopes.get(connection);
    if (scopes?.size !== 1) return false;
    scope = scopes.values().next().value;
  } else scope = active.get(connection);
  if (scope?.inbound !== inbound || scope.adapter !== connection.adapter || scope.extra.signal.aborted) return false;
  try {
    scope.assertCurrent?.();
    return true;
  } catch {
    return false;
  }
}
