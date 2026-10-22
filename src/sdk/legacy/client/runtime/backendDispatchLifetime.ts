import { runtimeAdmission } from '@src/core/server/runtimeDrain.js';
import {
  currentOperation,
  recordBackendRequestStarted,
  recordBackendRequestTerminal,
} from '@src/observability/operations/operationJournal.js';
import type { JSONRPCMessage } from '@src/sdk/legacy/types.js';

import type { AuthProviderTransport } from './legacyTransport.js';

type RequestId = string | number;
interface DispatchObserver {
  readonly pending: Map<RequestId, (() => void) | null>;
  readonly operationIds: Map<RequestId, string | null>;
  onmessage?: AuthProviderTransport['onmessage'];
}
const observers = new WeakMap<AuthProviderTransport, DispatchObserver>();

function retainDispatch(
  pending: DispatchObserver['pending'],
  operationIds: DispatchObserver['operationIds'],
  message: JSONRPCMessage,
): void {
  if (!('id' in message)) return;
  if (!('method' in message)) return;
  if (message.method === 'initialize' || message.method === 'ping') return;
  const release = runtimeAdmission.retain();
  if (!release) return;
  // Reusing a wire ID before completion makes its replies ambiguous. Fail closed.
  const reusedId = pending.has(message.id);
  pending.set(message.id, reusedId ? null : release);
  const operationId = currentOperation()?.operationId;
  if (reusedId) operationIds.set(message.id, null);
  else if (operationId) {
    operationIds.set(message.id, operationId);
    recordBackendRequestStarted(operationId);
  }
}

function releaseDispatch(
  pending: DispatchObserver['pending'],
  operationIds: DispatchObserver['operationIds'],
  message: JSONRPCMessage,
): void {
  if (!('id' in message)) return;
  if (typeof message.id !== 'string' && typeof message.id !== 'number') return;
  if ('method' in message) return;
  if (!('result' in message) && !('error' in message)) return;
  const release = pending.get(message.id);
  if (!release) return;
  pending.delete(message.id);
  release();
  const operationId = operationIds.get(message.id);
  operationIds.delete(message.id);
  if (operationId) recordBackendRequestTerminal(operationId);
}

/**
 * SDK cancellation/timeouts discard their response waiter before backend work is done.
 * Retain the admitted root independently until the transport sees its terminal reply.
 * Transport errors/disconnection cannot establish completion and deliberately retain it.
 */
export function observeBackendDispatchLifetime(transport: AuthProviderTransport): void {
  let observer = observers.get(transport);
  if (!observer) {
    observer = { pending: new Map(), operationIds: new Map() };
    observers.set(transport, observer);
    const pending = observer.pending;
    const operationIds = observer.operationIds;
    const send = transport.send;
    transport.send = function (message, options) {
      retainDispatch(pending, operationIds, message);
      return send.call(this, message, options);
    };
  }
  // SDK connect/reconnect replaces onmessage. Preserve the current callback and reattach our observer.
  if (transport.onmessage !== observer.onmessage || !observer.onmessage) {
    const receive = transport.onmessage;
    const pending = observer.pending;
    const operationIds = observer.operationIds;
    observer.onmessage = (message, extra) => {
      try {
        receive?.(message, extra);
      } finally {
        releaseDispatch(pending, operationIds, message);
      }
    };
    transport.onmessage = observer.onmessage;
  }
}
