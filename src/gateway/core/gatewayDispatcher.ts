import { runtimeAdmission, RuntimeDrainingError } from '@src/core/server/runtimeDrain.js';
import {
  beginOperation,
  finishOperation,
  getOperation,
  updateOperation,
  withOperationContext,
} from '@src/observability/operations/operationJournal.js';
import { isCapabilityListMethod, RESPONSE_JSON_VALUE_LIMITS } from '@src/sdk/contracts/index.js';

import { z } from 'zod';

import {
  authorityAllows,
  createGatewayFailure,
  gatewayFailure,
  gatewayFailureFromUnknown,
  type GatewayRequestEnvelope,
  type GatewayResult,
  gatewaySuccess,
  type ImmutableJsonValue,
  type ProtocolEraPin,
  toImmutableJsonValue,
} from '../contracts/index.js';
import type { OutboundEraAdapter, OutboundGatewayRequest } from '../ports/index.js';

export interface GatewayDispatcherDependencies {
  resolveOutbound(connectionId: string): OutboundEraAdapter | undefined;
  now?: () => number;
  maxActiveRequests?: number;
}

const maxActiveRequestsSchema = z.number().finite().int().positive().max(Number.MAX_SAFE_INTEGER).default(256);

function pinsEqual(left: ProtocolEraPin, right: ProtocolEraPin): boolean {
  return left.era === right.era && left.revision === right.revision;
}

export class GatewayDispatcher {
  private readonly active = new Map<string, OutboundEraAdapter>();
  private readonly dispatched = new WeakSet<GatewayRequestEnvelope>();
  private readonly now: () => number;
  private readonly maxActiveRequests: number;

  constructor(private readonly dependencies: GatewayDispatcherDependencies) {
    this.now = dependencies.now ?? Date.now;
    this.maxActiveRequests = maxActiveRequestsSchema.parse(dependencies.maxActiveRequests);
  }

  async dispatch(request: GatewayRequestEnvelope): Promise<GatewayResult<ImmutableJsonValue>> {
    const operationId = beginOperation('gateway/request', {
      profile: 'gateway',
      backend: request.targetConnectionId,
    });
    const sessionId = operationId ? (getOperation(operationId)?.sessionId ?? '') : '';
    return withOperationContext(operationId ?? '', sessionId, async () => {
      try {
        updateOperation(operationId, { stage: 'routing' });
        const result = await this.dispatchObserved(request, operationId);
        finishOperation(
          operationId,
          result.ok ? 'ok' : result.failure.kind === 'deadline-exceeded' ? 'timeout' : 'failed',
        );
        return result;
      } catch (error) {
        finishOperation(operationId, 'failed');
        throw error;
      }
    });
  }

  private async dispatchObserved(
    request: GatewayRequestEnvelope,
    operationId: string | undefined,
  ): Promise<GatewayResult<ImmutableJsonValue>> {
    if (this.active.has(request.requestId) || this.dispatched.has(request)) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'invalid-request',
          code: 'gateway_request_already_active',
          message: 'The gateway request id has already been dispatched',
        }),
      );
    }
    // Completed operation ownership follows the envelope lifetime, not reusable wire request IDs.
    if (this.active.size >= this.maxActiveRequests) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'transport',
          code: 'gateway_overloaded',
          message: 'Gateway request capacity exceeded',
        }),
      );
    }
    if (!authorityAllows(request.authority, request.targetConnectionId)) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'authorization',
          code: 'gateway_target_not_authorized',
          message: 'The request target is outside the effective authority',
        }),
      );
    }
    if (request.deadlineUnixMs <= this.now()) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'deadline-exceeded',
          code: 'gateway_deadline_exceeded',
          message: 'The gateway request deadline has expired',
        }),
      );
    }

    const outbound = this.dependencies.resolveOutbound(request.targetConnectionId);
    if (!outbound) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'transport',
          code: 'gateway_target_unavailable',
          message: 'No outbound adapter is available',
        }),
      );
    }
    if (!pinsEqual(outbound.pin, request.outbound)) {
      return gatewayFailure(
        createGatewayFailure({
          kind: 'protocol',
          code: 'gateway_outbound_pin_mismatch',
          message: 'The outbound adapter does not match the request era pin',
        }),
      );
    }

    const outboundRequest: OutboundGatewayRequest = Object.freeze({
      requestId: request.requestId,
      operation: request.operation,
      ...(request.params === undefined ? {} : { params: request.params }),
      authority: request.authority,
      deadlineUnixMs: request.deadlineUnixMs,
    });
    try {
      return await runtimeAdmission.run(() => this.dispatchAdmitted(request, outbound, outboundRequest, operationId));
    } catch (error) {
      if (!(error instanceof RuntimeDrainingError)) throw error;
      return gatewayFailure(
        createGatewayFailure({ kind: 'transport', code: 'runtime_draining', message: error.message, data: error.data }),
      );
    }
  }

  private async dispatchAdmitted(
    request: GatewayRequestEnvelope,
    outbound: OutboundEraAdapter,
    outboundRequest: OutboundGatewayRequest,
    operationId: string | undefined,
  ): Promise<GatewayResult<ImmutableJsonValue>> {
    this.active.set(request.requestId, outbound);
    this.dispatched.add(request);
    let terminalResponseObserved = false;
    try {
      const resultLimits =
        outbound.resultLimits ??
        (isCapabilityListMethod(outboundRequest.operation) ? RESPONSE_JSON_VALUE_LIMITS : undefined);
      updateOperation(operationId, { stage: 'dispatch', backendRequest: 'in_flight' });
      const response = await outbound.request(outboundRequest);
      terminalResponseObserved = true;
      updateOperation(operationId, { backendRequest: 'terminal_response_observed' });
      return gatewaySuccess(toImmutableJsonValue(response, resultLimits));
    } catch (error) {
      if (!terminalResponseObserved) updateOperation(operationId, { backendRequest: 'unknown' });
      const failure = gatewayFailureFromUnknown(error, 'transport');
      return gatewayFailure(
        request.operation === 'tools/call' && failure.kind === 'transport'
          ? createGatewayFailure({
              kind: 'transport',
              code: 'gateway_tool_outcome_unknown',
              message: 'Tool execution may have occurred; the outcome is unknown',
            })
          : failure,
      );
    } finally {
      if (this.active.get(request.requestId) === outbound) this.active.delete(request.requestId);
    }
  }

  async cancel(requestId: string): Promise<boolean> {
    const outbound = this.active.get(requestId);
    if (!outbound) return false;
    await outbound.cancel(requestId);
    return true;
  }
}
