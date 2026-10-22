import { randomUUID } from 'node:crypto';

import { ownData } from '@src/observability/privacy/fields.js';

import { OPERATION_METHODS, type OperationEvent, subscribeOperationEvents } from './operationJournal.js';

const QUEUE_LIMIT = 256;
const BATCH_LIMIT = 32;
const DEADLINE_MS = 5000;
const MAX_RESPONSE_BYTES = 4096;
const ENUM_FIELDS: Readonly<Record<string, readonly string[]>> = {
  kind: [
    'started',
    'stage',
    'caller_terminal',
    'backend_terminal',
    'backend_request',
    'callback',
    'private_connection',
  ],
  method: OPERATION_METHODS,
  profile: ['legacy', 'gateway'],
  stage: ['admitted', 'routing', 'dispatch', 'waiting', 'terminal'],
  callerOutcome: ['ok', 'tool_error', 'failed', 'cancelled', 'timeout', 'unknown'],
  backendRequest: ['not_dispatched', 'in_flight', 'terminal_response_observed', 'unknown'],
};
interface Config {
  url: string;
  service: string;
  tier: 'production' | 'staging';
}
type Failure = 'invalid_config' | 'network' | 'timeout' | 'http' | 'response';
export interface OperationExportStatus {
  enabled: boolean;
  queued: number;
  inFlight: boolean;
  dropped: number;
  acceptedBatches: number;
  lastAcceptedAt: string | null;
  lastFailure: Failure | null;
}
class ExportFailure extends Error {
  constructor(readonly category: 'http' | 'response') {
    super(category);
  }
}
let config: Config | undefined;
let queue: Record<string, unknown>[] = [];
let timer: ReturnType<typeof setInterval> | undefined;
let unsubscribe: (() => void) | undefined;
let activeController: AbortController | undefined;
let activeBatchSize = 0;
let dropped = 0;
let acceptedBatches = 0;
let lastAcceptedAt: string | null = null;
let lastFailure: Failure | null = null;

function validConfig(env: Readonly<Record<string, string | undefined>>): Config | undefined {
  const url = env.ONE_MCP_OBSERVABILITY_OMPHALOS_URL;
  const service = env.ONE_MCP_OBSERVABILITY_SERVICE;
  const tier = env.ONE_MCP_OBSERVABILITY_TIER;
  if (!url || !service || !/^[a-zA-Z0-9_.-]{1,96}$/.test(service)) return;
  if (tier !== 'production' && tier !== 'staging') return;
  try {
    const parsed = new URL(url);
    if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password) return;
    if (parsed.pathname !== '/' || parsed.search || parsed.hash) return;
    return { url: new URL('/invoke', parsed).href, service, tier };
  } catch {
    return;
  }
}

function safeEvent(event: OperationEvent): Record<string, unknown> | undefined {
  if (ownData(event, 'schemaVersion') !== 1) return;
  const result: Record<string, unknown> = { schemaVersion: 1 };
  for (const key of ['instanceId', 'operationId', 'sessionId']) {
    const value = ownData(event, key);
    if (typeof value !== 'string' || !/^[a-f0-9-]{1,128}$/.test(value)) return;
    result[key] = value;
  }
  const at = ownData(event, 'at');
  if (typeof at !== 'string' || at.length > 32 || !Number.isFinite(Date.parse(at))) return;
  result.at = at;
  for (const [key, values] of Object.entries(ENUM_FIELDS)) {
    const value = ownData(event, key);
    if (value === undefined && key !== 'kind' && key !== 'method' && key !== 'profile') continue;
    if (typeof value !== 'string' || !values.includes(value)) return;
    result[key] = value;
  }
  const backend = ownData(event, 'backend');
  if (backend !== null && (typeof backend !== 'string' || backend.length > 128 || !/^[\p{L}\p{N}_.-]+$/u.test(backend)))
    return;
  result.backend = backend;
  for (const key of ['elapsedMs', 'outstandingCallbacks', 'privateConnections']) {
    const value = ownData(event, key);
    if (value === undefined && key !== 'elapsedMs') continue;
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return;
    result[key] = value;
  }
  return result;
}

async function boundedResponse(response: Response): Promise<unknown> {
  if (!response.body) throw new ExportFailure('response');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_RESPONSE_BYTES) throw new ExportFailure('response');
      chunks.push(value);
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      throw new ExportFailure('response');
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

async function flush(): Promise<void> {
  if (!config || activeController || queue.length === 0) return;
  const destination = config;
  const batch = queue.splice(0, BATCH_LIMIT);
  const controller = new AbortController();
  activeController = controller;
  activeBatchSize = batch.length;
  const timeout = setTimeout(() => controller.abort(), DEADLINE_MS);
  try {
    const response = await fetch(destination.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      signal: controller.signal,
      redirect: 'error',
      body: JSON.stringify({
        caller: destination.service,
        request_id: randomUUID(),
        capability: 'ingest_logs',
        tier: destination.tier,
        payload: batch.map((event) => ({
          service: destination.service,
          level: 'info',
          message: JSON.stringify(event),
          logged_at: event.at,
        })),
      }),
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      throw new ExportFailure('http');
    }
    const envelope = await boundedResponse(response);
    const result = ownData(envelope, 'data');
    if (
      ownData(envelope, 'status') !== 'ok' ||
      ownData(result, 'status') !== 'accepted' ||
      ownData(result, 'count') !== batch.length
    ) {
      throw new ExportFailure('response');
    }
    if (activeController !== controller || controller.signal.aborted) return;
    acceptedBatches += 1;
    lastAcceptedAt = new Date().toISOString();
    lastFailure = null;
  } catch (error) {
    if (activeController === controller) {
      dropped += batch.length;
      lastFailure = controller.signal.aborted ? 'timeout' : error instanceof ExportFailure ? error.category : 'network';
    }
  } finally {
    clearTimeout(timeout);
    if (activeController === controller) {
      activeController = undefined;
      activeBatchSize = 0;
    }
  }
}

export function startClioOperationExporter(env: Readonly<Record<string, string | undefined>> = process.env): void {
  if (unsubscribe || timer) return;
  config = validConfig(env);
  if (!config) {
    lastFailure = Object.keys(env).some((key) => key.startsWith('ONE_MCP_OBSERVABILITY_')) ? 'invalid_config' : null;
    return;
  }
  lastFailure = null;
  unsubscribe = subscribeOperationEvents((event) => {
    try {
      const safe = safeEvent(event);
      if (!safe) {
        dropped += 1;
        return;
      }
      if (queue.length >= QUEUE_LIMIT) {
        queue.shift();
        dropped += 1;
      }
      queue.push(safe);
    } catch {
      dropped += 1;
    }
  });
  timer = setInterval(() => {
    void flush();
  }, 1000);
  timer.unref();
}

export function stopClioOperationExporter(): void {
  unsubscribe?.();
  unsubscribe = undefined;
  if (timer) clearInterval(timer);
  timer = undefined;
  dropped += queue.length + activeBatchSize;
  queue = [];
  activeBatchSize = 0;
  const controller = activeController;
  activeController = undefined;
  controller?.abort();
  config = undefined;
}

export function getOperationExportStatus(): OperationExportStatus {
  return {
    enabled: Boolean(config),
    queued: queue.length,
    inFlight: Boolean(activeController),
    dropped,
    acceptedBatches,
    lastAcceptedAt,
    lastFailure,
  };
}
