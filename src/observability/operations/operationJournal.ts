import { AsyncLocalStorage } from 'node:async_hooks';
import { createHmac, randomBytes, randomUUID } from 'node:crypto';

import { ownData } from '@src/observability/privacy/fields.js';

export const OPERATION_METHODS = [
  'tools/call',
  'resources/read',
  'prompts/get',
  'completion/complete',
  'gateway/request',
] as const;
export type OperationMethod = (typeof OPERATION_METHODS)[number];
export type CallerOutcome = 'ok' | 'tool_error' | 'failed' | 'cancelled' | 'timeout' | 'unknown';
export type BackendRequestState = 'not_dispatched' | 'in_flight' | 'terminal_response_observed' | 'unknown';
export type OperationStage = 'admitted' | 'routing' | 'dispatch' | 'waiting' | 'terminal';
export type OperationEventKind =
  'started' | 'stage' | 'caller_terminal' | 'backend_terminal' | 'backend_request' | 'callback' | 'private_connection';

export interface OperationEvent {
  schemaVersion: 1;
  instanceId: string;
  operationId: string;
  sessionId: string;
  kind: OperationEventKind;
  at: string;
  method: OperationMethod;
  profile: 'legacy' | 'gateway';
  backend: string | null;
  elapsedMs: number;
  stage?: OperationStage;
  callerOutcome?: CallerOutcome;
  backendRequest?: BackendRequestState;
  outstandingCallbacks?: number;
  privateConnections?: number;
}

export interface OperationTimelinePoint {
  at: string;
  kind: OperationEventKind;
  stage?: OperationStage;
  callerOutcome?: CallerOutcome;
  backendRequest?: BackendRequestState;
  outstandingCallbacks?: number;
  privateConnections?: number;
}

export interface OperationView {
  id: string;
  sessionId: string;
  method: OperationMethod;
  profile: 'legacy' | 'gateway';
  backend: string | null;
  startedAt: string;
  elapsedMs: number;
  stage: OperationStage;
  ageMs: number;
  timeline: OperationTimelinePoint[];
  callerOutcome: CallerOutcome | null;
  backendRequest: BackendRequestState;
  outstandingCallbacks: number;
  privateConnections: number;
}

export interface OperationSnapshot {
  schemaVersion: 1;
  instanceId: string;
  observedAt: string;
  coverage: {
    complete: boolean;
    scope: 'instrumented_invocations';
    methods: OperationMethod[];
    dropped: number;
    historyEvicted: number;
    activeOverflow: number;
    unknownBackendRequests: number;
  };
  summary: {
    started: number;
    finished: number;
    active: number;
    callerActive: number;
    outcomes: Record<CallerOutcome, number>;
  };
  operations: OperationView[];
  sessions: Array<{
    id: string;
    state: 'open' | 'unknown';
    outstandingCallbacks: number;
    privateConnections: number;
    outstandingBackendRequests: number;
  }>;
}

interface Entry extends OperationView {
  startedMonotonic: number;
  pendingBackendRequests: number;
  settledMonotonic?: number;
}
function safeSession(raw: string | undefined, operationId: string): string {
  return createHmac('sha256', hmacKey)
    .update(raw ?? `unknown:${operationId}`)
    .digest('hex')
    .slice(0, 24);
}
function backendReference(value: string | null | undefined): string | null {
  return value != null && value.length <= 128 && /^[\p{L}\p{N}_.-]+$/u.test(value) ? value : null;
}
const ACTIVE_LIMIT = 512;
const HISTORY_LIMIT = 2048;
const TIMELINE_LIMIT = 32;
const instanceId = randomUUID();
const hmacKey = randomBytes(32);
const context = new AsyncLocalStorage<{ operationId: string; sessionId: string }>();
const active = new Map<string, Entry>();
const history: Entry[] = [];
const outcomes: Record<CallerOutcome, number> = {
  ok: 0,
  tool_error: 0,
  failed: 0,
  cancelled: 0,
  timeout: 0,
  unknown: 0,
};
const subscribers = new Set<(event: OperationEvent) => void>();
let started = 0;
let finished = 0;
let dropped = 0;
let historyEvicted = 0;
let activeOverflow = 0;

function event(entry: Entry, kind: OperationEventKind, fields: Partial<OperationEvent> = {}): void {
  const value: OperationEvent = Object.freeze({
    schemaVersion: 1,
    instanceId,
    operationId: entry.id,
    sessionId: entry.sessionId,
    kind,
    at: new Date().toISOString(),
    method: entry.method,
    profile: entry.profile,
    backend: entry.backend,
    elapsedMs: Math.max(0, performance.now() - entry.startedMonotonic),
    ...fields,
  });
  for (const subscriber of subscribers) {
    try {
      subscriber(value);
    } catch {
      dropped += 1;
    }
  }
}

function view(entry: Entry): OperationView {
  const {
    startedMonotonic: _startedMonotonic,
    pendingBackendRequests: _pendingBackendRequests,
    settledMonotonic: _settledMonotonic,
    ...record
  } = entry;
  return {
    ...record,
    elapsedMs: entry.callerOutcome === null ? Math.max(0, performance.now() - entry.startedMonotonic) : entry.elapsedMs,
    ageMs: Math.max(0, (entry.settledMonotonic ?? performance.now()) - entry.startedMonotonic),
    timeline: [...entry.timeline],
  };
}

export function beginOperation(
  method: OperationMethod,
  options: { id?: string; session?: string; profile?: 'legacy' | 'gateway'; backend?: string } = {},
): string | undefined {
  try {
    const id = options.id ?? randomUUID();
    if (active.has(id)) {
      dropped += 1;
      return undefined;
    }
    if (active.size >= ACTIVE_LIMIT) {
      activeOverflow += 1;
      dropped += 1;
      return undefined;
    }
    const sessionId = safeSession(options.session, id);
    const now = new Date().toISOString();
    const entry: Entry = {
      id,
      sessionId,
      method,
      profile: options.profile ?? 'legacy',
      backend: backendReference(options.backend),
      startedAt: now,
      elapsedMs: 0,
      ageMs: 0,
      stage: 'admitted',
      timeline: [{ at: now, kind: 'started', stage: 'admitted', backendRequest: 'not_dispatched' }],
      callerOutcome: null,
      backendRequest: 'not_dispatched',
      outstandingCallbacks: 0,
      privateConnections: 0,
      startedMonotonic: performance.now(),
      pendingBackendRequests: 0,
    };
    active.set(id, entry);
    started += 1;
    event(entry, 'started');
    return id;
  } catch {
    dropped += 1;
    return undefined;
  }
}

export function currentOperation(): { operationId: string; sessionId: string } | undefined {
  return context.getStore();
}
export function withOperationContext<T>(operationId: string, sessionId: string, run: () => T): T {
  return context.run({ operationId, sessionId }, run);
}

function retireSettled(entry: Entry): void {
  if (
    !active.has(entry.id) ||
    entry.callerOutcome === null ||
    entry.backendRequest === 'in_flight' ||
    entry.backendRequest === 'unknown' ||
    entry.pendingBackendRequests > 0 ||
    entry.outstandingCallbacks > 0 ||
    entry.privateConnections > 0
  )
    return;
  active.delete(entry.id);
  entry.settledMonotonic = performance.now();
  if (history.length === HISTORY_LIMIT) {
    history.shift();
    historyEvicted += 1;
  }
  history.push(entry);
}

export function updateOperation(
  id: string | undefined,
  update: Partial<
    Pick<OperationView, 'stage' | 'backendRequest' | 'outstandingCallbacks' | 'privateConnections' | 'backend'>
  >,
): void {
  if (!id) return;
  try {
    const entry = active.get(id) ?? history.find((item) => item.id === id);
    if (!entry) return;
    if (update.backend !== undefined) update = { ...update, backend: backendReference(update.backend) };
    if (update.backendRequest === 'terminal_response_observed' && entry.pendingBackendRequests > 0) {
      update = { ...update, backendRequest: entry.backendRequest === 'unknown' ? 'unknown' : 'in_flight' };
    }
    Object.assign(entry, update);
    const kind: OperationEventKind =
      update.backendRequest === 'terminal_response_observed'
        ? 'backend_terminal'
        : update.backendRequest !== undefined
          ? 'backend_request'
          : update.outstandingCallbacks !== undefined
            ? 'callback'
            : update.privateConnections !== undefined
              ? 'private_connection'
              : 'stage';
    const point: OperationTimelinePoint = {
      at: new Date().toISOString(),
      kind,
      ...(update.stage ? { stage: update.stage } : {}),
      ...(update.backendRequest ? { backendRequest: update.backendRequest } : {}),
      ...(update.outstandingCallbacks !== undefined ? { outstandingCallbacks: update.outstandingCallbacks } : {}),
      ...(update.privateConnections !== undefined ? { privateConnections: update.privateConnections } : {}),
    };
    if (entry.timeline.length === TIMELINE_LIMIT) entry.timeline.shift();
    entry.timeline.push(point);
    event(entry, kind, {
      ...(update.stage ? { stage: update.stage } : {}),
      ...(update.backendRequest ? { backendRequest: update.backendRequest } : {}),
      ...(update.outstandingCallbacks !== undefined ? { outstandingCallbacks: update.outstandingCallbacks } : {}),
      ...(update.privateConnections !== undefined ? { privateConnections: update.privateConnections } : {}),
    });
    retireSettled(entry);
  } catch {
    dropped += 1;
  }
}

export function recordBackendRequestStarted(id: string): void {
  try {
    const entry = active.get(id);
    if (!entry) return;
    entry.pendingBackendRequests += 1;
    updateOperation(id, { backendRequest: 'in_flight' });
  } catch {
    dropped += 1;
  }
}

export function recordBackendRequestTerminal(id: string): void {
  try {
    const entry = active.get(id);
    if (!entry) return;
    entry.pendingBackendRequests = Math.max(0, entry.pendingBackendRequests - 1);
    updateOperation(id, { backendRequest: 'terminal_response_observed' });
  } catch {
    dropped += 1;
  }
}

export function finishOperation(id: string | undefined, outcome: CallerOutcome): void {
  if (!id) return;
  try {
    const entry = active.get(id);
    if (!entry || entry.callerOutcome !== null) return;
    entry.callerOutcome = outcome;
    entry.stage = 'terminal';
    entry.elapsedMs = Math.max(0, performance.now() - entry.startedMonotonic);
    if (entry.timeline.length === TIMELINE_LIMIT) entry.timeline.shift();
    entry.timeline.push({
      at: new Date().toISOString(),
      kind: 'caller_terminal',
      stage: 'terminal',
      callerOutcome: outcome,
    });
    event(entry, 'caller_terminal', { stage: 'terminal', callerOutcome: outcome });
    finished += 1;
    outcomes[outcome] += 1;
    retireSettled(entry);
  } catch {
    dropped += 1;
  }
}

export function subscribeOperationEvents(subscriber: (event: OperationEvent) => void): () => void {
  subscribers.add(subscriber);
  return () => subscribers.delete(subscriber);
}

export function getOperation(id: string): OperationView | undefined {
  const running = active.get(id);
  if (running) return view(running);
  const completed = history.find((entry) => entry.id === id);
  return completed ? view(completed) : undefined;
}

export function getOperationSnapshot(options: { active?: boolean; limit?: number } = {}): OperationSnapshot {
  const limit = options.limit ?? 50;
  const allEntries = [...history, ...active.values()];
  const currentEntries = allEntries.filter(
    (entry) =>
      active.has(entry.id) ||
      entry.outstandingCallbacks > 0 ||
      entry.privateConnections > 0 ||
      entry.backendRequest === 'in_flight' ||
      entry.backendRequest === 'unknown',
  );
  const current = currentEntries.map(view);
  const records = options.active ? current : allEntries.map(view).slice(-limit);
  const sessionMap = new Map<string, OperationSnapshot['sessions'][number]>();
  for (const entry of allEntries) {
    if (
      !active.has(entry.id) &&
      entry.outstandingCallbacks === 0 &&
      entry.privateConnections === 0 &&
      entry.backendRequest !== 'in_flight' &&
      entry.backendRequest !== 'unknown'
    )
      continue;
    const op = view(entry);
    const item = sessionMap.get(op.sessionId) ?? {
      id: op.sessionId,
      state: 'unknown' as const,
      outstandingCallbacks: 0,
      privateConnections: 0,
      outstandingBackendRequests: 0,
    };
    item.outstandingCallbacks += op.outstandingCallbacks;
    item.privateConnections += op.privateConnections;
    if (op.backendRequest === 'in_flight' || op.backendRequest === 'unknown') {
      item.outstandingBackendRequests += Math.max(1, entry.pendingBackendRequests);
    }
    sessionMap.set(item.id, item);
  }
  return {
    schemaVersion: 1,
    instanceId,
    observedAt: new Date().toISOString(),
    coverage: {
      complete: dropped === 0 && activeOverflow === 0,
      scope: 'instrumented_invocations',
      methods: [...OPERATION_METHODS],
      dropped,
      historyEvicted,
      activeOverflow,
      unknownBackendRequests: allEntries.filter((entry) => entry.backendRequest === 'unknown').length,
    },
    summary: {
      started,
      finished,
      active: active.size,
      callerActive: [...active.values()].filter((entry) => entry.callerOutcome === null).length,
      outcomes: { ...outcomes },
    },
    operations: records.slice(-limit),
    sessions: [...sessionMap.values()],
  };
}

export async function observeOperation<T>(
  method: OperationMethod,
  session: string | undefined,
  run: () => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  const id = beginOperation(method, { session });
  try {
    const operationContext = currentOperation();
    const result = await withOperationContext(
      id ?? operationContext?.operationId ?? '',
      id ? (getOperation(id)?.sessionId ?? '') : (operationContext?.sessionId ?? ''),
      run,
    );
    const isError = ownData(result, 'isError') === true;
    finishOperation(id, isError ? 'tool_error' : 'ok');
    return result;
  } catch (error) {
    const errorName = ownData(error, 'name');
    finishOperation(id, signal?.aborted ? 'cancelled' : errorName === 'TimeoutError' ? 'timeout' : 'failed');
    throw error;
  }
}

export function resetOperationJournalForTests(): void {
  active.clear();
  history.splice(0);
  started = 0;
  finished = 0;
  dropped = 0;
  historyEvicted = 0;
  activeOverflow = 0;
  for (const key of Object.keys(outcomes) as CallerOutcome[]) outcomes[key] = 0;
}
