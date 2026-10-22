import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  beginOperation,
  finishOperation,
  getOperation,
  getOperationSnapshot,
  observeOperation,
  resetOperationJournalForTests,
  subscribeOperationEvents,
  updateOperation,
} from './operationJournal.js';

afterEach(() => resetOperationJournalForTests());

describe('operation journal', () => {
  it('exposes active work before terminal response and separates caller and backend lifetimes', () => {
    const id = beginOperation('tools/call', { session: 'raw-session-secret' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { stage: 'dispatch', backendRequest: 'in_flight', backend: 'configured-backend' });
    const running = getOperationSnapshot({ active: true });
    expect(running.operations[0]).toMatchObject({
      id,
      stage: 'dispatch',
      callerOutcome: null,
      backendRequest: 'in_flight',
    });
    expect(JSON.stringify(running)).not.toContain('raw-session-secret');
    updateOperation(id, { backendRequest: 'terminal_response_observed' });
    finishOperation(id, 'tool_error');
    expect(getOperation(id)).toMatchObject({
      callerOutcome: 'tool_error',
      backendRequest: 'terminal_response_observed',
    });
    expect(getOperationSnapshot().summary.outcomes.tool_error).toBe(1);
  });

  it('retains cancellation with unknown backend state as two separate facts', () => {
    const id = beginOperation('tools/call', { session: 's' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { backendRequest: 'unknown' });
    finishOperation(id, 'cancelled');
    expect(getOperation(id)).toMatchObject({ callerOutcome: 'cancelled', backendRequest: 'unknown' });
  });

  it('records bounded owner counters and explicit unknown operations', () => {
    const id = beginOperation('resources/read', { session: 's' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { outstandingCallbacks: 1, privateConnections: 1 });
    expect(getOperationSnapshot({ active: true }).sessions[0]).toMatchObject({
      outstandingCallbacks: 1,
      privateConnections: 1,
      state: 'unknown',
    });
    finishOperation(id, 'ok');
    expect(getOperation('evicted-or-unknown')).toBeUndefined();
  });

  it('isolates a throwing subscriber from request completion and records observation loss', () => {
    const spy = vi.fn(() => {
      throw new Error('private provider text');
    });
    const unsubscribe = subscribeOperationEvents(spy);
    const id = beginOperation('prompts/get', { session: 's' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    finishOperation(id, 'ok');
    unsubscribe();
    expect(getOperation(id)?.callerOutcome).toBe('ok');
    expect(getOperationSnapshot().coverage).toMatchObject({ complete: false, dropped: 2 });
    expect(JSON.stringify(getOperationSnapshot())).not.toContain('private provider text');
  });

  it('classifies returned tool errors and rethrows the exact provider error value', async () => {
    const result = await observeOperation('prompts/get', 'session', async () => ({ isError: true }));
    expect(result).toEqual({ isError: true });
    expect(getOperationSnapshot().summary.outcomes.tool_error).toBe(1);
    expect(getOperationSnapshot().operations.at(-1)?.backendRequest).toBe('not_dispatched');
    const thrown = Object.freeze({ name: 'provider-failure', message: 'private detail' });
    await expect(
      observeOperation('resources/read', 'session', async () => {
        throw thrown;
      }),
    ).rejects.toBe(thrown);
    expect(getOperationSnapshot().summary.outcomes.failed).toBe(1);
    expect(JSON.stringify(getOperationSnapshot())).not.toContain('private detail');
  });

  it('keeps a delayed private close visible after caller completion until the owner settles it', () => {
    const id = beginOperation('tools/call', { session: 's' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { privateConnections: 1 });
    finishOperation(id, 'ok');
    expect(getOperationSnapshot({ active: true }).operations[0]).toMatchObject({
      callerOutcome: 'ok',
      privateConnections: 1,
    });
    expect(getOperationSnapshot({ active: true }).sessions[0]).toMatchObject({ privateConnections: 1 });
    updateOperation(id, { privateConnections: 0 });
    expect(getOperationSnapshot({ active: true }).sessions).toEqual([]);
  });

  it('drops observation at capacity without turning the bounded journal into admission control', () => {
    const ids = Array.from({ length: 512 }, () => beginOperation('tools/call', { session: 's' }));
    expect(beginOperation('tools/call', { session: 's' })).toBeUndefined();
    for (const id of ids) finishOperation(id, 'ok');
    expect(getOperationSnapshot().coverage).toMatchObject({ complete: false, dropped: 1, activeOverflow: 1 });
    expect(getOperationSnapshot().summary.finished).toBe(512);
  });

  it('protects post-cancel backend work from history eviction and counts it as unfinished', () => {
    const id = beginOperation('tools/call', { session: 'held' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { backendRequest: 'in_flight' });
    finishOperation(id, 'cancelled');
    expect(getOperationSnapshot().summary).toMatchObject({ active: 1, callerActive: 0, finished: 1 });
    for (let index = 0; index < 2049; index += 1) finishOperation(beginOperation('tools/call'), 'ok');
    expect(getOperation(id)).toMatchObject({ callerOutcome: 'cancelled', backendRequest: 'in_flight' });
    expect(getOperationSnapshot({ active: true }).operations).toHaveLength(1);
    updateOperation(id, { backendRequest: 'terminal_response_observed' });
    expect(getOperationSnapshot({ active: true }).operations).toEqual([]);
    expect(getOperationSnapshot().summary.active).toBe(0);
    expect(getOperationSnapshot().summary.finished).toBe(2050);
  });

  it('treats normal bounded history rotation separately from observation loss', () => {
    for (let index = 0; index < 2049; index += 1) finishOperation(beginOperation('tools/call'), 'ok');
    expect(getOperationSnapshot().coverage).toMatchObject({ complete: true, historyEvicted: 1, dropped: 0 });
  });

  it('protects a pending private close after the caller returns', () => {
    const id = beginOperation('tools/call', { session: 'closing' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { privateConnections: 1 });
    finishOperation(id, 'ok');
    expect(getOperationSnapshot().summary.active).toBe(1);
    updateOperation(id, { privateConnections: 0 });
    expect(getOperationSnapshot().summary.active).toBe(0);
    expect(getOperationSnapshot().summary.finished).toBe(1);
  });

  it('keeps unfinished work age advancing after caller latency stops', () => {
    const clock = vi.spyOn(performance, 'now').mockReturnValue(100);
    try {
      const id = beginOperation('tools/call');
      if (!id) throw new Error('journal unexpectedly refused available test capacity');
      updateOperation(id, { privateConnections: 1 });
      clock.mockReturnValue(200);
      finishOperation(id, 'ok');
      clock.mockReturnValue(500);
      expect(getOperation(id)).toMatchObject({ elapsedMs: 100, ageMs: 400 });
      clock.mockReturnValue(700);
      updateOperation(id, { privateConnections: 0 });
      clock.mockReturnValue(900);
      expect(getOperation(id)).toMatchObject({ elapsedMs: 100, ageMs: 600 });
    } finally {
      clock.mockRestore();
    }
  });

  it('applies the same backend-reference boundary to later route observations', () => {
    const id = beginOperation('tools/call', { backend: 'configured-backend' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    updateOperation(id, { backend: 'diagnostic/private-reference' });
    expect(getOperation(id)?.backend).toBeNull();
    expect(JSON.stringify(getOperationSnapshot())).not.toContain('diagnostic/private-reference');
    finishOperation(id, 'failed');
  });

  it('emits only allowlisted scalar lifecycle facts', () => {
    const events: unknown[] = [];
    const unsubscribe = subscribeOperationEvents((value) => events.push(value));
    const id = beginOperation('completion/complete', { session: 'session-token', backend: 'safe-ref' });
    if (!id) throw new Error('journal unexpectedly refused available test capacity');
    finishOperation(id, 'ok');
    unsubscribe();
    expect(JSON.stringify(events)).not.toContain('session-token');
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({ schemaVersion: 1, kind: 'started', method: 'completion/complete' });
  });
});
