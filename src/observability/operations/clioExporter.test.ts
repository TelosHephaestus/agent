import { randomUUID } from 'node:crypto';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import * as journal from './operationJournal.js';
import { getOperationExportStatus, startClioOperationExporter, stopClioOperationExporter } from './clioExporter.js';

const env = {
  ONE_MCP_OBSERVABILITY_OMPHALOS_URL: 'http://127.0.0.1:3000',
  ONE_MCP_OBSERVABILITY_SERVICE: 'unit-test',
  ONE_MCP_OBSERVABILITY_TIER: 'staging',
};
const emit = () => journal.finishOperation(journal.beginOperation('tools/call'), 'ok');
const accepted = (count: number) => new Response(JSON.stringify({ status: 'ok', data: { status: 'accepted', count } }));

beforeEach(() => vi.useFakeTimers());
afterEach(async () => {
  stopClioOperationExporter();
  await vi.advanceTimersByTimeAsync(0);
  journal.resetOperationJournalForTests();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('Clio operation exporter', () => {
  it('exports real journal events with the source-defined Omphalos data envelope', async () => {
    const before = getOperationExportStatus().acceptedBatches;
    const fetch = vi.fn(async (_url: string, init: RequestInit) => {
      const body = JSON.parse(String(init.body));
      expect(body).toMatchObject({ capability: 'ingest_logs', tier: 'staging', caller: 'unit-test' });
      expect(body.payload).toHaveLength(2);
      expect(JSON.parse(body.payload[0].message)).toMatchObject({
        schemaVersion: 1,
        kind: 'started',
        method: 'tools/call',
      });
      expect(body.payload[0].message).not.toContain('secret payload');
      expect(init.redirect).toBe('error');
      return accepted(body.payload.length);
    });
    vi.stubGlobal('fetch', fetch);
    startClioOperationExporter(env);
    emit();
    await vi.advanceTimersByTimeAsync(1000);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(getOperationExportStatus()).toMatchObject({ acceptedBatches: before + 1, queued: 0, inFlight: false });
  });

  it('stays disabled on incomplete, wrong-tier or non-origin configuration', () => {
    for (const invalid of [
      { ...env, ONE_MCP_OBSERVABILITY_TIER: 'wrong' },
      { ...env, ONE_MCP_OBSERVABILITY_OMPHALOS_URL: 'https://example.invalid/ignored-path' },
      { ONE_MCP_OBSERVABILITY_TIER: 'staging' },
    ]) {
      startClioOperationExporter(invalid);
      expect(getOperationExportStatus()).toMatchObject({ enabled: false, queued: 0, lastFailure: 'invalid_config' });
    }
  });

  it.each(['http', 'owner_error', 'partial_batch', 'wrong_envelope'] as const)(
    'drops %s without retry',
    async (failure) => {
      const before = getOperationExportStatus();
      const fetch = vi.fn(async () => {
        if (failure === 'http') return new Response('{}', { status: 503 });
        if (failure === 'owner_error') return new Response(JSON.stringify({ status: 'error', error: {} }));
        if (failure === 'partial_batch') return accepted(1);
        return new Response(JSON.stringify({ status: 'ok', result: { status: 'accepted', count: 2 } }));
      });
      vi.stubGlobal('fetch', fetch);
      startClioOperationExporter(env);
      emit();
      await vi.advanceTimersByTimeAsync(5000);
      expect(getOperationExportStatus()).toMatchObject({
        dropped: before.dropped + 2,
        acceptedBatches: before.acceptedBatches,
        lastFailure: failure === 'http' ? 'http' : 'response',
      });
      expect(fetch).toHaveBeenCalledTimes(1);
    },
  );

  it('keeps the queue bounded without restricting observed operations', () => {
    const before = getOperationExportStatus().dropped;
    startClioOperationExporter(env);
    for (let index = 0; index < 140; index += 1) emit();
    expect(getOperationExportStatus()).toMatchObject({ queued: 256, dropped: before + 280 - 256 });
    expect(journal.getOperationSnapshot().summary.finished).toBe(140);
  });

  it('cancels an oversized response before parsing it', async () => {
    const cancel = vi.fn();
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(
            new ReadableStream({
              start(controller) {
                controller.enqueue(new Uint8Array(4097));
              },
              cancel,
            }),
          ),
      ),
    );
    startClioOperationExporter(env);
    emit();
    await vi.advanceTimersByTimeAsync(1000);
    expect(getOperationExportStatus().lastFailure).toBe('response');
    expect(cancel).toHaveBeenCalledOnce();
  });

  it('bounds an unresponsive request by its deadline and never replays the batch', async () => {
    const before = getOperationExportStatus().dropped;
    const fetch = vi.fn(
      (_url: string, init: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init.signal?.addEventListener('abort', () => reject(new Error('test abort')), { once: true });
        }),
    );
    vi.stubGlobal('fetch', fetch);
    startClioOperationExporter(env);
    emit();
    await vi.advanceTimersByTimeAsync(7000);
    expect(getOperationExportStatus()).toMatchObject({ dropped: before + 2, lastFailure: 'timeout', inFlight: false });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('aborts active export and accounts for queued work once when stopped', async () => {
    const before = getOperationExportStatus().dropped;
    let signal: AbortSignal | undefined;
    vi.stubGlobal(
      'fetch',
      vi.fn(
        (_url: string, init: RequestInit) =>
          new Promise<Response>((_resolve, reject) => {
            signal = init.signal ?? undefined;
            signal?.addEventListener('abort', () => reject(new Error('test abort')), { once: true });
          }),
      ),
    );
    startClioOperationExporter(env);
    emit();
    await vi.advanceTimersByTimeAsync(1000);
    emit();
    stopClioOperationExporter();
    expect(signal?.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(getOperationExportStatus()).toMatchObject({
      enabled: false,
      inFlight: false,
      queued: 0,
      dropped: before + 4,
    });
  });

  it('does not invoke event accessors or serialize extra properties', async () => {
    let listener: (event: journal.OperationEvent) => void = () => {
      throw new Error('subscriber missing');
    };
    vi.spyOn(journal, 'subscribeOperationEvents').mockImplementation((callback) => {
      listener = callback;
      return () => {};
    });
    const event: journal.OperationEvent = {
      schemaVersion: 1,
      instanceId: randomUUID(),
      operationId: randomUUID(),
      sessionId: 'abc123',
      kind: 'started',
      at: new Date().toISOString(),
      method: 'tools/call',
      profile: 'legacy',
      backend: null,
      elapsedMs: 0,
    };
    const getter = vi.fn(() => 'private accessor value');
    const invalid = { ...event };
    Object.defineProperty(invalid, 'method', { get: getter });
    const before = getOperationExportStatus().dropped;
    const fetch = vi.fn(async (_url: string, init: RequestInit) => {
      expect(String(init.body)).not.toContain('private extra value');
      return accepted(1);
    });
    vi.stubGlobal('fetch', fetch);
    startClioOperationExporter(env);
    listener(invalid);
    listener(Object.assign(event, { arguments: 'private extra value', toJSON: getter }));
    await vi.advanceTimersByTimeAsync(1000);
    expect(getter).not.toHaveBeenCalled();
    expect(getOperationExportStatus().dropped).toBe(before + 1);
    expect(fetch).toHaveBeenCalledOnce();
  });
});
