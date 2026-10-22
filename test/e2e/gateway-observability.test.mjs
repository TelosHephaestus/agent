import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { ListRootsRequestSchema } from '@modelcontextprotocol/sdk/types.js';

import test from 'node:test';

import { startObservabilityBackend } from './fixtures/gateway-observability-backend.mjs';

const entry = process.env.GATEWAY_ENTRY ?? fileURLToPath(new URL('../../build/index.js', import.meta.url));
const scratch = process.env.TMPDIR ?? process.env.RUNNER_TEMP ?? tmpdir();
const operatorEntry =
  process.env.OPERATOR_ENTRY ?? fileURLToPath(new URL('../../scripts/gateway-observability.mjs', import.meta.url));
const execute = promisify(execFile);
const deadline = (promise, ms = 5000, label = 'operation') => {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} deadline`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
};
const resultText = (value) => {
  assert.equal(value.isError, undefined);
  return value.content.find((item) => item.type === 'text').text;
};

async function startGateway(t, { enableAuth = false, sinkOrigin } = {}) {
  assert.ok(entry, 'GATEWAY_ENTRY must name the compiled gateway entry');
  assert.ok(scratch, 'The temporary directory must not be empty');
  const backend = await startObservabilityBackend();
  const dir = await mkdtemp(join(scratch, 'gateway-observability-'));
  const home = join(dir, 'home');
  const configDir = join(dir, 'config');
  await mkdir(home);
  await mkdir(configDir);
  const configPath = join(configDir, 'mcp.json');
  await writeFile(configPath, JSON.stringify({ mcpServers: { fixture: { type: 'http', url: backend.url } } }));
  const reservation = createServer();
  await new Promise((resolveListen, reject) => {
    reservation.once('error', reject);
    reservation.listen(0, '127.0.0.1', resolveListen);
  });
  const port = reservation.address().port;
  await new Promise((resolveClose, reject) => reservation.close((error) => (error ? reject(error) : resolveClose())));

  const gateway = spawn(
    process.execPath,
    [
      entry,
      'serve',
      '--config',
      configPath,
      '--config-dir',
      configDir,
      '--transport',
      'http',
      '--host',
      '127.0.0.1',
      '--port',
      String(port),
      `--enable-auth=${enableAuth}`,
      '--enable-internal-tools=false',
      '--enable-config-reload=false',
      '--enable-session-persistence=false',
      '--enable-async-loading',
      '--async-background-retry',
      '--async-background-retry-interval=1000',
      '--log-level=warn',
    ],
    {
      cwd: dir,
      env: {
        PATH: process.env.PATH,
        HOME: home,
        TMPDIR: scratch,
        XDG_CONFIG_HOME: join(home, '.config'),
        XDG_STATE_HOME: join(home, '.local/state'),
        XDG_DATA_HOME: join(home, '.local/share'),
        XDG_CACHE_HOME: join(home, '.cache'),
        ...(sinkOrigin
          ? {
              MCP_GATEWAY_OBSERVABILITY_OMPHALOS_URL: sinkOrigin,
              MCP_GATEWAY_OBSERVABILITY_SERVICE: 'gateway-observability-e2e',
              MCP_GATEWAY_OBSERVABILITY_TIER: 'staging',
            }
          : {}),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let logs = '';
  gateway.stdout.on('data', (chunk) => {
    logs += chunk;
  });
  gateway.stderr.on('data', (chunk) => {
    logs += chunk;
  });
  const url = `http://127.0.0.1:${port}`;
  const clients = [];
  const progressTokens = [];
  const client = async (roots) => {
    const instance = new Client(
      { name: 'gateway-observability-acceptance', version: '1' },
      { capabilities: roots ? { roots: {} } : {} },
    );
    if (roots) instance.setRequestHandler(ListRootsRequestSchema, roots);
    const transport = new StreamableHTTPClientTransport(new URL(`${url}/mcp`), {
      fetch: async (target, options) => {
        if (options?.method === 'POST' && typeof options.body === 'string') {
          const message = JSON.parse(options.body);
          if (message.method === 'tools/call') progressTokens.push(message.params._meta?.progressToken);
        }
        return fetch(target, options);
      },
    });
    clients.push(instance);
    await instance.connect(transport, { timeout: 5000 });
    await instance.listTools({}, { timeout: 5000 });
    return instance;
  };
  const stop = async () => {
    for (const latch of backend.holds.values()) latch.release.resolve();
    for (const instance of clients) await instance.close().catch(() => undefined);
    if (gateway.exitCode === null && gateway.signalCode === null) {
      gateway.kill('SIGTERM');
      await deadline(once(gateway, 'exit'), 5000, 'gateway stop').catch(async () => {
        gateway.kill('SIGKILL');
        await once(gateway, 'exit');
      });
    }
    await backend.close();
  };
  t.after(stop);
  const ready = await deadline(
    (async () => {
      while (gateway.exitCode === null) {
        try {
          const response = await fetch(`${url}/health/mcp`, { signal: AbortSignal.timeout(500) });
          if (response.ok && (await response.json()).summary.ready === 1) return true;
        } catch {}
        await new Promise((resolveWait) => setTimeout(resolveWait, 40));
      }
      throw new Error(
        `compiled gateway exited before readiness: ${logs.slice(0, 3000)}${logs.length > 6000 ? '…' : ''}${logs.slice(-3000)}`,
      );
    })(),
    20000,
    'gateway readiness',
  );
  assert.equal(ready, true);
  return {
    backend,
    client,
    clients,
    progressTokens,
    url,
    dir,
    call: (instance, name, label, options = {}) =>
      instance.callTool({ name: `fixture_1mcp_${name}`, arguments: { label } }, undefined, {
        timeout: 10000,
        onprogress: () => {},
        ...options,
      }),
  };
}

async function observations(url, query = '?active=true&limit=50') {
  return fetch(`${url}/api/v1/observability${query}`, { signal: AbortSignal.timeout(3000) });
}

test('compiled gateway exposes bounded read-only operation observability', { timeout: 120000 }, async (t) => {
  const h = await startGateway(t);
  const initialMarkers = { ...h.backend.markers };
  const firstRead = await observations(h.url);
  assert.equal(firstRead.status, 200, 'observability route must exist on the compiled gateway');
  const initial = await firstRead.json();
  assert.equal(initial.schemaVersion, 1);
  assert.equal(initial.operations.length, 0);
  assert.deepEqual(h.backend.markers, initialMarkers, 'read-only query must not contact MCP backend');
  assert.equal((await observations(h.url, '?active=true&limit=0')).status, 400);
  assert.equal((await observations(h.url, '?active=maybe')).status, 400);
  assert.equal((await observations(h.url, '?limit=not-an-integer')).status, 400);
  assert.equal((await fetch(`${h.url}/api/v1/observability/operations/not-a-known-operation`)).status, 404);

  const caller = await h.client();
  const callsBeforeRefusal = h.backend.markers.calls;
  await assert.rejects(
    caller.callTool({ name: 'fixture_1mcp_missing_tool', arguments: { label: 'refusal-secret-label' } }),
    (error) => error.code === -32603,
  );
  assert.equal(h.backend.markers.calls, callsBeforeRefusal, 'invalid tool refusal must not dispatch to MCP backend');
  const held = h.backend.hold('active-secret-label');
  const inFlight = h.call(caller, 'hold', 'active-secret-label').then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  try {
    await deadline(held.entered.promise, 5000, 'backend call entered');
    const beforeSnapshot = { ...h.backend.markers };
    const response = await observations(h.url);
    assert.equal(response.status, 200);
    const snapshot = await response.json();
    assert.equal(snapshot.schemaVersion, 1);
    assert.equal(snapshot.operations.length, 1);
    assert.equal(snapshot.operations[0].backendRequest, 'in_flight');
    assert.equal(JSON.stringify(snapshot).includes(h.backend.secret), false);
    assert.equal(JSON.stringify(snapshot).includes('active-secret-label'), false);
    assert.deepEqual(h.backend.markers, beforeSnapshot, 'operation query must not contact the backend');
    assert.equal(h.backend.markers.calls, callsBeforeRefusal + 1);
    const detail = await fetch(
      `${h.url}/api/v1/observability/operations/${encodeURIComponent(snapshot.operations[0].id)}`,
    );
    assert.equal(detail.status, 200);
    const detailView = await detail.json();
    assert.equal(detailView.operation.id, snapshot.operations[0].id);
    assert.equal(detailView.schemaVersion, 1);
    assert.equal(detailView.instanceId, snapshot.instanceId);
    const credentialFile = join(h.dir, 'operator-credential.json');
    const markerToken = 'CLI_ACCEPTANCE_TOKEN_NOT_REAL';
    await writeFile(credentialFile, JSON.stringify({ token: markerToken }), { mode: 0o600 });
    const cli = await execute(
      process.execPath,
      [operatorEntry, '--url', h.url, '--credential-file', credentialFile, '--active', '--json'],
      { timeout: 15000 },
    );
    const cliSnapshot = JSON.parse(cli.stdout);
    assert.equal(cliSnapshot.instanceId, snapshot.instanceId);
    assert.equal(cliSnapshot.backendHealth.available, true);
    assert.equal(cliSnapshot.operations[0].id, snapshot.operations[0].id);
    assert(!cli.stdout.includes(markerToken));
    const exact = await execute(
      process.execPath,
      [operatorEntry, '--url', h.url, '--operation', snapshot.operations[0].id, '--json'],
      { timeout: 15000 },
    );
    assert.equal(JSON.parse(exact.stdout).operation.id, snapshot.operations[0].id);
  } finally {
    held.release.resolve();
  }
  const completed = await inFlight;
  assert.ifError(completed.error);
  assert.match(resultText(completed.value), /^OBSERVABILITY_SENTINEL_/);

  const failure = await h.call(caller, 'fail', 'failure-secret-label');
  assert.equal(failure.isError, true);
  const after = await (await observations(h.url, '?limit=50')).json();
  assert.ok(after.operations.some((operation) => operation.callerOutcome === 'tool_error'));
  assert.equal(JSON.stringify(after).includes(h.backend.secret), false);
  assert.equal(JSON.stringify(after).includes('failure-secret-label'), false);
  const beforeQuery = { ...h.backend.markers };
  await observations(h.url);
  assert.deepEqual(h.backend.markers, beforeQuery);
});

test(
  'operation snapshot keeps concurrent calls distinct and cancellation is not a backend terminal fact',
  { timeout: 60000 },
  async (t) => {
    const h = await startGateway(t);
    assert.equal((await observations(h.url)).status, 200);
    const a = await h.client();
    const b = await h.client();
    const firstLatch = h.backend.hold('collision-A');
    const secondLatch = h.backend.hold('collision-B');
    const abort = new AbortController();
    const one = h.call(a, 'hold', 'collision-A', { signal: abort.signal }).then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    const two = h.call(b, 'hold', 'collision-B').then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    try {
      await deadline(
        Promise.all([firstLatch.entered.promise, secondLatch.entered.promise]),
        5000,
        'two backend calls entered',
      );
      assert.equal(h.progressTokens.length, 2);
      assert.equal(h.progressTokens[0], h.progressTokens[1], 'separate clients must produce colliding progress tokens');
      let snapshot = await (await observations(h.url)).json();
      assert.equal(snapshot.operations.length, 2);
      assert.equal(new Set(snapshot.operations.map((operation) => operation.id)).size, 2);
      abort.abort();
      const callerResult = await deadline(one, 5000, 'caller cancellation');
      assert.ok(callerResult.error);
      snapshot = await (await observations(h.url)).json();
      const cancelled = snapshot.operations.find((operation) => operation.callerOutcome === 'cancelled');
      assert.ok(cancelled);
      assert.notEqual(
        cancelled.backendRequest,
        'terminal_response_observed',
        'caller abort does not prove MCP terminal response',
      );
      assert.match(resultText(await h.call(a, 'read', 'independent-after-cancel')), /:independent-after-cancel$/);
    } finally {
      firstLatch.release.resolve();
      secondLatch.release.resolve();
    }
    const survivor = await two;
    assert.ifError(survivor.error);
  },
);

test(
  'legacy callback wait and backend session loss retain safe evidence without callback payloads or replay',
  { timeout: 60000 },
  async (t) => {
    const h = await startGateway(t);
    assert.equal((await observations(h.url)).status, 200);
    let callbackEnteredResolve;
    const callbackEntered = new Promise((resolvePromise) => {
      callbackEnteredResolve = resolvePromise;
    });
    let releaseCallback;
    const callbackRelease = new Promise((resolvePromise) => {
      releaseCallback = resolvePromise;
    });
    const caller = await h.client(async () => {
      callbackEnteredResolve();
      await callbackRelease;
      return { roots: [{ uri: 'file:///isolated-test-root' }] };
    });
    const pending = h.call(caller, 'reverse', 'callback-held').then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    try {
      await deadline(callbackEntered, 5000, 'caller callback entered');
      const view = await (await observations(h.url)).json();
      assert.ok(view.operations.length >= 1);
      const callbackOperation = view.operations.find((operation) => operation.outstandingCallbacks > 0);
      assert.ok(callbackOperation, 'the real callback owner must remain visible while the caller waits');
      assert.equal(callbackOperation.privateConnections, 1);
      assert.equal(JSON.stringify(view).includes('isolated-test-root'), false);
    } finally {
      releaseCallback();
    }
    const completed = await pending;
    assert.ifError(completed.error);
    assert.deepEqual(JSON.parse(resultText(completed.value)), { roots: [{ uri: 'file:///isolated-test-root' }] });
    assert.equal(h.backend.markers.callbacks, 1);

    const mutation = await h.call(caller, 'mutate', 'accepted-once');
    assert.equal(h.backend.mutationCount, 1);
    assert.equal(resultText(mutation), 'mutation accepted');
    await h.backend.loseSessions();
    assert.equal((await observations(h.url)).status, 200);
    const snapshot = await (await observations(h.url)).json();
    assert.equal(JSON.stringify(snapshot).includes(h.backend.secret), false);
    assert.equal(h.backend.calls.filter((call) => call.label === 'accepted-once').length, 1);
  },
);

test('observation routes retain the existing HTTP authentication boundary', { timeout: 30000 }, async (t) => {
  const h = await startGateway(t, { enableAuth: true });
  assert.equal((await observations(h.url)).status, 401);
  assert.equal((await fetch(`${h.url}/api/v1/observability/operations/unknown`)).status, 401);
});

test('invalid exporter configuration keeps compiled gateway startup fail-open', { timeout: 30000 }, async (t) => {
  const h = await startGateway(t, { sinkOrigin: 'not-a-valid-origin' });
  const caller = await h.client();
  const result = await h.call(caller, 'read', 'invalid-export-config');
  assert.equal(resultText(result), `${h.backend.secret}:invalid-export-config`);
  assert.equal(h.backend.markers.calls, 1, 'the business call must dispatch exactly once');
  const view = await (await observations(h.url)).json();
  assert.equal(view.export.enabled, false);
  assert.equal(view.export.lastFailure, 'invalid_config');
});

test(
  'sink timeout and queue overflow do not change compiled gateway tool results or dispatches',
  { timeout: 90000 },
  async (t) => {
    let firstRequestEnteredResolve;
    const firstRequestEntered = new Promise((resolvePromise) => {
      firstRequestEnteredResolve = resolvePromise;
    });
    let firstSocketClosedResolve;
    const firstSocketClosed = new Promise((resolvePromise) => {
      firstSocketClosedResolve = resolvePromise;
    });
    let secondRequestEnteredResolve;
    const secondRequestEntered = new Promise((resolvePromise) => {
      secondRequestEnteredResolve = resolvePromise;
    });
    let secondResponse;
    const batches = [];
    let firstSocket;
    let collectorRequests = 0;
    const collector = createServer(async (request, response) => {
      let raw = '';
      for await (const chunk of request) raw += chunk;
      const body = JSON.parse(raw);
      batches.push(body);
      collectorRequests++;
      if (collectorRequests === 1) {
        firstSocket = request.socket;
        firstSocket.once('close', firstSocketClosedResolve);
        firstRequestEnteredResolve();
        return;
      }
      if (collectorRequests === 2) {
        secondResponse = response;
        secondRequestEnteredResolve();
        return;
      }
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ status: 'ok', data: { status: 'accepted', count: body.payload.length } }));
    });
    await new Promise((resolveListen, reject) => {
      collector.once('error', reject);
      collector.listen(0, '127.0.0.1', resolveListen);
    });
    const sinkOrigin = `http://127.0.0.1:${collector.address().port}`;
    t.after(async () => {
      collector.closeAllConnections();
      await new Promise((resolveClose) => collector.close(resolveClose));
    });

    const h = await startGateway(t, { sinkOrigin });
    const caller = await h.client();
    const heldLabel = 'SINK_ARGUMENT_SENTINEL:SINK_PROGRESS_SENTINEL';
    const held = h.backend.hold(heldLabel);
    const heldCall = h.call(caller, 'hold', heldLabel).then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    await deadline(held.entered.promise, 5000, 'sentinel tool entered backend');
    await deadline(firstRequestEntered, 8000, 'collector received first export batch');

    const count = 120;
    const concurrency = 16;
    const results = new Array(count);
    let next = 0;
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        for (;;) {
          const index = next++;
          if (index >= count) return;
          const label = `sink-overflow-${index}`;
          results[index] = await h.call(caller, 'read', label);
        }
      }),
    );
    assert.equal(results.length, count);
    for (let index = 0; index < count; index++) {
      assert.equal(resultText(results[index]), `${h.backend.secret}:sink-overflow-${index}`);
    }
    assert.equal(h.backend.markers.calls, count + 1, 'every accepted business call must dispatch exactly once');
    assert.equal(h.backend.calls.filter((call) => call.label === heldLabel).length, 1);
    assert.equal(h.backend.calls.filter((call) => call.label.startsWith('sink-overflow-')).length, count);
    const duringStall = await (await observations(h.url)).json();
    assert.equal(duringStall.export.enabled, true);
    assert.equal(duringStall.export.inFlight, true, 'collector response remains held during business calls');
    assert.ok(duringStall.export.dropped > 0, 'overflow or in-flight telemetry must be dropped');
    assert.equal(duringStall.export.queued, 256, 'the fixed queue must fill before excess evidence drops');
    const serializedBatch = JSON.stringify(batches[0]);
    for (const marker of [heldLabel, h.backend.secret, 'sink-overflow-0', 'SINK_PROGRESS_SENTINEL']) {
      assert.equal(serializedBatch.includes(marker), false, `export request must exclude ${marker}`);
    }

    held.release.resolve();
    const heldResult = await deadline(heldCall, 5000, 'held business tool completion');
    assert.ifError(heldResult.error);
    assert.equal(resultText(heldResult.value), `${h.backend.secret}:${heldLabel}`);
    await deadline(firstSocketClosed, 8000, 'collector connection closed after native export deadline');
    await deadline(secondRequestEntered, 8000, 'subsequent export batch after failed batch');
    const timedOut = await deadline(
      (async () => {
        for (;;) {
          const view = await (await observations(h.url)).json();
          if (view.export.lastFailure === 'timeout') return view.export;
          await new Promise((resolveWait) => setTimeout(resolveWait, 25));
        }
      })(),
      3000,
      'export timeout status',
    );
    assert.equal(timedOut.lastFailure, 'timeout');
    assert.ok(timedOut.dropped > 0);
    assert.ok(firstSocket.destroyed, 'timed-out collector socket must be closed');
    secondResponse.writeHead(200, { 'content-type': 'application/json' });
    secondResponse.end(
      JSON.stringify({ status: 'ok', data: { status: 'accepted', count: batches[1].payload.length } }),
    );

    await deadline(
      (async () => {
        for (;;) {
          const view = await (await observations(h.url)).json();
          if (view.export.acceptedBatches > 0) return;
          await new Promise((resolveWait) => setTimeout(resolveWait, 25));
        }
      })(),
      3000,
      'subsequent batch accepted',
    );
    const first = batches[0];
    const later = batches.slice(1);
    assert.ok(later.length >= 1);
    assert.ok(new Set([first.request_id, ...later.map((batch) => batch.request_id)]).size === later.length + 1);
    const eventIdentity = (event) => JSON.stringify(event);
    const failedEvents = new Set(first.payload.map((item) => eventIdentity(JSON.parse(item.message))));
    assert.equal(failedEvents.size, first.payload.length, 'failed batch events must have unique identities');
    for (const batch of later) {
      const laterEvents = batch.payload.map((item) => eventIdentity(JSON.parse(item.message)));
      assert.equal(new Set(laterEvents).size, laterEvents.length, 'later batch events must have unique identities');
      for (const identity of laterEvents) {
        assert.equal(failedEvents.has(identity), false, 'failed event must not be replayed');
      }
    }
    assert.equal(h.backend.markers.calls, count + 1, 'telemetry failure must not retry or suppress business dispatch');
  },
);
