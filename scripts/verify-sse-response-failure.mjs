#!/usr/bin/env node
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, writeFile } from 'node:fs/promises';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { createRequire } from 'node:module';

const entry = resolve(process.argv[2] ?? 'build/index.js');
const require = createRequire(join(dirname(dirname(entry)), 'package.json'));
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');
const directory = await mkdtemp(join(tmpdir(), 'gateway-sse-failure-'));
const counts = { abort: 0, hold: 0, ok: 0 };
const pending = new Map();
const peer = http.createServer(async (req, res) => {
  if (req.method !== 'POST') {
    res.writeHead(405).end();
    return;
  }
  let raw = '';
  for await (const chunk of req) raw += chunk;
  const message = JSON.parse(raw);
  if (message.id === undefined) {
    res.writeHead(202).end();
    return;
  }
  let result;
  if (message.method === 'initialize') {
    result = {
      protocolVersion: '2025-06-18',
      capabilities: { tools: {} },
      serverInfo: { name: 'socket-fault-fixture', version: '1' },
    };
  } else if (message.method === 'tools/list') {
    result = {
      tools: [
        {
          name: 'fixture',
          description: 'Synthetic controlled side-effect witness',
          inputSchema: { type: 'object', properties: { mode: { type: 'string' } }, required: ['mode'] },
          annotations: { readOnlyHint: false, idempotentHint: false },
        },
      ],
    };
  } else if (message.method === 'tools/call') {
    const mode = message.params.arguments.mode;
    counts[mode]++;
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(': admitted\n\n');
    if (mode !== 'ok') {
      pending.set(mode, { res, id: message.id });
      return;
    }
    res.end(
      `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: message.id, result: { content: [{ type: 'text', text: 'ok' }] } })}\n\n`,
    );
    return;
  } else {
    res
      .writeHead(200, { 'content-type': 'application/json' })
      .end(JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32601, message: 'Method not found' } }));
    return;
  }
  res
    .writeHead(200, { 'content-type': 'application/json' })
    .end(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }));
});
await new Promise((done) => peer.listen(0, '127.0.0.1', done));
const reserve = http.createServer();
await new Promise((done) => reserve.listen(0, '127.0.0.1', done));
const port = reserve.address().port;
await new Promise((done) => reserve.close(done));
await writeFile(
  join(directory, 'mcp.json'),
  JSON.stringify({
    mcpServers: {
      fixture: {
        type: 'http',
        url: `http://127.0.0.1:${peer.address().port}/mcp`,
        connectionTimeout: 5000,
        requestTimeout: 4000,
      },
    },
  }),
);
const env = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !key.startsWith('ONE_MCP_') && !key.startsWith('MCP_GATEWAY_')),
);
const child = spawn(
  process.execPath,
  [
    entry,
    'serve',
    '--config',
    join(directory, 'mcp.json'),
    '--config-dir',
    join(directory, 'state'),
    '--transport',
    'http',
    '--host',
    '127.0.0.1',
    '--port',
    String(port),
    '--enable-auth=false',
    '--enable-internal-tools=false',
    '--enable-async-loading',
    '--log-level=warn',
  ],
  { cwd: directory, env, stdio: ['ignore', 'pipe', 'pipe'] },
);
let log = '';
child.stdout.on('data', (data) => {
  log += data;
});
child.stderr.on('data', (data) => {
  log += data;
});
const client = new Client({ name: 'real-socket-regression', version: '1' });
let broken;
let held;
async function until(check, label, budget = 15000) {
  const deadline = Date.now() + budget;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error('Gateway exited');
    if (await check()) return;
    await delay(20);
  }
  throw new Error(`Timed out: ${label}`);
}
function releaseHeld() {
  const held = pending.get('hold');
  if (held && !held.res.writableEnded)
    held.res.end(
      `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: held.id, result: { content: [{ type: 'text', text: 'held-completed' }] } })}\n\n`,
    );
}
try {
  await until(async () => {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health/mcp`, { signal: AbortSignal.timeout(500) });
      return response.ok && (await response.json()).summary.ready === 1;
    } catch {
      return false;
    }
  }, 'gateway backend readiness');
  await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`)));
  await client.listTools();
  const call = (mode) =>
    client.callTool({ name: 'fixture_1mcp_fixture', arguments: { mode } }, undefined, { timeout: 6000 });
  let siblingSettled = false;
  held = call('hold').finally(() => {
    siblingSettled = true;
  });
  const heldOutcome = held.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  broken = call('abort').then(
    (value) => (value.isError ? 'rejected' : 'unexpected_success'),
    () => 'rejected',
  );
  await until(() => pending.has('hold') && pending.has('abort'), 'both upstream calls admitted');
  pending.get('abort').res.destroy();
  let timer;
  const deadline = new Promise((done) => {
    timer = setTimeout(() => done('request_still_pending'), 750);
  });
  const failureOutcome = await Promise.race([broken, deadline]);
  clearTimeout(timer);
  assert.equal(failureOutcome, 'rejected', 'cut-off request must fail before its 4000ms timeout');
  assert.equal(siblingSettled, false, 'request failure must not close a healthy sibling');
  assert.equal(counts.abort, 1, 'accepted side effect must never replay');
  assert.equal((await call('ok')).isError, undefined);
  releaseHeld();
  const heldResult = await heldOutcome;
  assert(!heldResult.error && !heldResult.value.isError);
  assert.equal((await call('ok')).isError, undefined);
  await delay(100);
  assert.equal(counts.abort, 1);
  console.log(
    JSON.stringify({
      ok: true,
      entry,
      promptlyRejected: true,
      siblingPreserved: true,
      subsequentReadsPassed: true,
      noReplay: true,
      counts,
      evidence: directory,
    }),
  );
} catch (error) {
  console.error(JSON.stringify({ ok: false, entry, error: error.message, counts, evidence: directory }));
  process.exitCode = 1;
} finally {
  releaseHeld();
  await client.close().catch(() => undefined);
  await Promise.allSettled([broken, held]);
  if (child.exitCode === null && child.signalCode === null) {
    const exited = once(child, 'exit');
    child.kill('SIGTERM');
    const kill = setTimeout(() => child.kill('SIGKILL'), 5000);
    await exited;
    clearTimeout(kill);
  }
  assert.throws(() => process.kill(child.pid, 0), /ESRCH/);
  peer.closeAllConnections();
  await new Promise((done) => peer.close(done));
  await writeFile(join(directory, 'gateway.log'), log);
}
