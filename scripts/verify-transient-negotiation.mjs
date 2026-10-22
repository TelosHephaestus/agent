#!/usr/bin/env node
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile } from 'node:fs/promises';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

// An isolated real-HTTP startup outage. Never contacts household services.
const entry = resolve(process.argv[2] ?? 'build/index.js');
const root = await mkdtemp(join(tmpdir(), 'gateway-negotiation-recovery-'));
let available = false;
let child;
let log = '';
let session;
let sequence = 0;
const counts = { unavailable: 0, denied: 0, flakyReads: 0, healthyReads: 0 };
const peer = http.createServer(async (req, res) => {
  let raw = '';
  for await (const chunk of req) raw += chunk;
  if (req.url === '/denied') {
    counts.denied++;
    res.writeHead(403).end('Forbidden');
    return;
  }
  if (req.url === '/flaky' && !available) {
    counts.unavailable++;
    res.writeHead(502).end('Bad Gateway');
    return;
  }
  if (req.method !== 'POST') {
    res.writeHead(405).end();
    return;
  }
  const message = JSON.parse(raw || '{}');
  if (message.id === undefined) {
    res.writeHead(202).end();
    return;
  }
  let result;
  switch (message.method) {
    case 'initialize':
      result = {
        protocolVersion: '2025-06-18',
        capabilities: { tools: {} },
        serverInfo: { name: 'read-fixture', version: '1' },
      };
      break;
    case 'tools/list':
      result = {
        tools: [
          {
            name: 'read_fixture',
            description: 'Read a synthetic witness',
            inputSchema: { type: 'object', properties: {} },
            annotations: { readOnlyHint: true },
          },
        ],
      };
      break;
    case 'tools/call':
      if (req.url === '/flaky') counts.flakyReads++;
      else counts.healthyReads++;
      result = { content: [{ type: 'text', text: JSON.stringify({ witness: req.url.slice(1) }) }] };
      break;
    case 'ping':
      result = {};
      break;
    default:
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
const reservation = http.createServer();
await new Promise((done) => reservation.listen(0, '127.0.0.1', done));
const port = reservation.address().port;
await new Promise((done) => reservation.close(done));
const url = `http://127.0.0.1:${port}`;
const peerUrl = `http://127.0.0.1:${peer.address().port}`;
await writeFile(
  join(root, 'mcp.json'),
  JSON.stringify({
    mcpServers: Object.fromEntries(
      ['healthy', 'flaky', 'denied'].map((name) => [
        name,
        { type: 'http', url: `${peerUrl}/${name}`, connectionTimeout: 3000, requestTimeout: 3000 },
      ]),
    ),
  }),
);

async function until(check, label, budget = 15000) {
  const deadline = Date.now() + budget;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`Gateway exited: ${child.exitCode}`);
    if (await check()) return;
    await delay(100);
  }
  throw new Error(`Timed out: ${label}`);
}
async function health() {
  try {
    const response = await fetch(`${url}/health/mcp`, { signal: AbortSignal.timeout(1500) });
    return response.ok ? await response.json() : undefined;
  } catch {
    return undefined;
  }
}
async function rpc(method, params = {}, notification = false) {
  const id = ++sequence;
  const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream' };
  if (session) headers['mcp-session-id'] = session;
  const response = await fetch(`${url}/mcp`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ jsonrpc: '2.0', ...(notification ? {} : { id }), method, params }),
    signal: AbortSignal.timeout(5000),
  });
  session = response.headers.get('mcp-session-id') ?? session;
  assert(response.ok, `RPC HTTP ${response.status}`);
  if (notification) {
    await response.body?.cancel();
    return;
  }
  const reader = response.body.getReader();
  let raw = '';
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      raw += Buffer.from(chunk.value).toString('utf8');
      if (response.headers.get('content-type')?.includes('application/json')) continue;
      for (const line of raw.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const message = JSON.parse(line.slice(5));
        if (message.id === id) return message;
      }
    }
    return JSON.parse(raw);
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}
async function witness(backend) {
  const message = await rpc('tools/call', { name: `${backend}_1mcp_read_fixture`, arguments: {} });
  assert(!message.error && !message.result?.isError, `${backend} read refused`);
  assert.equal(JSON.parse(message.result.content[0].text).witness, backend);
}
try {
  const env = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !key.startsWith('ONE_MCP_') && !key.startsWith('MCP_GATEWAY_')),
  );
  child = spawn(
    process.execPath,
    [
      entry,
      'serve',
      '--config',
      join(root, 'mcp.json'),
      '--config-dir',
      join(root, 'state'),
      '--transport',
      'http',
      '--host',
      '127.0.0.1',
      '--port',
      String(port),
      '--enable-auth=false',
      '--enable-internal-tools=false',
      '--health-info-level',
      'full',
      '--log-level',
      'warn',
      '--enable-async-loading',
      '--async-max-retries=0',
      '--async-background-retry',
      '--async-background-retry-interval=1000',
    ],
    { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] },
  );
  child.stdout.on('data', (data) => {
    log += data;
  });
  child.stderr.on('data', (data) => {
    log += data;
  });
  await until(async () => {
    const h = await health();
    return (
      h?.servers?.details?.healthy?.state === 'ready' &&
      h?.servers?.details?.flaky?.state === 'failed' &&
      h?.servers?.details?.denied?.state === 'failed'
    );
  }, 'initial failed loading settlement');
  const initialPid = child.pid;
  const deniedAtSettlement = counts.denied;
  assert(counts.unavailable > 0);
  await rpc('initialize', {
    protocolVersion: '2025-03-26',
    capabilities: {},
    clientInfo: { name: 'isolated-outage-proof', version: '1' },
  });
  await rpc('notifications/initialized', {}, true);
  await rpc('tools/list');
  await witness('healthy');
  available = true;
  await until(async () => {
    await witness('healthy');
    return (await health())?.servers?.details?.flaky?.state === 'ready';
  }, 'background recovery after HTTP 502');
  assert.equal(child.pid, initialPid);
  assert.equal(child.exitCode, null);
  assert.equal(counts.denied, deniedAtSettlement, 'permanent HTTP 403 must not be retried');
  const names = (await rpc('tools/list')).result.tools.map((tool) => tool.name);
  assert(names.includes('healthy_1mcp_read_fixture'));
  assert(names.includes('flaky_1mcp_read_fixture'));
  await witness('flaky');
  const originalSession = session;
  session = undefined;
  await rpc('initialize', {
    protocolVersion: '2025-03-26',
    capabilities: {},
    clientInfo: { name: 'fresh-outage-proof', version: '1' },
  });
  await rpc('notifications/initialized', {}, true);
  await witness('flaky');
  assert.notEqual(session, originalSession);
  console.log(
    JSON.stringify({
      ok: true,
      entry,
      initialPid,
      sameGatewayProcess: true,
      initialFailureObserved: true,
      sameFrontendRecovered: true,
      freshFrontendRecovered: true,
      permanentDenialPreserved: true,
      counts,
      evidence: root,
    }),
  );
} catch (error) {
  console.error(JSON.stringify({ ok: false, entry, error: error.message, counts, evidence: root }));
  process.exitCode = 1;
} finally {
  if (session && child?.exitCode === null)
    await fetch(`${url}/mcp`, {
      method: 'DELETE',
      headers: { 'mcp-session-id': session },
      signal: AbortSignal.timeout(2000),
    })
      .then((r) => r.body?.cancel())
      .catch(() => undefined);
  if (child && child.exitCode === null) {
    const exited = new Promise((done) => child.once('exit', done));
    child.kill('SIGTERM');
    const kill = setTimeout(() => child.kill('SIGKILL'), 5000);
    await exited;
    clearTimeout(kill);
  }
  await writeFile(join(root, 'gateway.log'), log);
  peer.closeAllConnections();
  await new Promise((done) => peer.close(done));
  if (child) assert.notEqual(child.exitCode ?? child.signalCode, null, 'exact gateway child must be reaped');
}
