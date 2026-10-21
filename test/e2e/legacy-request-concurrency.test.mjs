import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  CallToolRequestSchema,
  ListRootsRequestSchema,
  ListRootsResultSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

import test from 'node:test';

const entry = process.env.GATEWAY_ENTRY || resolve('build/index.js');
const scratch = process.env.TMPDIR || process.env.RUNNER_TEMP || tmpdir();
function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function within(promise, ms = 5000, label = 'operation') {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} deadline`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function until(predicate, ms = 15000) {
  const deadline = Date.now() + ms;
  let last;
  while (Date.now() < deadline) {
    try {
      const result = await predicate();
      if (result) return result;
    } catch (error) {
      last = error;
    }
    await new Promise((done) => setTimeout(done, 50));
  }
  throw last || new Error('readiness deadline');
}
function result(text) {
  return { content: [{ type: 'text', text }] };
}
function text(value) {
  assert(!value.isError, JSON.stringify(value));
  return value.content.find((item) => item.type === 'text').text;
}

async function backend() {
  const sessions = new Map();
  const holds = new Map();
  const calls = [];
  const profiles = [];
  let available = true;
  let mutations = 0;
  const tools = ['hold', 'read', 'reverse', 'commit'].map((name) => ({
    name,
    inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
  }));
  function control(label) {
    if (!holds.has(label))
      holds.set(label, {
        entered: deferred(),
        release: deferred(),
        cancelled: deferred(),
        finished: deferred(),
        aborted: false,
      });
    return holds.get(label);
  }
  const http = createServer(async (req, res) => {
    try {
      let body;
      if (req.method === 'POST') {
        let data = '';
        for await (const chunk of req) data += chunk;
        body = JSON.parse(data);
      }
      const sid = req.headers['mcp-session-id'];
      if (!available || (sid && !sessions.has(sid))) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            jsonrpc: '2.0',
            id: body?.id ?? null,
            error: { code: -32000, message: 'Bad Request: No valid session ID' },
          }),
        );
        return;
      }
      let selected = sessions.get(sid);
      if (!selected && body?.method === 'initialize') {
        profiles.push(body.params.capabilities);
        const server = new Server({ name: 'concurrency-fixture', version: '1' }, { capabilities: { tools: {} } });
        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: randomUUID,
          enableJsonResponse: false,
          onsessioninitialized: (id) => sessions.set(id, { server, transport }),
        });
        server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));
        server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
          const label = request.params.arguments.label;
          const name = request.params.name;
          calls.push({ label, name });
          if (name === 'read') return result(label);
          if (name === 'reverse') {
            const roots = await server.request({ method: 'roots/list' }, ListRootsResultSchema, {
              signal: extra.signal,
            });
            return result(JSON.stringify(roots));
          }
          const held = control(label);
          if (name === 'commit') mutations++;
          const cancel = () => {
            held.aborted = true;
            held.cancelled.resolve();
          };
          extra.signal.addEventListener('abort', cancel, { once: true });
          held.entered.resolve();
          try {
            if (request.params._meta?.progressToken !== undefined) {
              await extra.sendNotification({
                method: 'notifications/progress',
                params: { progressToken: request.params._meta.progressToken, progress: 1, total: 2, message: label },
              });
            }
            await Promise.race([held.release.promise, held.cancelled.promise]);
            if (held.aborted) throw new Error('fixture cancelled');
            return result(label);
          } finally {
            extra.signal.removeEventListener('abort', cancel);
            held.finished.resolve();
          }
        });
        await server.connect(transport);
        selected = { server, transport };
      }
      if (!selected) {
        res.writeHead(400);
        res.end();
        return;
      }
      await selected.transport.handleRequest(req, res, body);
    } catch {
      if (!res.headersSent) res.writeHead(500);
      res.end();
    }
  });
  await new Promise((done) => http.listen(0, '127.0.0.1', done));
  return {
    url: `http://127.0.0.1:${http.address().port}/mcp`,
    control,
    calls,
    profiles,
    get mutations() {
      return mutations;
    },
    async expire() {
      const old = [...sessions.values()];
      sessions.clear();
      await Promise.all(old.map((item) => item.transport.close()));
    },
    async close() {
      available = false;
      for (const held of holds.values()) held.release.resolve();
      await Promise.all([...sessions.values()].map((item) => item.server.close()));
      http.closeAllConnections();
      await new Promise((done) => http.close(done));
    },
  };
}

async function harness() {
  const dir = await mkdtemp(join(scratch, 'household-request-concurrency-'));
  const home = join(dir, 'home');
  const configDir = join(dir, 'config');
  await mkdir(home);
  await mkdir(configDir);
  const main = await backend();
  const healthy = await backend();
  const config = join(configDir, 'mcp.json');
  const configBody = JSON.stringify({
    mcpServers: { fixture: { type: 'http', url: main.url }, healthy: { type: 'http', url: healthy.url } },
  });
  await writeFile(config, configBody);
  const reserve = createServer();
  await new Promise((done) => reserve.listen(0, '127.0.0.1', done));
  const port = reserve.address().port;
  await new Promise((done) => reserve.close(done));
  const gateway = spawn(
    process.execPath,
    [
      entry,
      'serve',
      '--config',
      config,
      '--config-dir',
      configDir,
      '--transport',
      'http',
      '--host',
      '127.0.0.1',
      '--port',
      String(port),
      '--enable-auth=false',
      '--enable-internal-tools=false',
      '--enable-config-reload=false',
      '--enable-session-persistence=false',
      '--enable-async-loading',
      '--async-background-retry',
      '--async-background-retry-interval=1000',
      '--log-level=debug',
    ],
    {
      cwd: dir,
      env: {
        PATH: '/usr/bin:/bin',
        HOME: home,
        TMPDIR: scratch,
        XDG_CONFIG_HOME: join(home, '.config'),
        XDG_STATE_HOME: join(home, '.local/state'),
        XDG_DATA_HOME: join(home, '.local/share'),
        XDG_CACHE_HOME: join(home, '.cache'),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let logs = '';
  gateway.stdout.on('data', (bytes) => {
    logs += bytes;
  });
  gateway.stderr.on('data', (bytes) => {
    logs += bytes;
  });
  const clients = [];
  const tokenRecords = [];
  async function client(roots) {
    const c = new Client(
      { name: 'request-local-acceptance', version: '1' },
      { capabilities: roots ? { roots: {} } : {} },
    );
    if (roots) c.setRequestHandler(ListRootsRequestSchema, roots);
    const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`), {
      fetch: async (url, options) => {
        if (options?.method === 'POST' && typeof options.body === 'string') {
          const body = JSON.parse(options.body);
          if (body.method === 'tools/call')
            tokenRecords.push({
              client: clients.indexOf(c),
              token: body.params._meta?.progressToken,
              label: body.params.arguments?.label,
            });
        }
        return fetch(url, options);
      },
    });
    clients.push(c);
    await c.connect(transport, { timeout: 5000 });
    await c.listTools({}, { timeout: 5000 });
    return c;
  }
  async function close() {
    for (const held of [main, healthy]) for (const call of held.calls) held.control(call.label).release.resolve();
    for (const c of clients) await c.close().catch(() => undefined);
    gateway.kill('SIGTERM');
    if (gateway.exitCode === null && gateway.signalCode === null) {
      await within(once(gateway, 'exit'), 5000, 'gateway termination').catch(async () => {
        gateway.kill('SIGKILL');
        await once(gateway, 'exit');
      });
    }
    await main.close();
    await healthy.close();
    assert.throws(() => process.kill(gateway.pid, 0), /ESRCH/);
    assert.equal(await readFile(config, 'utf8'), configBody);
    await writeFile(join(dir, 'gateway.log'), logs);
    await writeFile(
      join(dir, 'evidence.json'),
      JSON.stringify(
        {
          entry,
          calls: main.calls,
          profiles: main.profiles,
          mutations: main.mutations,
          tokenRecords,
          gatewayExit: { code: gateway.exitCode, signal: gateway.signalCode },
          configUnchanged: true,
        },
        null,
        2,
      ),
    );
  }
  try {
    await until(async () => {
      if (gateway.exitCode !== null) throw new Error(`gateway exited: ${logs.slice(-1500)}`);
      return (
        /initial.server.loading.phase.completed/.test(logs) &&
        (await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(500) })).ok
      );
    }, 20000);
  } catch (error) {
    await close();
    throw error;
  }
  console.log(`EVIDENCE ${dir}`);
  return { main, healthy, client, close, tokenRecords };
}

const call = (client, name, label, options) =>
  client.callTool({ name: `fixture_1mcp_${name}`, arguments: { label } }, undefined, { timeout: 5000, ...options });

test(
  'real HTTP gateway isolates request concurrency, cancellation, progress and reverse requests',
  { timeout: 90000 },
  async (t) => {
    const h = await harness();
    try {
      await t.test('a held request does not block an independent call on the same client', async () => {
        const c = await h.client();
        const held = h.main.control('held');
        const pending = call(c, 'hold', 'held').then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        try {
          await within(held.entered.promise);
          assert.equal(
            text(await within(call(c, 'read', 'independent'), 2000, 'independent read while held')),
            'independent',
          );
        } finally {
          held.release.resolve();
        }
        const outcome = await pending;
        assert(!outcome.error, outcome.error?.message);
        assert.equal(text(outcome.value), 'held');
        assert.deepEqual(h.main.profiles[0], {}, 'shared discovery must not advertise reverse requests');
        assert.equal(h.main.profiles.length, 1, 'ordinary calls must reuse the shared session');
      });
      await t.test('ordinary requests do not consume interactive owner or provider slots', async () => {
        const c = await h.client();
        const holds = Array.from({ length: 40 }, (_, index) => h.main.control(`burst-${index}`));
        const pending = holds.map((_, index) =>
          call(c, 'hold', `burst-${index}`, { timeout: 10000 }).then(
            (value) => ({ value }),
            (error) => ({ error }),
          ),
        );
        try {
          await within(Promise.all(holds.map((held) => held.entered.promise)), 5000, 'all ordinary requests admitted');
          assert.equal(text(await call(c, 'read', 'during-burst')), 'during-burst');
        } finally {
          for (const held of holds) held.release.resolve();
        }
        for (const outcome of await Promise.all(pending)) assert(!outcome.error, outcome.error?.message);
      });
      await t.test('cancelling one request leaves a sibling held request and independent reads alive', async () => {
        const c = await h.client();
        const cancelled = h.main.control('cancel-me');
        const sibling = h.main.control('keep-me');
        const controller = new AbortController();
        const first = call(c, 'hold', 'cancel-me', { signal: controller.signal }).then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        const second = call(c, 'hold', 'keep-me').then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        try {
          await within(Promise.all([cancelled.entered.promise, sibling.entered.promise]));
          controller.abort();
          assert((await first).error, 'cancelled call must fail');
          await within(cancelled.cancelled.promise, 5000, 'backend cancellation');
          assert.equal(sibling.aborted, false);
          assert.equal(text(await call(c, 'read', 'after-cancel')), 'after-cancel');
        } finally {
          cancelled.release.resolve();
          sibling.release.resolve();
        }
        assert.equal(text((await second).value), 'keep-me');
      });
      await t.test('colliding caller progress tokens never mix request destinations', async () => {
        const a = await h.client();
        const b = await h.client();
        const seenA = [],
          seenB = [];
        const progressA = deferred(),
          progressB = deferred();
        const heldA = h.main.control('progress-A'),
          heldB = h.main.control('progress-B');
        const first = call(a, 'hold', 'progress-A', {
          onprogress: (value) => {
            seenA.push(value.message);
            progressA.resolve();
          },
        }).then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        const second = call(b, 'hold', 'progress-B', {
          onprogress: (value) => {
            seenB.push(value.message);
            progressB.resolve();
          },
        }).then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        try {
          await within(
            Promise.all([progressA.promise, progressB.promise]),
            5000,
            'both request-local progress callbacks',
          );
          assert.deepEqual(seenA, ['progress-A']);
          assert.deepEqual(seenB, ['progress-B']);
          const tokens = h.tokenRecords.filter((item) => ['progress-A', 'progress-B'].includes(item.label));
          assert.equal(tokens.length, 2);
          assert.equal(tokens[0].token, tokens[1].token, 'test must exercise actual caller token collision');
        } finally {
          heldA.release.resolve();
          heldB.release.resolve();
        }
        assert.equal(text((await first).value), 'progress-A');
        assert.equal(text((await second).value), 'progress-B');
      });
      await t.test('interactive legacy callers use isolated sessions while ordinary calls continue', async () => {
        const enteredA = deferred(),
          enteredB = deferred(),
          releaseA = deferred(),
          releaseB = deferred();
        const a = await h.client(async () => {
          enteredA.resolve();
          await releaseA.promise;
          return { roots: [{ uri: 'file:///caller-A' }] };
        });
        const b = await h.client(async () => {
          enteredB.resolve();
          await releaseB.promise;
          return { roots: [{ uri: 'file:///caller-B' }] };
        });
        const ordinary = await h.client();
        const first = call(a, 'reverse', 'reverse-A').then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        const second = call(b, 'reverse', 'reverse-B').then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        try {
          await within(Promise.all([enteredA.promise, enteredB.promise]), 5000, 'both isolated legacy callbacks');
          assert.equal(text(await call(ordinary, 'read', 'during-callbacks')), 'during-callbacks');
          assert(h.main.profiles.slice(1).every((profile) => 'roots' in profile));
        } finally {
          releaseA.resolve();
          releaseB.resolve();
        }
        assert.deepEqual(JSON.parse(text((await first).value)), { roots: [{ uri: 'file:///caller-A' }] });
        assert.deepEqual(JSON.parse(text((await second).value)), { roots: [{ uri: 'file:///caller-B' }] });
      });
      await t.test(
        'backend session loss preserves unrelated reads and never replays an accepted mutation',
        async () => {
          const c = await h.client();
          const commit = h.main.control('accepted-once');
          const pending = call(c, 'commit', 'accepted-once').then(
            (value) => ({ value }),
            (error) => ({ error }),
          );
          await within(commit.entered.promise);
          assert.equal(h.main.mutations, 1);
          await h.main.expire();
          assert.equal(
            text(
              await c.callTool(
                { name: 'healthy_1mcp_read', arguments: { label: 'healthy-during-expiry' } },
                undefined,
                { timeout: 3000 },
              ),
            ),
            'healthy-during-expiry',
          );
          commit.release.resolve();
          await pending;
          await until(async () => {
            const response = await call(c, 'read', `recovered-${randomUUID()}`);
            return !response.isError;
          }, 15000);
          assert.equal(h.main.mutations, 1);
          assert.equal(h.main.calls.filter((item) => item.label === 'accepted-once').length, 1);
        },
      );
    } finally {
      await h.close();
    }
  },
);
