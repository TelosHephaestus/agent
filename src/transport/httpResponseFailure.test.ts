import { StreamableHTTPClientTransport as ModernTransport } from '@modelcontextprotocol/client';

import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';

import { StreamableHTTPClientTransport as LegacyTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

import { ClientStatus } from '@src/core/types/client.js';
import { ClientFactory } from '@src/sdk/legacy/client/runtime/clientFactory.js';
import { ConnectionHandler } from '@src/sdk/legacy/client/runtime/connectionHandler.js';
import {
  createLegacyOutboundConnection,
  requestLegacyOutbound,
} from '@src/sdk/legacy/client/runtime/legacyOutboundConnection.js';
import type { AuthProviderTransport } from '@src/sdk/legacy/client/runtime/legacyTransport.js';
import { createSseWireLimitedFetch, registerSseWireLimitOwner } from '@src/transport/sseWireLimit.js';

import { describe, expect, it } from 'vitest';

async function fixture() {
  const pending = new Map<string, { res: http.ServerResponse; id: string | number }>();
  const counts = new Map<string, number>();
  let initializes = 0;
  const server = http.createServer(async (req, res) => {
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
    const json = (result: unknown) =>
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }));
    if (message.method === 'initialize') {
      initializes++;
      json({
        protocolVersion: '2025-06-18',
        capabilities: { tools: {} },
        serverInfo: { name: 'stream-fault-fixture', version: '1' },
      });
      return;
    }
    if (message.method === 'tools/list') {
      json({ tools: [{ name: 'fixture', inputSchema: { type: 'object', properties: { mode: { type: 'string' } } } }] });
      return;
    }
    if (message.method !== 'tools/call') {
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32601, message: 'Method not found' } }));
      return;
    }
    const mode = message.params.arguments.mode;
    counts.set(mode, (counts.get(mode) ?? 0) + 1);
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(': stream is open\n\n');
    if (mode === 'abort' || mode === 'hold') {
      pending.set(mode, { res, id: message.id });
      return;
    }
    const frame =
      mode === 'error'
        ? { jsonrpc: '2.0', id: message.id, error: { code: -32001, message: 'fixture refusal' } }
        : { jsonrpc: '2.0', id: message.id, result: { content: [{ type: 'text', text: mode }] } };
    res.end(`event: message\ndata: ${JSON.stringify(frame)}\n\n`);
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  return {
    url: new URL(`http://127.0.0.1:${(server.address() as AddressInfo).port}/mcp`),
    counts,
    get initializes() {
      return initializes;
    },
    async accepted(mode: string) {
      const deadline = Date.now() + 1000;
      while (!pending.has(mode)) {
        if (Date.now() > deadline) throw new Error('Fixture request was not admitted');
        await new Promise((done) => setTimeout(done, 5));
      }
    },
    cut() {
      assert(pending.get('abort'));
      pending.get('abort')!.res.destroy();
    },
    release() {
      const held = pending.get('hold');
      assert(held);
      held.res.end(
        `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: held.id, result: { content: [{ type: 'text', text: 'held-completed' }] } })}\n\n`,
      );
    },
    async close() {
      server.closeAllConnections();
      await new Promise<void>((done) => server.close(() => done()));
    },
  };
}

for (const mode of ['legacy', 'auto'] as const) {
  describe(`${mode} upstream POST SSE response failure`, () => {
    it('rejects only the cut-off request promptly, never replays it, and preserves sibling and subsequent reads', async () => {
      const upstream = await fixture();
      let transport: AuthProviderTransport;
      const fetch = createSseWireLimitedFetch(undefined, () => transport);
      transport =
        mode === 'legacy'
          ? new LegacyTransport(upstream.url, { fetch })
          : (new ModernTransport(upstream.url, { fetch }) as AuthProviderTransport);
      transport.outboundProtocolVersion = mode;
      transport.requestTimeout = 4000;
      registerSseWireLimitOwner(transport, undefined);
      const factory = new ClientFactory();
      const connected = await new ConnectionHandler().connectWithRetry(
        factory.createClient(transport),
        transport,
        'fixture',
      );
      const connection = createLegacyOutboundConnection({
        name: 'fixture',
        ...connected,
        status: ClientStatus.Connected,
      });
      const call = (value: string) =>
        requestLegacyOutbound(connection, 'tools/call', { name: 'fixture', arguments: { mode: value } });
      let heldSettled = false;
      const held = call('hold').finally(() => {
        heldSettled = true;
      });
      // Attach rejection handlers before injecting the socket fault.
      const heldOutcome = held.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      const broken = call('abort').then(
        () => 'unexpected_success',
        () => 'rejected',
      );
      try {
        await upstream.accepted('hold');
        await upstream.accepted('abort');
        upstream.cut();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadline = new Promise<string>((done) => {
          timer = setTimeout(() => done('request_still_pending'), 750);
        });
        const outcome = await Promise.race([broken, deadline]);
        clearTimeout(timer);
        expect(outcome).toBe('rejected');
        expect(heldSettled).toBe(false);
        expect(upstream.counts.get('abort')).toBe(1);
        upstream.release();
        expect(await heldOutcome).toHaveProperty('value');
        await expect(call('ok')).resolves.toBeDefined();
        await expect(call('error')).rejects.toBeDefined();
        await expect(call('after-error')).resolves.toBeDefined();
        expect(upstream.counts.get('abort')).toBe(1);
        expect(upstream.initializes).toBe(1);
      } finally {
        upstream.release();
        await connected.client.close();
        await transport.close();
        await Promise.allSettled([held, broken]);
        await upstream.close();
      }
    }, 10000);
  });
}
