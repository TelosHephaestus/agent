import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  CallToolRequestSchema,
  ListRootsResultSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

export async function startObservabilityBackend() {
  const sessions = new Map();
  const calls = [];
  const holds = new Map();
  const markers = { initialize: 0, listTools: 0, calls: 0, callbacks: 0 };
  const secret = `OBSERVABILITY_SENTINEL_${randomUUID()}`;
  let mutationCount = 0;
  let accepting = true;

  const hold = (label) => {
    if (!holds.has(label)) holds.set(label, { entered: deferred(), release: deferred(), done: deferred() });
    return holds.get(label);
  };

  const http = createServer(async (request, response) => {
    let message;
    if (request.method === 'POST') {
      let raw = '';
      for await (const chunk of request) raw += chunk;
      message = JSON.parse(raw);
    }
    const sessionId = request.headers['mcp-session-id'];
    if (!accepting || (sessionId && !sessions.has(sessionId))) {
      response.writeHead(400, { 'Content-Type': 'application/json' });
      response.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: message?.id ?? null,
          error: { code: -32000, message: 'No valid session' },
        }),
      );
      return;
    }

    let current = sessions.get(sessionId);
    if (!current && message?.method === 'initialize') {
      markers.initialize++;
      const server = new Server(
        { name: 'gateway-observability-fixture', version: '1' },
        { capabilities: { tools: {} } },
      );
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: randomUUID,
        enableJsonResponse: false,
        onsessioninitialized: (id) => sessions.set(id, { server, transport }),
      });
      server.setRequestHandler(ListToolsRequestSchema, async () => {
        markers.listTools++;
        return {
          tools: [
            {
              name: 'hold',
              description: 'Wait for explicit release',
              inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
            },
            {
              name: 'read',
              description: 'Return the supplied marker',
              inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
            },
            {
              name: 'fail',
              description: 'Return a tool error',
              inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
            },
            {
              name: 'mutate',
              description: 'Count a mutation once',
              inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
            },
            {
              name: 'reverse',
              description: 'Request caller roots',
              inputSchema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'] },
            },
          ],
        };
      });
      server.setRequestHandler(CallToolRequestSchema, async (rpc, extra) => {
        const label = rpc.params.arguments?.label ?? '';
        markers.calls++;
        calls.push({ name: rpc.params.name, label });
        if (rpc.params.name === 'read') return { content: [{ type: 'text', text: `${secret}:${label}` }] };
        if (rpc.params.name === 'fail')
          return { isError: true, content: [{ type: 'text', text: `${secret}:failure` }] };
        if (rpc.params.name === 'mutate') {
          mutationCount++;
          return { content: [{ type: 'text', text: 'mutation accepted' }] };
        }
        if (rpc.params.name === 'reverse') {
          markers.callbacks++;
          const roots = await server.request({ method: 'roots/list' }, ListRootsResultSchema, { signal: extra.signal });
          return { content: [{ type: 'text', text: JSON.stringify(roots) }] };
        }
        const latch = hold(label);
        latch.entered.resolve();
        extra.signal.addEventListener('abort', () => latch.done.resolve('aborted'), { once: true });
        try {
          if (rpc.params._meta?.progressToken !== undefined) {
            await extra.sendNotification({
              method: 'notifications/progress',
              params: {
                progressToken: rpc.params._meta.progressToken,
                progress: 1,
                total: 2,
                message: `${secret}:${label}`,
              },
            });
          }
          await Promise.race([latch.release.promise, latch.done.promise]);
          return { content: [{ type: 'text', text: `${secret}:${label}` }] };
        } finally {
          latch.done.resolve('finished');
        }
      });
      await server.connect(transport);
      current = { server, transport };
    }
    if (!current) {
      response.writeHead(400);
      response.end();
      return;
    }
    if (message?.method === 'roots/list') markers.callbacks++;
    await current.transport.handleRequest(request, response, message);
  });

  await new Promise((resolve, reject) => {
    http.once('error', reject);
    http.listen(0, '127.0.0.1', resolve);
  });

  return {
    url: `http://127.0.0.1:${http.address().port}/mcp`,
    secret,
    markers,
    calls,
    holds,
    hold,
    get mutationCount() {
      return mutationCount;
    },
    async loseSessions() {
      const active = [...sessions.values()];
      sessions.clear();
      await Promise.all(active.map(({ transport }) => transport.close()));
    },
    async close() {
      accepting = false;
      for (const latch of holds.values()) latch.release.resolve();
      await Promise.all([...sessions.values()].map(({ server }) => server.close().catch(() => undefined)));
      http.closeAllConnections();
      await new Promise((resolve) => http.close(resolve));
    },
  };
}
