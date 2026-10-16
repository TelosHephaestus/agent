import { createMockLegacyInboundConnection, createMockOutboundConnection } from '@test/unit-utils/MockFactories.js';

import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';

import logger, * as loggerModule from '@src/logger/logger.js';
import ConfigContext from '@src/config/configContext.js';
import { getRetainedToolCatalog } from '@src/core/capabilities/retainedToolCatalog.js';
import { resolveCapabilityVisibility } from '@src/core/protocol/requestHandlerUtils.js';
import {
  ClientStatus,
  type InboundConnectionConfig,
  type MCPServerParams,
  type OutboundConnections,
} from '@src/core/types/index.js';
import type { LocalDiagnosticRecord } from '@src/logger/localDiagnostics.js';
import { ClientFactory } from '@src/sdk/legacy/client/runtime/clientFactory.js';
import { createLegacyOutboundConnection } from '@src/sdk/legacy/client/runtime/legacyOutboundConnection.js';
import type { AuthProviderTransport } from '@src/sdk/legacy/client/runtime/legacyTransport.js';
import { Server as GatewayServer } from '@src/sdk/legacy/server/index.js';

import { registerToolHandlers } from './toolRequestHandlers.js';

const config = vi.hoisted(() => ({ servers: {} as Record<string, MCPServerParams> }));
vi.mock('@src/config/mcpConfigManager.js', () => ({
  McpConfigManager: {
    getInstance: () => ({ getTransportConfig: () => config.servers, getConfiguredServerTargets: () => config.servers }),
  },
}));
vi.mock('@src/core/server/serverManager.js', () => ({
  ServerManager: {
    current: { getTemplateServerManager: () => undefined },
  },
}));
vi.mock('@src/core/capabilities/internalCapabilitiesProvider.js', () => ({
  InternalCapabilitiesProvider: {
    getInstance: () => ({ initialize: async () => undefined, getAvailableTools: () => [] }),
  },
}));

type Handler = (request: { params?: Record<string, unknown> }, extra: { signal: AbortSignal }) => Promise<any>;
const tool = (name: string, properties?: Record<string, unknown>) => ({
  name,
  inputSchema: { type: 'object', ...(properties ? { properties } : {}) },
});

describe('retained catalog protocol with a real HTTP Streamable upstream', () => {
  let dir: string;
  let upstream: http.Server;
  let client: Client;
  let upstreamTransport: AuthProviderTransport;
  let connections: OutboundConnections;
  let records: LocalDiagnosticRecord[];
  let level: string;
  let tools: ReturnType<typeof tool>[];
  let partial: boolean;
  let requestCount: number;

  function handlers(filter: Partial<InboundConnectionConfig> = {}) {
    const captured: Handler[] = [];
    const inbound = createMockLegacyInboundConnection({
      ...filter,
      server: {
        setRequestHandler: vi.fn((_schema, handler) => captured.push(handler)),
        getClientCapabilities: () => ({}),
      } as never,
    });
    registerToolHandlers(connections, inbound);
    const controller = new AbortController();
    const extra = { signal: controller.signal };
    return {
      list: (cursor?: string) => captured[0]({ params: cursor ? { cursor } : {} }, extra),
      call: (name = 'backend_1mcp_old', args: Record<string, unknown> = {}) =>
        captured[1]({ params: { name, arguments: args } }, extra),
      abort: () => controller.abort(),
    };
  }

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'retention-http-'));
    ConfigContext.getInstance().setConfigDir(dir);
    records = [];
    level = logger.level;
    logger.level = 'info';
    vi.spyOn(loggerModule, 'writeLocalDiagnosticRecord').mockImplementation((record) => {
      records.push(record);
    });
    tools = [tool('old'), tool('keep')];
    partial = false;
    requestCount = 0;
    upstream = http.createServer((req, res) => {
      if (req.method !== 'POST') {
        res.writeHead(405);
        res.end();
        return;
      }
      let raw = '';
      req.on('data', (chunk) => {
        raw += chunk;
      });
      req.on('end', () => {
        const message = JSON.parse(raw);
        requestCount++;
        if (message.id === undefined) {
          res.writeHead(202);
          res.end();
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        if (message.method === 'initialize') {
          res.end(
            JSON.stringify({
              jsonrpc: '2.0',
              id: message.id,
              result: {
                protocolVersion: message.params.protocolVersion,
                serverInfo: { name: 'fake', version: '1' },
                capabilities: { tools: {} },
              },
            }),
          );
          return;
        }
        if (partial && message.params?.cursor) {
          res.end(
            JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32603, message: 'page unavailable' } }),
          );
          return;
        }
        res.end(
          JSON.stringify({
            jsonrpc: '2.0',
            id: message.id,
            result:
              message.method === 'tools/list'
                ? { tools, ...(partial ? { nextCursor: 'page-2' } : {}) }
                : { content: [{ type: 'text', text: 'ok' }] },
          }),
        );
      });
    });
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    const url = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}/mcp`;
    config.servers = { backend: { type: 'http', url, tags: ['household'] } };
    const transport = new StreamableHTTPClientTransport(new URL(url));
    upstreamTransport = transport as AuthProviderTransport;
    client = new ClientFactory().createClientInstance();
    await client.connect(transport);
    connections = new Map([
      [
        'backend',
        createLegacyOutboundConnection({
          name: 'backend',
          client,
          transport: transport as AuthProviderTransport,
          status: ClientStatus.Connected,
          capabilities: client.getServerCapabilities(),
        }),
      ],
    ]);
  });

  afterEach(async () => {
    await client?.close();
    upstream?.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    ConfigContext.getInstance().reset();
    rmSync(dir, { recursive: true, force: true });
    logger.level = level;
    vi.restoreAllMocks();
  });

  it('serves the identical projected catalog while down and rejects calls without any network traffic', async () => {
    const h = handlers();
    const healthy = await h.list();
    connections.get('backend')!.status = ClientStatus.Disconnected;
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    const before = requestCount;
    expect(await h.list()).toEqual(healthy);
    const stalledRequest = vi.fn(() => new Promise<never>(() => {}));
    connections.set(
      'unrelated-stalled-backend',
      createMockOutboundConnection({
        name: 'unrelated-stalled-backend',
        capabilities: { tools: {} },
        adapter: { request: stalledRequest },
      }),
    );
    const started = performance.now();
    expect(await h.call()).toMatchObject({
      isError: true,
      content: [{ type: 'text', text: expect.stringMatching(/temporarily unavailable.*retry/i) }],
    });
    expect(performance.now() - started).toBeLessThan(500);
    expect(requestCount).toBe(before);
    expect(stalledRequest).not.toHaveBeenCalled();
    expect(records.filter((r) => r.event === 'tool.catalog-response').map((r) => JSON.parse(r.details))).toEqual([
      { totalToolCount: 2, retainedToolCount: 0 },
      { totalToolCount: 2, retainedToolCount: 2 },
    ]);
    expect(records.some((r) => r.event === 'tool.catalog-retained' && JSON.parse(r.details).count === 2)).toBe(true);
  });

  it('discriminates a connected-but-stopped upstream from the disconnected pre-dispatch path', async () => {
    const h = handlers();
    const healthy = await h.list();
    const connection = connections.get('backend')!;
    expect(await h.call()).toEqual({ content: [{ type: 'text', text: 'ok' }] });
    expect(connection.status).toBe(ClientStatus.Connected);
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));

    expect(connection.status).toBe(ClientStatus.Connected);
    const retained = await h.list();
    expect(retained).toEqual(healthy);
    expect(
      records.some(
        (record) => record.event === 'tool.catalog-response' && JSON.parse(record.details).retainedToolCount === 2,
      ),
    ).toBe(true);

    const beforeCall = requestCount;
    const adapterRequests = vi.spyOn(connection.adapter, 'request');
    const disconnected = await h.call();
    expect(disconnected).toEqual({
      isError: true,
      content: [
        { type: 'text', text: 'Server not connected: backend. Backend temporarily unavailable; retry the request.' },
      ],
    });
    expect(adapterRequests.mock.calls.filter(([request]) => request.method === 'tools/call')).toHaveLength(0);
    expect(requestCount).toBe(beforeCall);
    expect(
      records.some(
        (record) => record.event === 'tool.catalog-response' && JSON.parse(record.details).retainedToolCount === 2,
      ),
    ).toBe(true);
  });

  it.each([false, true])(
    'emits the retained result through the actual SDK envelope, canonical=%s',
    async (canonicalSchemaProjection) => {
      tools = [tool('list_tasks', { page_size: {}, include_all_statuses: {} })];
      config.servers = { taskmaster: config.servers.backend };
      const connection = createLegacyOutboundConnection({
        name: 'taskmaster',
        client,
        transport: upstreamTransport,
        status: ClientStatus.Connected,
        capabilities: client.getServerCapabilities(),
      });
      connections = new Map([['taskmaster', connection]]);
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      const server = new GatewayServer({ name: 'synthetic-gateway', version: '1' }, { capabilities: { tools: {} } });
      const inbound = createMockLegacyInboundConnection({
        server,
        transport: serverTransport,
        canonicalSchemaProjection,
      });
      registerToolHandlers(connections, inbound);
      const gatewayClient = new Client({ name: 'synthetic-observer', version: '1' });
      const sent = vi.spyOn(serverTransport, 'send');
      try {
        await inbound.adapter.start();
        await gatewayClient.connect(clientTransport);
        const healthy = await gatewayClient.listTools();
        expect(
          await gatewayClient.callTool({
            name: 'taskmaster_1mcp_list_tasks',
            arguments: { page_size: 100, include_all_statuses: true },
          }),
        ).toEqual({ content: [{ type: 'text', text: 'ok' }] });
        upstream.closeAllConnections();
        await new Promise<void>((resolve) => upstream.close(() => resolve()));
        expect(connection.status).toBe(ClientStatus.Connected);
        expect(await gatewayClient.listTools()).toEqual(healthy);
        const adapterRequests = vi.spyOn(connection.adapter, 'request');
        sent.mockClear();
        const started = performance.now();
        let outcome: unknown;
        try {
          outcome = await gatewayClient.callTool({
            name: 'taskmaster_1mcp_list_tasks',
            arguments: { page_size: 100, include_all_statuses: true },
          });
        } catch {
          outcome = { rejected: true };
        }
        const envelopes = sent.mock.calls.map(([message]) => message).filter((message) => 'id' in message);
        process.stdout.write(`SYNTHETIC_NATIVE_SDK_ENVELOPES ${JSON.stringify(envelopes)}\n`);
        const expected = {
          isError: true,
          content: [
            {
              type: 'text',
              text: 'Server not connected: taskmaster. Backend temporarily unavailable; retry the request.',
            },
          ],
        };
        expect(outcome).toEqual(expected);
        expect(performance.now() - started).toBeLessThan(1000);
        expect(adapterRequests.mock.calls.filter(([request]) => request.method === 'tools/call')).toHaveLength(0);
        expect(envelopes).toHaveLength(1);
        const envelope = envelopes[0];
        expect(envelope).toMatchObject({ jsonrpc: '2.0', result: expected });
        expect('error' in envelope).toBe(false);
      } finally {
        await gatewayClient.close();
        await server.close();
      }
    },
  );

  it('preserves an ordinary upstream call error when acquisition is not retained', async () => {
    const h = handlers();
    await h.list();
    const adapter = connections.get('backend')!.adapter;
    const original = adapter.request.bind(adapter);
    const requests = vi.spyOn(adapter, 'request').mockImplementation((request) => {
      if (request.method === 'tools/call') throw new Error('synthetic ordinary upstream error');
      return original(request);
    });
    await expect(h.call()).rejects.toThrow('Error calling tool');
    expect(requests.mock.calls.filter(([request]) => request.method === 'tools/call')).toHaveLength(1);
  });

  it('preserves input validation before unavailable for a retained connected route', async () => {
    tools = [tool('old', { count: { type: 'integer' } })];
    const h = handlers();
    await h.list();
    expect(await h.call('backend_1mcp_old', { count: 1 })).toEqual({ content: [{ type: 'text', text: 'ok' }] });
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    await h.list();
    const adapter = vi.spyOn(connections.get('backend')!.adapter, 'request');
    expect(await h.call('backend_1mcp_old', { count: 'invalid' })).toEqual({
      isError: true,
      content: [{ type: 'text', text: 'schema_input_invalid' }],
    });
    expect(adapter.mock.calls.filter(([request]) => request.method === 'tools/call')).toHaveLength(0);
  });

  it('does not turn cancellation into a retained unavailable result', async () => {
    const h = handlers();
    await h.list();
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    await h.list();
    h.abort();
    const adapter = vi.spyOn(connections.get('backend')!.adapter, 'request');
    await expect(h.call()).rejects.toThrow();
    expect(adapter.mock.calls.filter(([request]) => request.method === 'tools/call')).toHaveLength(0);
  });

  it('serves startup retention before a backend connection exists, using normal tag and disabled-tool filters', async () => {
    const h = handlers();
    await h.list();
    connections.clear();
    expect((await handlers().list()).tools).toHaveLength(2);
    expect((await handlers({ tagFilterMode: 'simple-or', tags: ['other'] }).list()).tools).toEqual([]);
    expect((await handlers({ tagFilterMode: 'simple-or', tags: ['household'] }).list()).tools).toHaveLength(2);
    const visible = resolveCapabilityVisibility(
      connections,
      { tagFilterMode: 'none' } as InboundConnectionConfig,
      undefined,
      'tools',
      config.servers,
    );
    expect(visible.serverCandidates.get('backend')).toBe('backend');
    expect(getRetainedToolCatalog().read('backend')).toHaveLength(2);
  });

  it('keeps the full catalog after an incomplete page, then replaces it after a complete reconnect', async () => {
    const h = handlers();
    await h.list();
    partial = true;
    tools = [tool('partial')];
    expect((await h.list()).tools.map((t: { name: string }) => t.name)).toEqual([
      'backend_1mcp_keep',
      'backend_1mcp_old',
    ]);
    partial = false;
    tools = [tool('new')];
    expect((await h.list()).tools.map((t: { name: string }) => t.name)).toEqual(['backend_1mcp_new']);
    connections.get('backend')!.status = ClientStatus.Restarting;
    expect((await h.list()).tools.map((t: { name: string }) => t.name)).toEqual(['backend_1mcp_new']);
  });
});
