import { createMockLegacyInboundConnection, createMockOutboundConnection } from '@test/unit-utils/MockFactories.js';

import { mkdtempSync, rmSync } from 'node:fs';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

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
import { createLegacyOutboundConnection } from '@src/sdk/legacy/client/runtime/legacyOutboundConnection.js';
import type { AuthProviderTransport } from '@src/sdk/legacy/client/runtime/legacyTransport.js';

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
const tool = (name: string) => ({ name, inputSchema: { type: 'object' } });

describe('retained catalog protocol with a real HTTP Streamable upstream', () => {
  let dir: string;
  let upstream: http.Server;
  let client: Client;
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
      server: { setRequestHandler: vi.fn((_schema, handler) => captured.push(handler)) } as never,
    });
    registerToolHandlers(connections, inbound);
    const extra = { signal: new AbortController().signal };
    return {
      list: (cursor?: string) => captured[0]({ params: cursor ? { cursor } : {} }, extra),
      call: (name = 'backend_1mcp_old') => captured[1]({ params: { name, arguments: {} } }, extra),
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
    client = new Client({ name: 'retention-test', version: '1' });
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
