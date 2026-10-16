import { createMockOutboundConnection } from '@test/unit-utils/MockFactories.js';

import { mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { ClientStatus, type MCPServerParams } from '@src/core/types/index.js';

import { RetainedToolCatalog } from './retainedToolCatalog.js';
import { acquireRuntimeCapabilityCatalog } from './runtimeCapabilityCatalog.js';

const tool = (name: string) => ({ name, inputSchema: { type: 'object' as const } });

describe('retained static backend tool catalogs', () => {
  let dir: string;
  let store: RetainedToolCatalog;
  let configs: Record<string, MCPServerParams>;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'retained-tools-'));
    store = new RetainedToolCatalog(join(dir, 'catalog.json'));
    configs = { backend: { type: 'http', url: 'http://127.0.0.1:12345/mcp', tags: ['household'] } };
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  function fixture(protocolRevision?: string) {
    const connection = createMockOutboundConnection({
      name: 'backend',
      capabilities: { tools: {} },
      adapter: {
        protocolRevision,
        request: vi.fn(async () => ({ tools: [tool('old'), tool('keep')] }) as never),
      },
    });
    const connections = new Map([['backend', connection]]);
    const acquire = () =>
      acquireRuntimeCapabilityCatalog(connections, undefined, {
        serverConfigs: configs,
        retention: { store, serverConfigs: configs },
      });
    return { connection, connections, acquire };
  }
  async function names(snapshot: Awaited<ReturnType<typeof acquireRuntimeCapabilityCatalog>>) {
    return (await snapshot.list<{ name: string }>('tools', { enablePagination: false })).items.map((t) => t.name);
  }

  it('keeps complete tools during outage and binds them to no live connection', async () => {
    const f = fixture();
    const original = await names(await f.acquire());
    f.connection.status = ClientStatus.Disconnected;
    const retained = await f.acquire();
    expect(await names(retained)).toEqual(original);
    expect(retained.resolve('tools', 'backend_1mcp_old')).toMatchObject({ connection: undefined });
    expect(f.connection.adapter.request).toHaveBeenCalledTimes(1);
  });

  it('loads persisted tools before startup discovery with no connection object', async () => {
    const f = fixture();
    await f.acquire();
    store = new RetainedToolCatalog(join(dir, 'catalog.json'));
    f.connections.clear();
    expect(await names(await f.acquire())).toEqual(['backend_1mcp_keep', 'backend_1mcp_old']);
    expect(statSync(join(dir, 'catalog.json')).mode & 0o777).toBe(0o600);
    expect(JSON.parse(readFileSync(join(dir, 'catalog.json'), 'utf8')).version).toBe(1);
  });

  it('preserves the upstream schema dialect across disconnect and disk reload', async () => {
    const f = fixture('2025-06-18');
    const healthy = (await (await f.acquire()).list('tools', { enablePagination: false })).items;
    f.connections.clear();
    store = new RetainedToolCatalog(join(dir, 'catalog.json'));
    const retained = (await (await f.acquire()).list('tools', { enablePagination: false })).items;
    expect(retained).toEqual(healthy);
  });

  it('preserves upstream page boundaries when gateway pagination is enabled', async () => {
    const f = fixture();
    vi.mocked(f.connection.adapter.request).mockImplementation(
      async (request) =>
        ((request.params as { cursor?: string } | undefined)?.cursor
          ? { tools: [tool('second')] }
          : { tools: [tool('first')], nextCursor: 'second-page' }) as never,
    );
    const live = await f.acquire();
    const first = await live.list('tools', { enablePagination: true });
    expect(first.items).toHaveLength(1);
    const second = await live.list('tools', { enablePagination: true, cursor: first.nextCursor });
    f.connections.clear();
    store = new RetainedToolCatalog(join(dir, 'catalog.json'));
    const retained = await f.acquire();
    const retainedFirst = await retained.list('tools', { enablePagination: true });
    expect(retainedFirst.items).toEqual(first.items);
    expect(retainedFirst.nextCursor).toEqual(expect.any(String));
    const retainedSecond = await retained.list('tools', { enablePagination: true, cursor: retainedFirst.nextCursor });
    expect(retainedSecond.items).toEqual(second.items);
    expect(retainedSecond.nextCursor).toBeUndefined();
  });

  it('replaces retention including removals after a complete reconnect', async () => {
    const f = fixture();
    await f.acquire();
    vi.mocked(f.connection.adapter.request).mockResolvedValue({ tools: [tool('new')] } as never);
    await f.acquire();
    f.connection.status = ClientStatus.Error;
    expect(await names(await f.acquire())).toEqual(['backend_1mcp_new']);
  });

  it.each(['removed', 'disabled', 'changed'] as const)('discards retention when config is %s', async (change) => {
    const f = fixture();
    await f.acquire();
    f.connections.clear();
    if (change === 'removed') configs = {};
    if (change === 'disabled') configs.backend.disabled = true;
    if (change === 'changed') configs.backend.url = 'http://127.0.0.1:54321/mcp';
    expect(await names(await f.acquire())).toEqual([]);
    expect(JSON.parse(readFileSync(join(dir, 'catalog.json'), 'utf8')).servers).toEqual({});
  });

  it('applies disabled-tool and description projection identically to retained tools', async () => {
    configs.backend.disabledTools = ['old'];
    configs.backend.toolDescriptionOverrides = { keep: 'Operator description' };
    const f = fixture();
    const healthy = (
      await (await f.acquire()).list<{ name: string; description?: string }>('tools', { enablePagination: false })
    ).items;
    expect(healthy).toHaveLength(1);
    expect(healthy[0]).toMatchObject({ name: 'backend_1mcp_keep', description: 'Operator description' });
    f.connections.clear();
    expect((await (await f.acquire()).list('tools', { enablePagination: false })).items).toEqual(healthy);
  });

  it('does not replace retention with an upstream-declared partial observation', async () => {
    const f = fixture();
    await f.acquire();
    vi.mocked(f.connection.adapter.request).mockResolvedValue({
      tools: [tool('partial')],
      _meta: { 'app.1mcp/capability-pagination': { partial: true, complete: false } },
    } as never);
    await f.acquire();
    f.connections.clear();
    expect(await names(await f.acquire())).toEqual(['backend_1mcp_keep', 'backend_1mcp_old']);
  });

  it('never overwrites complete retention with a failed continuation', async () => {
    const f = fixture();
    await f.acquire();
    vi.mocked(f.connection.adapter.request).mockImplementation(async (request) => {
      if ((request.params as { cursor?: string } | undefined)?.cursor) throw new Error('page unavailable');
      return { tools: [tool('partial')], nextCursor: 'next' } as never;
    });
    await f.acquire();
    f.connection.status = ClientStatus.Error;
    expect(await names(await f.acquire())).toEqual(['backend_1mcp_keep', 'backend_1mcp_old']);
  });
});
