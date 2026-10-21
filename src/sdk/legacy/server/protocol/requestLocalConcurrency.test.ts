import { createMockLegacyInboundConnection } from '@test/unit-utils/MockFactories.js';

import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';

import { ClientStatus } from '@src/core/types/index.js';
import { ClientFactory } from '@src/sdk/legacy/client/runtime/clientFactory.js';
import {
  createLegacyOutboundConnection,
  requestLegacyOutbound,
} from '@src/sdk/legacy/client/runtime/legacyOutboundConnection.js';
import { Server } from '@src/sdk/legacy/server/index.js';
import type { RequestHandlerExtra } from '@src/sdk/legacy/shared/protocol.js';
import { CallToolRequestSchema, type ServerNotification, type ServerRequest } from '@src/sdk/legacy/types.js';

import { describe, expect, it, vi } from 'vitest';

import { withRequestInteractionScope } from './requestInteractionScope.js';

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('noninteractive legacy request-local concurrency', () => {
  it('does not charge ordinary requests to the interactive continuation owner quota', async () => {
    const release = deferred();
    const client = new ClientFactory().createClient();
    const transport = {} as Parameters<typeof createLegacyOutboundConnection>[0]['transport'];
    const source = createLegacyOutboundConnection({
      name: 'fixture',
      client,
      transport,
      status: ClientStatus.Connected,
    });
    const inboundServer = new Server({ name: 'inbound', version: '1' });
    vi.spyOn(inboundServer, 'getClientCapabilities').mockReturnValue({});
    const inbound = createMockLegacyInboundConnection({ server: inboundServer });
    let entered = 0;
    const pending: Promise<unknown>[] = [];
    try {
      for (let index = 0; index < 40; index++) {
        pending.push(
          withRequestInteractionScope(
            source,
            inbound,
            {
              requestId: `ordinary-${index}`,
              signal: new AbortController().signal,
              sendRequest: vi.fn(),
              sendNotification: vi.fn(),
            },
            () => {
              entered++;
              return release.promise;
            },
          ).catch((error) => error),
        );
      }
      expect(entered).toBe(40);
    } finally {
      release.resolve();
      await Promise.all(pending);
      await inboundServer.close();
    }
  });
  it('completes an independent real SDK call before a held call is released', async () => {
    const entered = deferred();
    const release = deferred();
    const backend = new Server({ name: 'fixture', version: '1' }, { capabilities: { tools: {} } });
    backend.setRequestHandler(CallToolRequestSchema, async (request) => {
      if (request.params.name === 'hold') {
        entered.resolve();
        await release.promise;
      }
      return { content: [{ type: 'text', text: request.params.name }] };
    });
    const [transport, peer] = InMemoryTransport.createLinkedPair();
    await backend.connect(peer);
    const client = new ClientFactory().createClient(transport);
    await client.connect(transport);
    const source = createLegacyOutboundConnection({
      name: 'fixture',
      client,
      transport,
      status: ClientStatus.Connected,
    });
    const inboundServer = new Server({ name: 'inbound', version: '1' });
    vi.spyOn(inboundServer, 'getClientCapabilities').mockReturnValue({});
    const inbound = createMockLegacyInboundConnection({ server: inboundServer });
    const extra = (id: string): RequestHandlerExtra<ServerRequest, ServerNotification> => ({
      requestId: id,
      signal: new AbortController().signal,
      sendRequest: vi.fn(),
      sendNotification: vi.fn(),
    });
    let held: Promise<unknown> | undefined;
    try {
      held = withRequestInteractionScope(source, inbound, extra('held'), () =>
        requestLegacyOutbound(source, 'tools/call', { name: 'hold' }),
      );
      await entered.promise;
      const result = await withRequestInteractionScope(source, inbound, extra('independent'), () =>
        requestLegacyOutbound(source, 'tools/call', { name: 'read' }),
      );
      expect(result).toMatchObject({ content: [{ type: 'text', text: 'read' }] });
    } finally {
      release.resolve();
      await held;
      await source.adapter.close();
      await backend.close();
      await inboundServer.close();
    }
  });
});
