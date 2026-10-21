import { Client as ModernClient } from '@modelcontextprotocol/client';

import { ClientStatus } from '@src/core/types/index.js';
import type { LegacySdkAdapter } from '@src/sdk/contracts/index.js';

import { describe, expect, it, vi } from 'vitest';

import { ClientFactory } from './clientFactory.js';
import {
  beginLegacyInteractionRequest,
  currentLegacyInteractionCapabilities,
  supportsRequestLocalInteractions,
  withLegacyInteractionLease,
} from './legacyInteractionLease.js';
import { createLegacyOutboundConnection } from './legacyOutboundConnection.js';
import type { AuthProviderTransport } from './legacyTransport.js';

describe('adapter interaction lease', () => {
  it.each(['legacy', 'modern-sdk-legacy'] as const)(
    'multiplexes only current empty factory profiles for %s',
    async (sdk) => {
      const transport = { ...(sdk === 'legacy' ? {} : { outboundProtocolVersion: 'auto' }) } as AuthProviderTransport;
      const client = new ClientFactory().createClient(transport, {});
      if (client instanceof ModernClient) {
        vi.spyOn(client, 'getProtocolEra').mockReturnValue('legacy');
        vi.spyOn(client, 'getNegotiatedProtocolVersion').mockReturnValue('2025-11-25');
      }
      const { adapter } = createLegacyOutboundConnection({
        name: 'profile',
        client,
        transport,
        status: ClientStatus.Connected,
      });
      expect(supportsRequestLocalInteractions(adapter)).toBe(true);
      let release!: () => void;
      const owned = withLegacyInteractionLease(
        adapter,
        () =>
          new Promise<void>((resolve) => {
            release = resolve;
          }),
      );
      try {
        const finish = beginLegacyInteractionRequest(adapter, 'tools/call');
        await withLegacyInteractionLease(adapter, async () => {
          beginLegacyInteractionRequest(adapter, 'resources/read')();
        });
        finish();
      } finally {
        release();
        await owned;
      }
      client.registerCapabilities({ roots: {} });
      expect(supportsRequestLocalInteractions(adapter)).toBe(false);
      const exclusive = withLegacyInteractionLease(
        adapter,
        () =>
          new Promise<void>((resolve) => {
            release = resolve;
          }),
      );
      try {
        expect(() => beginLegacyInteractionRequest(adapter, 'tools/call')).toThrow('interaction_capacity_exceeded');
      } finally {
        release();
        await exclusive;
      }
    },
  );
  it('keeps overlapping negotiated-modern contexts independent', async () => {
    const adapter = { protocol: { era: 'modern' } } as LegacySdkAdapter;
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    const owned = withLegacyInteractionLease(
      adapter,
      async () => {
        await pending;
        expect(currentLegacyInteractionCapabilities()).toEqual({ roots: {} });
      },
      undefined,
      undefined,
      { roots: {} },
    );
    await withLegacyInteractionLease(
      adapter,
      async () => {
        expect(currentLegacyInteractionCapabilities()).toEqual({ sampling: {} });
        beginLegacyInteractionRequest(adapter, 'tools/call')();
      },
      undefined,
      undefined,
      { sampling: {} },
    );
    release();
    await owned;
  });

  it('retains exclusivity when a modern SDK has negotiated legacy', async () => {
    const adapter = { protocol: { era: 'legacy' } } as LegacySdkAdapter;
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    const owned = withLegacyInteractionLease(adapter, () => pending);
    expect(() => beginLegacyInteractionRequest(adapter, 'tools/call')).toThrow('interaction_capacity_exceeded');
    release();
    await owned;
  });
  it('prevents unscoped REST or CLI invocation from sharing a protocol owner', async () => {
    const adapter = {} as LegacySdkAdapter;
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    const owned = withLegacyInteractionLease(adapter, () => pending);
    expect(() => beginLegacyInteractionRequest(adapter, 'tools/call')).toThrow('interaction_capacity_exceeded');
    release();
    await owned;
    const end = beginLegacyInteractionRequest(adapter, 'tools/call');
    await expect(withLegacyInteractionLease(adapter, async () => undefined)).rejects.toThrow(
      'interaction_capacity_exceeded',
    );
    end();
    await expect(withLegacyInteractionLease(adapter, async () => undefined)).resolves.toBeUndefined();
  });

  it('admits the owner once and rejects concurrent nested invocation', async () => {
    const adapter = {} as LegacySdkAdapter;
    await withLegacyInteractionLease(adapter, async () => {
      const end = beginLegacyInteractionRequest(adapter, 'prompts/get');
      expect(() => beginLegacyInteractionRequest(adapter, 'resources/read')).toThrow('interaction_capacity_exceeded');
      end();
      beginLegacyInteractionRequest(adapter, 'resources/read')();
    });
  });
});
