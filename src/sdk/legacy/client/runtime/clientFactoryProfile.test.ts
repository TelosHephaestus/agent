import { Client } from '@modelcontextprotocol/sdk/client/index.js';

import { describe, expect, it, vi } from 'vitest';

import { ClientFactory, getAdvertisedClientCapabilities } from './clientFactory.js';

describe('factory-owned advertised capability evidence', () => {
  it('invalidates evidence before a capability mutation even when registration throws', () => {
    const registration = vi.spyOn(Client.prototype, 'registerCapabilities').mockImplementation(() => {
      throw new Error('registration failed');
    });
    try {
      const client = new ClientFactory().createClient(undefined, {});
      expect(getAdvertisedClientCapabilities(client)).toEqual({});
      expect(() => client.registerCapabilities({ roots: {} })).toThrow('registration failed');
      expect(getAdvertisedClientCapabilities(client)).toBeUndefined();
    } finally {
      registration.mockRestore();
    }
  });
  it('keeps shared discovery clients noninteractive unless an explicit caller profile is supplied', () => {
    const factory = new ClientFactory();
    expect(getAdvertisedClientCapabilities(factory.createClient())).toEqual({});
    expect(getAdvertisedClientCapabilities(factory.createClient(undefined, { sampling: {} }))).toEqual({
      sampling: {},
    });
  });
  it('records detached frozen profiles on every constructor path and leaves external clients unknown', () => {
    const factory = new ClientFactory();
    const capabilities = { roots: { listChanged: false } };
    const client = factory.createClient(undefined, capabilities);
    capabilities.roots.listChanged = true;
    const recorded = getAdvertisedClientCapabilities(client);
    expect(recorded).toEqual({ roots: { listChanged: false } });
    expect(Object.isFrozen(recorded)).toBe(true);
    expect(Object.isFrozen(recorded?.roots)).toBe(true);
    expect(getAdvertisedClientCapabilities(factory.createClient(undefined, {}))).toEqual({});
    expect(getAdvertisedClientCapabilities(factory.createClientInstance())).toEqual({});
    expect(getAdvertisedClientCapabilities(factory.createPooledClientInstance())).toEqual({});
    expect(
      getAdvertisedClientCapabilities(factory.createClient({ outboundProtocolVersion: '2026-07-28' } as never, {})),
    ).toEqual({});
    expect(getAdvertisedClientCapabilities(new Client({ name: 'external', version: '1' }))).toBeUndefined();
  });

  it('invalidates proof after a successful public capability mutation', () => {
    const client = new ClientFactory().createClient(undefined, {});
    expect(getAdvertisedClientCapabilities(client)).toEqual({});
    client.registerCapabilities({ sampling: {} });
    expect(getAdvertisedClientCapabilities(client)).toBeUndefined();
  });
});
