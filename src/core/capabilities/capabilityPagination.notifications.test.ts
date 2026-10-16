import { createMockOutboundConnection } from '@test/unit-utils/MockFactories.js';

import type { LegacySdkEvent } from '@src/sdk/contracts/index.js';

import { describe, expect, it, vi } from 'vitest';

import {
  registerCapabilityPaginationNotifications,
  unregisterCapabilityPaginationForwarder,
} from './capabilityPagination.js';

describe('catalog notification generation ownership', () => {
  it('follows a replaced connection before polling rebind and unregisters the owner', async () => {
    const original = createMockOutboundConnection({
      adapter: { nextEvent: vi.fn().mockReturnValue(new Promise(() => {})) },
    });
    const connections = new Map([['backend', original]]);
    const forward = vi.fn().mockResolvedValue(undefined);
    const owner = {};
    registerCapabilityPaginationNotifications(connections, original, owner, forward);
    function replacement() {
      return createMockOutboundConnection({
        adapter: {
          nextEvent: vi
            .fn()
            .mockResolvedValueOnce({
              type: 'notification',
              notification: { method: 'notifications/tools/list_changed' },
            })
            .mockResolvedValue({ type: 'closed' }),
        },
      });
    }
    const recovered = replacement();
    connections.set('backend', recovered);
    // This is the real ClientManager path: no inbound forwarding key is passed.
    registerCapabilityPaginationNotifications(connections, recovered);
    await vi.waitFor(() =>
      expect(forward).toHaveBeenCalledWith({ method: 'notifications/tools/list_changed' }, recovered),
    );
    unregisterCapabilityPaginationForwarder(connections, owner);
    const later = replacement();
    connections.set('backend', later);
    registerCapabilityPaginationNotifications(connections, later);
    await vi.waitFor(() => expect(later.adapter.nextEvent).toHaveBeenCalledTimes(2));
    expect(forward).toHaveBeenCalledOnce();
  });
  it('drops a retired generation event and only delivers replacement events', async () => {
    let emit!: (event: LegacySdkEvent) => void;
    const pending = new Promise<LegacySdkEvent>((resolve) => {
      emit = resolve;
    });
    const connection = createMockOutboundConnection({ adapter: { nextEvent: vi.fn().mockReturnValueOnce(pending) } });
    const connections = new Map([['backend', connection]]);
    const forward = vi.fn().mockResolvedValue(undefined);
    const owner = {};
    registerCapabilityPaginationNotifications(connections, connection, owner, forward);
    const retired = connection.adapter;
    const replacement = createMockOutboundConnection({
      adapter: {
        nextEvent: vi
          .fn()
          .mockResolvedValueOnce({ type: 'notification', notification: { method: 'notifications/tools/list_changed' } })
          .mockResolvedValue({ type: 'closed' }),
      },
    }).adapter;
    Object.assign(connection, { adapter: replacement });
    emit({ type: 'notification', notification: { method: 'notifications/tools/list_changed' } });
    await Promise.resolve();
    await Promise.resolve();
    expect(forward).not.toHaveBeenCalled();
    expect(retired.nextEvent).toHaveBeenCalledOnce();
    expect(replacement.nextEvent).not.toHaveBeenCalled();
    registerCapabilityPaginationNotifications(connections, connection, owner, forward);
    await vi.waitFor(() => expect(forward).toHaveBeenCalledOnce());
    expect(forward).toHaveBeenCalledWith({ method: 'notifications/tools/list_changed' }, connection);
  });
});
