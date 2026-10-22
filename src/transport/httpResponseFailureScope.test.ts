import { currentHttpResponseFailure, withHttpResponseFailure } from '@src/transport/httpResponseFailure.js';
import { createSseWireLimitedFetch } from '@src/transport/sseWireLimit.js';

import { describe, expect, it } from 'vitest';

describe('HTTP response request ownership', () => {
  it('does not abort completed work when its body reports a later error', async () => {
    const controller = new AbortController();
    let lateFailure: (() => void) | undefined;
    await withHttpResponseFailure(controller, async () => {
      lateFailure = currentHttpResponseFailure();
      return 'complete';
    });
    expect(lateFailure).toBeDefined();
    lateFailure?.();
    expect(controller.signal.aborted).toBe(false);
    expect(currentHttpResponseFailure()).toBeUndefined();
  });

  it('preserves caller cancellation instead of replacing it with a stream failure', async () => {
    const controller = new AbortController();
    const cancellation = new Error('caller cancelled');
    const request = withHttpResponseFailure(controller, async () => {
      const failResponse = currentHttpResponseFailure();
      controller.abort(cancellation);
      failResponse?.();
      controller.signal.throwIfAborted();
    });
    await expect(request).rejects.toBe(cancellation);
  });

  it('does not attribute an independent GET stream failure to its enclosing POST request', async () => {
    const controller = new AbortController();
    const fetch = createSseWireLimitedFetch(
      async () =>
        new Response(
          new ReadableStream({
            start(body) {
              body.error(new Error('GET lost'));
            },
          }),
          { headers: { 'content-type': 'text/event-stream' } },
        ),
      () => ({ close: async () => undefined }),
    );
    await withHttpResponseFailure(controller, async () => {
      const response = await fetch('http://fixture.invalid/mcp', { method: 'GET' });
      await expect(response.text()).rejects.toThrow('GET lost');
      expect(controller.signal.aborted).toBe(false);
    });
  });
});
