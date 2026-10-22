import { AsyncLocalStorage } from 'node:async_hooks';

class UpstreamHttpResponseError extends Error {
  constructor() {
    super('Upstream HTTP response stream failed before completion');
    this.name = 'UpstreamHttpResponseError';
  }
}

const responseFailure = new AsyncLocalStorage<() => void>();

/** Capture the exact SDK request that owns an HTTP response, not the shared transport. */
export function currentHttpResponseFailure(): (() => void) | undefined {
  return responseFailure.getStore();
}

/** Let a failed body settle its SDK waiter without closing siblings or replaying work. */
export async function withHttpResponseFailure<T>(controller: AbortController, request: () => Promise<T>): Promise<T> {
  let active = true;
  let failure: UpstreamHttpResponseError | undefined;
  const fail = () => {
    if (!active || controller.signal.aborted) return;
    failure = new UpstreamHttpResponseError();
    controller.abort(failure);
  };
  try {
    return await responseFailure.run(fail, request);
  } catch (error) {
    throw failure ?? error;
  } finally {
    active = false;
  }
}
