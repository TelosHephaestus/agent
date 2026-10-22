import type { Request, Response } from 'express';
import { describe, expect, it } from 'vitest';

import { createObservabilityHandler, lookupOperation } from './apiRoutes.js';

function call(query: Request['query']) {
  const result: { status: number; body?: unknown; cacheControl?: string } = { status: 200 };
  const response = {
    setHeader(name: string, value: string) {
      if (name === 'Cache-Control') result.cacheControl = value;
    },
    status(code: number) {
      result.status = code;
      return response;
    },
    json(body: unknown) {
      result.body = body;
      return response;
    },
  } as Response;
  createObservabilityHandler()({ query } as Request, response, () => undefined);
  return result;
}

describe('observability query', () => {
  it('returns schema version 1 and a bounded snapshot without dispatching requests', () => {
    const result = call({ active: 'true', limit: '1' });
    expect(result.status).toBe(200);
    expect(result.cacheControl).toBe('no-store');
    expect(result.body).toMatchObject({
      schemaVersion: 1,
      coverage: expect.any(Object),
      summary: expect.any(Object),
      operations: [],
      sessions: [],
    });
  });

  it('returns an explicit unknown for an operation outside bounded history', () => {
    expect(lookupOperation('missing-operation')).toEqual({
      status: 404,
      body: { schemaVersion: 1, unknown: true, id: 'missing-operation' },
    });
  });

  it.each([
    { query: { active: 'yes' }, field: 'active' },
    { query: { limit: '0' }, field: 'limit' },
    { query: { limit: '01' }, field: 'limit' },
    { query: { limit: '201' }, field: 'limit' },
    { query: { limit: ['1'] }, field: 'limit' },
  ])('rejects malformed query values $query', ({ query }) => {
    expect(call(query as Request['query']).status).toBe(400);
  });
});
