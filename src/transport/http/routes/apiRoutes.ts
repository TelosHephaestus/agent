import { RATE_LIMIT_CONFIG } from '@src/constants.js';
import { ServerManager } from '@src/core/server/serverManager.js';
import { getOperation, getOperationSnapshot } from '@src/observability/operations/operationJournal.js';
import tagsExtractor from '@src/transport/http/middlewares/tagsExtractor.js';

import { NextFunction, Request, RequestHandler, Response, Router } from 'express';
import rateLimit from 'express-rate-limit';

import { createInspectHandler, createServersHandler } from './inspectRoutes.js';
import { createInstructionsHandler } from './instructionsRoutes.js';
import { createToolInvocationsHandler, createToolsHandler } from './toolRoutes.js';

// Re-export types and handlers so existing imports keep working
export type {
  InspectServerPayload,
  InspectServersPayload,
  InspectToolPayload,
  ServerSummary,
} from './inspectRoutes.js';
export {
  buildFilterConfig,
  createInspectHandler,
  createServersHandler,
  matchesFilterConfig,
  parseTarget,
  resolveConnectionByServerName,
} from './inspectRoutes.js';
export { createToolInvocationsHandler, createToolsHandler } from './toolRoutes.js';
export { createCliTokenRoute } from './cliTokenRoute.js';
export type { SDKOAuthServerProvider } from '@src/auth/sdkOAuthServerProvider.js';

// ---- Route factory ----

export interface ToolInvocationRateLimitPolicy {
  windowMs: number;
  max: number;
}

const DEFAULT_TOOL_INVOCATION_RATE_LIMIT_POLICY: ToolInvocationRateLimitPolicy = {
  windowMs: RATE_LIMIT_CONFIG.TOOL_INVOCATIONS.WINDOW_MS,
  max: RATE_LIMIT_CONFIG.TOOL_INVOCATIONS.MAX,
};

export function rejectBrowserOriginRequests(req: Request, res: Response, next: NextFunction): void {
  if (req.headers.origin) {
    res.status(403).json({ error: 'Cross-origin requests are not allowed for this endpoint' });
    return;
  }

  next();
}

export function createObservabilityHandler(): RequestHandler {
  return (req, res) => {
    const activeValue = req.query.active;
    const limitValue = req.query.limit;
    if (activeValue !== undefined && activeValue !== 'true' && activeValue !== 'false') {
      res.status(400).json({ error: 'active must be true or false' });
      return;
    }
    if (limitValue !== undefined && (typeof limitValue !== 'string' || !/^[1-9][0-9]*$/.test(limitValue))) {
      res.status(400).json({ error: 'limit must be a positive integer' });
      return;
    }
    const limit = limitValue === undefined ? 50 : Number(limitValue);
    if (!Number.isSafeInteger(limit) || limit > 200) {
      res.status(400).json({ error: 'limit must be between 1 and 200' });
      return;
    }
    res.setHeader('Cache-Control', 'no-store');
    res.json(getOperationSnapshot({ active: activeValue === 'true', limit }));
  };
}

export function lookupOperation(id: string): { status: 200 | 404; body: unknown } {
  const operation = getOperation(id);
  if (!operation) return { status: 404, body: { schemaVersion: 1, unknown: true, id } };
  const snapshot = getOperationSnapshot({ active: true, limit: 1 });
  return { status: 200, body: { schemaVersion: 1, instanceId: snapshot.instanceId, operation } };
}

export function createOperationLookupHandler(): RequestHandler {
  return (req, res) => {
    const result = lookupOperation(req.params.id);
    res.setHeader('Cache-Control', 'no-store');
    res.status(result.status).json(result.body);
  };
}

export function createApiRoutes(
  serverManager: ServerManager,
  scopeAuthMiddleware: RequestHandler,
  rateLimitPolicy: ToolInvocationRateLimitPolicy = DEFAULT_TOOL_INVOCATION_RATE_LIMIT_POLICY,
): Router {
  const router = Router();
  const toolInvocationLimiter = rateLimit({
    windowMs: rateLimitPolicy.windowMs,
    max: rateLimitPolicy.max,
    message: RATE_LIMIT_CONFIG.TOOL_INVOCATIONS.MESSAGE,
    standardHeaders: true,
    legacyHeaders: false,
  });

  router.get('/inspect', tagsExtractor, scopeAuthMiddleware, createInspectHandler(serverManager));
  router.get('/instructions', tagsExtractor, scopeAuthMiddleware, createInstructionsHandler(serverManager));
  router.get('/servers', tagsExtractor, scopeAuthMiddleware, createServersHandler(serverManager));
  router.get('/tools', tagsExtractor, scopeAuthMiddleware, createToolsHandler(serverManager));
  router.get('/observability', tagsExtractor, scopeAuthMiddleware, createObservabilityHandler());
  router.get('/observability/operations/:id', tagsExtractor, scopeAuthMiddleware, createOperationLookupHandler());
  router.post(
    '/tool-invocations',
    rejectBrowserOriginRequests,
    toolInvocationLimiter,
    tagsExtractor,
    scopeAuthMiddleware,
    createToolInvocationsHandler(serverManager),
  );

  return router;
}

export default createApiRoutes;
