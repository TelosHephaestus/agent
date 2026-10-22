#!/usr/bin/env node
import { readFile } from 'node:fs/promises';

class OperatorError extends Error {}

async function readJson(response) {
  if (!response.body) throw new OperatorError('Gateway returned an empty response');
  const reader = response.body.getReader();
  const chunks = [];
  let bytes = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > 4 * 1024 * 1024) throw new OperatorError('Gateway response exceeds the operator budget');
      chunks.push(value);
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      throw new OperatorError('Gateway returned malformed JSON');
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function operationLine(operation) {
  return `${operation.id} ${operation.backend ?? '-'} ${operation.method} ${operation.stage} caller=${operation.callerOutcome ?? 'pending'} rpc=${operation.backendRequest} callbacks=${operation.outstandingCallbacks} private=${operation.privateConnections} callerMs=${Math.round(operation.elapsedMs)} ageMs=${Math.round(operation.ageMs)}`;
}

function args(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === '--active' || key === '--json') {
      out[key.slice(2)] = true;
      continue;
    }
    if (!['--url', '--credential-file', '--operation', '--limit'].includes(key) || !argv[i + 1])
      throw new OperatorError('Invalid or incomplete option');
    out[key.slice(2)] = argv[++i];
  }
  if (!out.url) throw new OperatorError('--url is required');
  const url = new URL(out.url);
  if (
    !['http:', 'https:'].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== '/' ||
    url.search ||
    url.hash
  )
    throw new OperatorError('--url must be a gateway HTTP origin without credentials or a path');
  if (out.limit && (!/^[1-9][0-9]*$/.test(out.limit) || Number(out.limit) > 200))
    throw new OperatorError('--limit must be 1 through 200');
  return { ...out, origin: url.origin };
}

async function main() {
  if (process.argv.slice(2).includes('--help')) {
    process.stdout.write(
      'node scripts/gateway-observability.mjs --url <gateway-origin> [--credential-file <json-file>] [--operation <id>] [--active] [--limit 1..200] [--json]\n',
    );
    return;
  }
  const options = args(process.argv.slice(2));
  let credentials = {};
  if (options['credential-file']) {
    try {
      credentials = JSON.parse(await readFile(options['credential-file'], 'utf8'));
    } catch {
      throw new OperatorError('Credential file is unreadable or invalid JSON');
    }
  }
  const token = credentials.token ?? credentials.bearer ?? credentials.access_token;
  if (options['credential-file'] && (typeof token !== 'string' || !token || /[\r\n]/.test(token)))
    throw new OperatorError('Credential file has no valid supported token field');
  const headers = { accept: 'application/json', ...(token ? { authorization: 'Bearer ' + token } : {}) };
  const base = options.origin;
  const path = options.operation
    ? `/api/v1/observability/operations/${encodeURIComponent(options.operation)}`
    : `/api/v1/observability?active=${Boolean(options.active)}&limit=${options.limit ?? 50}`;
  const response = await fetch(`${base}${path}`, { headers, signal: AbortSignal.timeout(10000), redirect: 'error' });
  if (!response.ok) {
    await response.body?.cancel().catch(() => undefined);
    throw new OperatorError(
      response.status === 404 && options.operation
        ? 'Operation is unknown or outside retained history'
        : `Gateway returned HTTP ${response.status}`,
    );
  }
  const data = await readJson(response);
  if (options.operation) {
    if (
      data.schemaVersion !== 1 ||
      !data.operation ||
      data.operation.id !== options.operation ||
      typeof data.instanceId !== 'string'
    )
      throw new OperatorError('Gateway returned an invalid operation response');
    const result = { instanceId: data.instanceId, operation: data.operation };
    process.stdout.write(
      options.json
        ? `${JSON.stringify(result, null, 2)}\n`
        : `Instance ${result.instanceId}\n${operationLine(result.operation)}\n`,
    );
    if (!options.json)
      for (const point of result.operation.timeline ?? [])
        process.stdout.write(
          `  ${point.at} ${point.kind} ${point.stage ?? ''} ${point.callerOutcome ?? ''} ${point.backendRequest ?? ''}\n`,
        );
  } else {
    if (
      data.schemaVersion !== 1 ||
      !Array.isArray(data.operations) ||
      !data.coverage ||
      !data.summary ||
      typeof data.instanceId !== 'string'
    )
      throw new OperatorError('Gateway returned an invalid observability response');
    try {
      const healthResponse = await fetch(`${base}/health/mcp`, {
        headers,
        signal: AbortSignal.timeout(10000),
        redirect: 'error',
      });
      if (!healthResponse.ok) {
        await healthResponse.body?.cancel();
        throw new OperatorError('Backend health unavailable');
      }
      const health = await readJson(healthResponse);
      if (
        !health.servers?.details ||
        typeof health.servers.details !== 'object' ||
        Array.isArray(health.servers.details)
      )
        throw new OperatorError('Backend health unavailable');
      const entries = Object.entries(health.servers.details);
      data.backendHealth = {
        available: true,
        total: entries.length,
        truncated: entries.length > 200,
        backends: entries
          .slice(0, 200)
          .map(([name, value]) => ({ name, state: value.state, retryCount: value.retryCount })),
      };
    } catch {
      data.backendHealth = { available: false };
    }
    process.stdout.write(
      options.json
        ? `${JSON.stringify(data, null, 2)}\n`
        : `Instance ${data.instanceId}\nLocal coverage ${data.coverage.complete ? 'complete' : 'incomplete'} (dropped ${data.coverage.dropped}, history evicted ${data.coverage.historyEvicted})\nOperations returned ${data.operations.length}; unfinished or uncertain ${data.summary.active}; caller-active ${data.summary.callerActive}\n`,
    );
    if (!options.json) {
      if (data.export)
        process.stdout.write(
          `Export ${data.export.enabled ? 'enabled' : 'disabled'}; queued ${data.export.queued}; dropped ${data.export.dropped}; acknowledged batches ${data.export.acceptedBatches}; last failure ${data.export.lastFailure ?? 'none'}\n`,
        );
      for (const operation of data.operations) process.stdout.write(`${operationLine(operation)}\n`);
      if (!data.backendHealth.available) process.stdout.write('Backend health unavailable\n');
      else
        for (const backend of data.backendHealth.backends)
          process.stdout.write(`Backend ${backend.name}: ${backend.state}; retries ${backend.retryCount}\n`);
    }
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof OperatorError ? error.message : 'Operator query failed'}\n`);
  process.exitCode = 1;
});
