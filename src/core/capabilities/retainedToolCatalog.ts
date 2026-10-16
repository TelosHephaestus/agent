import { createHash, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

import ConfigContext from '@src/config/configContext.js';
import { isOperatorDisabledTemplateDefinition } from '@src/config/configuredServerTargets.js';
import type { MCPServerParams } from '@src/core/types/index.js';
import { writeLocalDiagnostic } from '@src/logger/localDiagnostics.js';
import { type Tool, toProtocolTool } from '@src/sdk/contracts/index.js';

import { z } from 'zod';

interface RetainedServer {
  fingerprint: string;
  tools: Tool[];
  protocolRevision?: string;
  pageSizes?: number[];
}

const retainedServerSchema = z
  .object({
    fingerprint: z.string().regex(/^[a-f0-9]{64}$/),
    tools: z.array(z.unknown()),
    protocolRevision: z.string().optional(),
    pageSizes: z.array(z.number().int().nonnegative()).min(1).optional(),
  })
  .refine(
    (entry) => !entry.pageSizes || entry.pageSizes.reduce((total, size) => total + size, 0) === entry.tools.length,
  );
const retainedFileSchema = z.object({
  version: z.literal(1),
  servers: z.record(z.string(), retainedServerSchema),
});

function fingerprint(config: MCPServerParams): string {
  return createHash('sha256')
    .update(
      JSON.stringify(config, (_key, value: unknown) =>
        value && typeof value === 'object' && !Array.isArray(value)
          ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)))
          : value,
      ),
    )
    .digest('hex');
}

/** Raw complete upstream lists only; request visibility and schema projection remain runtime-owned. */
export class RetainedToolCatalog {
  private servers = new Map<string, RetainedServer>();

  constructor(readonly path: string) {
    if (!existsSync(path)) return;
    try {
      const data = retainedFileSchema.parse(JSON.parse(readFileSync(path, 'utf8')));
      for (const [name, entry] of Object.entries(data.servers)) {
        this.servers.set(name, { ...entry, tools: entry.tools.map(toProtocolTool) });
      }
    } catch {
      this.servers.clear();
      writeLocalDiagnostic('warn', 'tool.retention-load-failed', {});
    }
  }

  reconcile(configs: Record<string, MCPServerParams>): void {
    let changed = false;
    for (const [name, entry] of this.servers) {
      const config = configs[name];
      if (config && !isOperatorDisabledTemplateDefinition(config) && entry.fingerprint === fingerprint(config))
        continue;
      this.servers.delete(name);
      changed = true;
    }
    if (changed) this.persist();
  }

  read(name: string): readonly Tool[] | undefined {
    return this.servers.get(name)?.tools;
  }

  readProtocolRevision(name: string): string | undefined {
    return this.servers.get(name)?.protocolRevision;
  }

  readPageSizes(name: string): readonly number[] | undefined {
    return this.servers.get(name)?.pageSizes;
  }

  publish(
    name: string,
    config: MCPServerParams,
    tools: readonly Tool[],
    protocolRevision?: string,
    pageSizes?: number[],
  ): void {
    if (isOperatorDisabledTemplateDefinition(config)) return;
    const entry = { fingerprint: fingerprint(config), tools: tools.map(toProtocolTool), protocolRevision, pageSizes };
    if (JSON.stringify(this.servers.get(name)) === JSON.stringify(entry)) return;
    this.servers.set(name, entry);
    this.persist();
  }

  private persist(): void {
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    try {
      mkdirSync(dirname(this.path), { recursive: true, mode: 0o700 });
      writeFileSync(temporary, JSON.stringify({ version: 1, servers: Object.fromEntries(this.servers) }), {
        mode: 0o600,
        flag: 'wx',
      });
      renameSync(temporary, this.path);
    } catch {
      writeLocalDiagnostic('warn', 'tool.retention-write-failed', {});
    } finally {
      try {
        if (existsSync(temporary)) unlinkSync(temporary);
      } catch {
        writeLocalDiagnostic('warn', 'tool.retention-cleanup-failed', {});
      }
    }
  }
}

const stores = new Map<string, RetainedToolCatalog>();
export function getRetainedToolCatalog(): RetainedToolCatalog {
  const path = join(
    dirname(ConfigContext.getInstance().getResolvedConfigPath()),
    'runtime',
    'retained-tool-catalog.json',
  );
  let store = stores.get(path);
  if (!store) {
    store = new RetainedToolCatalog(path);
    stores.set(path, store);
  }
  return store;
}
