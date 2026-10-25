import { readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { DaemonDiscovery } from "../protocol/index.js";

export type Credentials =
  | { kind: "local-discovery"; path?: string; paths?: string[] }
  | { kind: "endpoint"; endpoint: string; token: string };

export interface DiscoveryOptions {
  /** Reject a discovery document whose mtime is older than this (stale daemon restart leftovers). */
  maxAgeMs?: number;
  /** Verify the recorded daemon pid is alive (default true); disable only for exotic namespaces. */
  checkPid?: boolean;
}

export class DiscoveryError extends Error {
  constructor(readonly code: "not_found" | "invalid_json" | "invalid_shape" | "stale" | "wrong_protocol" | "dead_pid" | "bad_endpoint", message: string) { super(message); this.name = "DiscoveryError"; }
}

/** Liveness probe via signal 0: ESRCH means gone, EPERM means alive but owned by another user. */
export function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}

function parseEndpoint(value: unknown): URL {
  if (typeof value !== "string") throw new DiscoveryError("invalid_shape", "Discovery endpoint must be a string");
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new DiscoveryError("bad_endpoint", "Discovery endpoint must use http(s)");
  return url;
}

export async function resolveCredentials(credentials: Credentials, options: DiscoveryOptions = {}): Promise<DaemonDiscovery> {
  if (credentials.kind === "endpoint") {
    parseEndpoint(credentials.endpoint);
    return { endpoint: credentials.endpoint, token: credentials.token } as DaemonDiscovery;
  }

  const paths = credentials.paths?.length
    ? credentials.paths
    : [credentials.path ?? join(homedir(), ".qaqh", "daemon.json")];
  const failures: DiscoveryError[] = [];
  for (const path of paths) {
    try {
      return await resolveDiscoveryFile(path, options);
    } catch (error) {
      if (!(error instanceof DiscoveryError)) throw error;
      failures.push(error);
    }
  }
  // Prefer the most specific failure (a document that exists but was rejected)
  // over a bare not_found when every candidate missed.
  throw failures.find((error) => error.code !== "not_found")
    ?? new DiscoveryError("not_found", `Daemon discovery document not found at any of: ${paths.join(", ")}`);
}

async function resolveDiscoveryFile(path: string, options: DiscoveryOptions): Promise<DaemonDiscovery> {
  let text: string; let mtimeMs: number;
  try {
    const [content, stats] = await Promise.all([readFile(path, "utf8"), stat(path)]);
    text = content; mtimeMs = stats.mtimeMs;
  } catch {
    throw new DiscoveryError("not_found", `Daemon discovery document not found at ${path}`);
  }
  if (options.maxAgeMs != null && Date.now() - mtimeMs > options.maxAgeMs) {
    throw new DiscoveryError("stale", `Daemon discovery document is older than ${options.maxAgeMs}ms`);
  }

  let discovery: unknown;
  try { discovery = JSON.parse(text); } catch { throw new DiscoveryError("invalid_json", "Daemon discovery document is not valid JSON"); }
  if (!discovery || typeof discovery !== "object") throw new DiscoveryError("invalid_shape", "Invalid daemon discovery document");
  const value = discovery as Record<string, unknown>;
  if (typeof value.endpoint !== "string" || typeof value.token !== "string" || typeof value.pid !== "number" || typeof value.protocol_version !== "number") {
    throw new DiscoveryError("invalid_shape", "Daemon discovery document is missing required fields");
  }
  parseEndpoint(value.endpoint);
  // discovery `protocol_version` is the control-plane version the daemon
  // publishes (CONTROL_PROTOCOL_VERSION in the backend) — NOT the Ringing v2
  // wire version, which is negotiated per-connection in `open` and hard
  // validated in `connect()`. Conflating the two here would reject every
  // real daemon (control version is 1 while the wire is v2).
  if (typeof value.protocol_version !== "number" || value.protocol_version <= 0) {
    throw new DiscoveryError("invalid_shape", "Daemon discovery document has an invalid protocol_version");
  }
  if ((options.checkPid ?? true) && !isAlive(value.pid)) {
    throw new DiscoveryError("dead_pid", `Daemon pid ${value.pid} is not running; discovery document is stale`);
  }
  return value as unknown as DaemonDiscovery;
}
