import { readFile, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { RINGING_V2_VERSION, type DaemonDiscovery } from "../protocol/index.js";

export type Credentials =
  | { kind: "local-discovery"; path?: string }
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

function isAlive(pid: number): boolean {
  // signal 0 performs the permission/existence check without signalling; ESRCH
  // means gone, EPERM means alive but owned by another user.
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

  const path = credentials.path ?? join(homedir(), ".qaqh", "daemon.json");
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
  if (value.protocol_version !== RINGING_V2_VERSION) {
    throw new DiscoveryError("wrong_protocol", `Daemon speaks Ringing v${value.protocol_version}, SDK implements v${RINGING_V2_VERSION}`);
  }
  if ((options.checkPid ?? true) && !isAlive(value.pid)) {
    throw new DiscoveryError("dead_pid", `Daemon pid ${value.pid} is not running; discovery document is stale`);
  }
  return value as unknown as DaemonDiscovery;
}
