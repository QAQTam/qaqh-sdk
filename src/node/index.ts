import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { DaemonDiscovery } from "../protocol/index.js";

export type Credentials =
  | { kind: "local-discovery"; path?: string }
  | { kind: "endpoint"; endpoint: string; token: string };

export async function resolveCredentials(credentials: Credentials): Promise<DaemonDiscovery> {
  if (credentials.kind === "endpoint") {
    const url = new URL(credentials.endpoint);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new TypeError("Explicit endpoint must use http(s)");
    }
    return { endpoint: credentials.endpoint, token: credentials.token } as DaemonDiscovery;
  }

  const path = credentials.path ?? join(homedir(), ".qaqh", "daemon.json");
  const text = await readFile(path, "utf8");
  const discovery: unknown = JSON.parse(text);
  if (!discovery || typeof discovery !== "object") throw new TypeError("Invalid daemon discovery document");
  const value = discovery as Record<string, unknown>;
  if (typeof value.endpoint !== "string" || typeof value.token !== "string" || typeof value.pid !== "number" || typeof value.protocol_version !== "number") {
    throw new TypeError("Daemon discovery document is missing required fields");
  }
  const endpoint = new URL(value.endpoint);
  if (endpoint.protocol !== "http:" && endpoint.protocol !== "https:") throw new TypeError("Invalid discovery endpoint protocol");
  return value as unknown as DaemonDiscovery;
}
