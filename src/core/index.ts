import type { DaemonDiscovery, RingingV2CommandAck, RingingV2CommandEnvelope, RingingV2CommandStatus, RingingV2LeaseRenewResponse, RingingV2OpenResponse } from "../protocol/index.js";
import { CLIENT_SESSION_HEADER, RINGING_SCHEMA, RINGING_V2_BASE_PATH, RINGING_V2_VERSION } from "../protocol/index.js";
import type { ServiceMap } from "../protocol/index.js";

function createId(): string { return globalThis.crypto.randomUUID(); }
export type ClientState = "idle" | "connecting" | "ready" | "recovering" | "closed";
export class SdkError extends Error {
  constructor(readonly kind: "transport" | "timeout" | "cancelled" | "protocol" | "incompatible" | "authentication" | "authorization" | "command_rejected" | "outcome_unknown", message: string, readonly status?: number, readonly code?: string, readonly commandId?: string) { super(message); this.name = "SdkError"; }
}
export interface ClientOptions { credentials: DaemonDiscovery; fetch?: typeof fetch; timeoutMs?: number; logger?: Pick<Console, "warn" | "error"> }
export interface CommandSubmission { commandId: string; ack: RingingV2CommandAck }
export function createClient(options: ClientOptions) { return new RingingClient(options); }

export class RingingClient {
  private readonly fetcher: typeof fetch; private readonly timeoutMs: number;
  private stateValue: ClientState = "idle"; private readonly clientInstanceId = `sdk-${createId()}`;
  private clientSessionId: string | null = null; private openResponse: RingingV2OpenResponse | null = null;
  private leaseTimer: ReturnType<typeof setTimeout> | null = null; private connectFlight: Promise<RingingV2OpenResponse> | null = null;
  private readonly controllers = new Set<AbortController>();
  constructor(private readonly options: ClientOptions) { this.fetcher = options.fetch ?? fetch; this.timeoutMs = options.timeoutMs ?? 15000; }
  get state(): ClientState { return this.stateValue; } get epoch(): string | null { return this.openResponse?.server_epoch ?? null; }
  private get endpoint(): string { return this.options.credentials.endpoint.replace(/^ws:/, "http:").replace(/^wss:/, "https:").replace(/\/$/, ""); }
  async connect(): Promise<RingingV2OpenResponse> {
    if (this.stateValue === "ready" && this.openResponse) return this.openResponse;
    if (this.connectFlight) return this.connectFlight;
    if (this.stateValue === "closed") throw new SdkError("cancelled", "Client is closed");
    this.stateValue = "connecting";
    this.connectFlight = this.request<RingingV2OpenResponse>(`${RINGING_V2_BASE_PATH}/clients/open`, { method: "POST", body: JSON.stringify({ schema: RINGING_SCHEMA, version: RINGING_V2_VERSION, client_instance_id: this.clientInstanceId }) }, false).then(response => {
      if (response.schema !== RINGING_SCHEMA || response.version !== RINGING_V2_VERSION || !response.accepted || !response.client_session_id || !response.server_epoch) throw new SdkError("protocol", "Invalid Ringing v2 open response");
      this.openResponse = response; this.clientSessionId = response.client_session_id; this.stateValue = "ready"; this.scheduleRenew(response.renew_interval_ms); return response;
    }).catch(error => { this.stateValue = "idle"; throw error; }).finally(() => { this.connectFlight = null; });
    return this.connectFlight;
  }
  async service<K extends keyof ServiceMap>(method: K, params: ServiceMap[K]["params"]): Promise<ServiceMap[K]["result"]> {
    await this.ensureReady(); return this.request<ServiceMap[K]["result"]>(`${RINGING_V2_BASE_PATH}/service/${encodeURIComponent(method)}`, { method: "POST", body: JSON.stringify(params) });
  }
  async submit(channel: RingingV2CommandEnvelope["channel"], command: RingingV2CommandEnvelope["command"], sessionId: string | null = null, commandId = createId()): Promise<CommandSubmission> {
    await this.ensureReady();
    const envelope: RingingV2CommandEnvelope = { schema: RINGING_SCHEMA, version: RINGING_V2_VERSION, channel, command_id: commandId, client_instance_id: this.clientInstanceId, client_session_id: this.clientSessionId!, session_id: sessionId, command };
    const ack = await this.request<RingingV2CommandAck>(`${RINGING_V2_BASE_PATH}/commands/${channel}`, { method: "POST", body: JSON.stringify(envelope) });
    if (ack.command_id !== commandId) throw new SdkError("protocol", "Command acknowledgement ID mismatch", undefined, undefined, commandId);
    if (ack.status === "rejected") throw new SdkError("command_rejected", ack.message ?? ack.code ?? "Command rejected", undefined, ack.code ?? undefined, commandId);
    return { commandId, ack };
  }
  async commandStatus(commandId: string): Promise<RingingV2CommandStatus> { await this.ensureReady(); return this.request(`${RINGING_V2_BASE_PATH}/commands/${encodeURIComponent(commandId)}`, { method: "GET" }); }
  async disconnect(): Promise<void> { if (this.stateValue === "closed") return; this.stateValue = "closed"; if (this.leaseTimer) clearTimeout(this.leaseTimer); this.leaseTimer = null; for (const c of this.controllers) c.abort(); this.controllers.clear(); this.clientSessionId = null; }
  private async ensureReady(): Promise<void> { if (this.stateValue !== "ready") await this.connect(); }
  private scheduleRenew(ms: number): void {
    if (this.leaseTimer) clearTimeout(this.leaseTimer);
    this.leaseTimer = setTimeout(async () => { if (this.stateValue !== "ready") return; try { const result = await this.request<RingingV2LeaseRenewResponse>(`${RINGING_V2_BASE_PATH}/leases/renew`, { method: "POST", body: "{}" }); if (!result.ok) throw new SdkError("protocol", "Lease renewal rejected"); this.scheduleRenew(result.renew_interval_ms); } catch (error) { this.options.logger?.warn("Ringing lease renewal failed", error); if ((this.stateValue as ClientState) !== "closed") { this.stateValue = "recovering"; this.openResponse = null; this.clientSessionId = null; try { await this.connect(); } catch { this.stateValue = "idle"; } } } }, Math.max(1000, ms));
  }
  private async request<T>(path: string, init: RequestInit, authenticated = true): Promise<T> {
    const controller = new AbortController(); this.controllers.add(controller); const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try { const headers = new Headers(init.headers); headers.set("Content-Type", "application/json"); headers.set("Authorization", `Bearer ${this.options.credentials.token}`); if (authenticated && this.clientSessionId) headers.set(CLIENT_SESSION_HEADER, this.clientSessionId); const response = await this.fetcher(`${this.endpoint}${path}`, { ...init, headers, signal: controller.signal }); if (!response.ok) throw new SdkError(response.status === 401 ? "authentication" : response.status === 403 ? "authorization" : "transport", `Ringing request failed (${response.status})`, response.status); try { return await response.json() as T; } catch { throw new SdkError("protocol", "Invalid JSON response"); } }
    catch (error) { if (error instanceof SdkError) throw error; if (controller.signal.aborted) throw new SdkError("timeout", "Ringing request timed out"); throw new SdkError("transport", error instanceof Error ? error.message : "Network error"); }
    finally { clearTimeout(timer); this.controllers.delete(controller); }
  }
}
