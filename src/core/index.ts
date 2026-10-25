import type {
  ClientV2Bootstrap, ClientV2Event, ClientV2TeamResponse, CursorToken, DaemonDiscovery, JsonValue,
  RingingV2ApprovalRespondRequest, RingingV2CommandAck, RingingV2CommandEnvelope, RingingV2CommandStatus,
  RingingV2ContentUploadResponse, RingingV2DeviceChallenges, RingingV2DevicesResponse, RingingV2DriverClaimResponse,
  RingingV2DriverReleaseResponse, RingingV2LeaseRenewResponse, RingingV2OpenResponse, RingingV2PairRequest,
  RingingV2PairResponse, RingingV2PairTokenRequest, RingingV2PairTokenResponse, RingingV2PendingApprovals,
  RingingV2ResetRequired, TimelinePage, TimelineSseFrame,
} from "../protocol/index.js";
import { CLIENT_SESSION_HEADER, RINGING_SCHEMA, RINGING_V2_BASE_PATH, RINGING_V2_VERSION } from "../protocol/index.js";
import type { ServiceMap } from "../protocol/index.js";
import { SseDecoder } from "./sse.js";

export { SseDecoder } from "./sse.js";
export type { SseFrame } from "./sse.js";
export type { ClientV2Bootstrap } from "../protocol/index.js";

function createId(): string { return globalThis.crypto.randomUUID(); }
function backoff(attempt: number, initialMs: number, maxMs: number): number { return Math.min(maxMs, initialMs * 2 ** Math.min(attempt, 10)) + Math.floor(Math.random() * 250); }

export type ClientState = "idle" | "connecting" | "ready" | "recovering" | "closed";
export type WatchStatus = "connecting" | "live" | "reconnecting" | "paused" | "closed";

export class SdkError extends Error {
  constructor(readonly kind: "transport" | "timeout" | "cancelled" | "protocol" | "incompatible" | "authentication" | "authorization" | "command_rejected" | "outcome_unknown", message: string, readonly status?: number, readonly code?: string, readonly commandId?: string) { super(message); this.name = "SdkError"; }
}

export interface ClientOptions {
  credentials: DaemonDiscovery;
  fetch?: typeof fetch;
  timeoutMs?: number;
  logger?: Pick<Console, "warn" | "error">;
  /** Re-resolve credentials (e.g. re-read daemon.json) before a recovery reconnect. */
  refreshCredentials?: () => Promise<DaemonDiscovery>;
  /** SSE reconnect cadence; defaults to 1s initial / 15s max. */
  reconnect?: { initialMs?: number; maxMs?: number };
}

export interface CommandSubmission { commandId: string; ack: RingingV2CommandAck }
export type ApprovalsView = { kind: "admin"; pending: RingingV2PendingApprovals } | { kind: "device"; challenges: RingingV2DeviceChallenges };
export interface ContentDownload { bytes: Uint8Array; mediaType: string; status: number; contentRange: string | null }
export interface TimelinePageQuery { limit?: number; beforeIndex?: number }

export interface WatchOptions {
  /** Signal aborts the watch; both streams stop and the watch reports "closed". */
  signal?: AbortSignal;
  /** First-connection cursor (session: bootstrap `snapshot_cursor`; timeline: `Last-Event-ID`). */
  sinceCursor?: string;
  /** Skip the initial bootstrap fetch; the session watch then starts live at `sinceCursor` (or now). */
  skipBootstrap?: boolean;
}

export interface SessionWatchHandlers {
  onBootstrap?: (bootstrap: ClientV2Bootstrap) => void;
  onEvent?: (event: ClientV2Event) => void;
  onReset?: (reset: RingingV2ResetRequired) => void;
  onTerminated?: (info: { code: string; session_id: string; message: string }) => void;
  onDiagnostics?: (snapshot: JsonValue) => void;
  onStatus?: (status: WatchStatus) => void;
  onError?: (error: SdkError) => void;
}

export interface TimelineWatchHandlers {
  onEntry?: (frame: TimelineSseFrame) => void;
  onTerminated?: (info: { code: string; session_id: string; message: string }) => void;
  onStatus?: (status: WatchStatus) => void;
  onError?: (error: SdkError) => void;
}

export interface SessionWatch { close(): void; resync(): Promise<void>; readonly status: WatchStatus; readonly cursor: CursorToken | null; readonly serverEpoch: string | null }
export interface TimelineWatch { close(): void; readonly status: WatchStatus; readonly lastEventId: string | null }

export function createClient(options: ClientOptions) { return new RingingClient(options); }

const TERMINAL_COMMAND_STATES = new Set(["succeeded", "failed", "rejected"]);

interface RequestOverrides { authenticated?: boolean; bearer?: boolean; timeoutMs?: number; headers?: HeadersInit; contentType?: string | false; body?: BodyInit }

export class RingingClient {
  private readonly fetcher: typeof fetch; private readonly timeoutMs: number;
  private stateValue: ClientState = "idle"; private readonly clientInstanceId = `sdk-${createId()}`;
  private clientSessionId: string | null = null; private openResponse: RingingV2OpenResponse | null = null;
  private leaseTimer: ReturnType<typeof setTimeout> | null = null; private connectFlight: Promise<RingingV2OpenResponse> | null = null;
  private readonly controllers = new Set<AbortController>();
  private readonly stateListeners = new Set<(state: ClientState) => void>();
  constructor(private readonly options: ClientOptions) { this.fetcher = options.fetch ?? fetch; this.timeoutMs = options.timeoutMs ?? 15000; }

  get state(): ClientState { return this.stateValue; } get epoch(): string | null { return this.openResponse?.server_epoch ?? null; }
  get sessionId(): string | null { return this.clientSessionId; }
  onStateChange(listener: (state: ClientState) => void): () => void { this.stateListeners.add(listener); return () => { this.stateListeners.delete(listener); }; }
  private setState(state: ClientState): void { if (this.stateValue === state) return; this.stateValue = state; for (const listener of [...this.stateListeners]) { try { listener(state); } catch { /* listener errors must not break the client */ } } }

  private get endpoint(): string { return this.options.credentials.endpoint.replace(/^ws:/, "http:").replace(/^wss:/, "https:").replace(/\/$/, ""); }

  async connect(): Promise<RingingV2OpenResponse> {
    if (this.stateValue === "ready" && this.openResponse) return this.openResponse;
    if (this.connectFlight) return this.connectFlight;
    if (this.stateValue === "closed") throw new SdkError("cancelled", "Client is closed");
    this.setState("connecting");
    this.connectFlight = this.request<RingingV2OpenResponse>(`${RINGING_V2_BASE_PATH}/clients/open`, { method: "POST", body: JSON.stringify({ schema: RINGING_SCHEMA, version: RINGING_V2_VERSION, client_instance_id: this.clientInstanceId }), authenticated: false }).then(response => {
      if (response.schema !== RINGING_SCHEMA || response.version !== RINGING_V2_VERSION || !response.accepted || !response.client_session_id || !response.server_epoch) throw new SdkError("protocol", "Invalid Ringing v2 open response");
      this.openResponse = response; this.clientSessionId = response.client_session_id; this.setState("ready"); this.scheduleRenew(response.renew_interval_ms); return response;
    }).catch(error => { this.setState("idle"); throw error; }).finally(() => { this.connectFlight = null; });
    return this.connectFlight;
  }

  async service<K extends keyof ServiceMap>(method: K, params: ServiceMap[K]["params"]): Promise<ServiceMap[K]["result"]> {
    await this.ensureReady(); return this.request<ServiceMap[K]["result"]>(`${RINGING_V2_BASE_PATH}/service/${encodeURIComponent(method)}`, { method: "POST", body: JSON.stringify(params) });
  }

  async submit(channel: RingingV2CommandEnvelope["channel"], command: RingingV2CommandEnvelope["command"], sessionId: string | null = null, commandId = createId()): Promise<CommandSubmission> {
    await this.ensureReady();
    // RingingCommand is internally tagged by `channel` on the wire
    // (`{ channel, type, ... }`): stamp the tag from the envelope channel so
    // callers can pass the bare command. Stamping last makes a caller-supplied
    // tag unable to disagree with the envelope.
    const envelope: RingingV2CommandEnvelope = { schema: RINGING_SCHEMA, version: RINGING_V2_VERSION, channel, command_id: commandId, client_instance_id: this.clientInstanceId, client_session_id: this.clientSessionId!, session_id: sessionId, command: { ...command, channel } as RingingV2CommandEnvelope["command"] };
    try {
      const ack = await this.request<RingingV2CommandAck>(`${RINGING_V2_BASE_PATH}/commands/${channel}`, { method: "POST", body: JSON.stringify(envelope) });
      if (ack.command_id !== commandId) throw new SdkError("protocol", "Command acknowledgement ID mismatch", undefined, undefined, commandId);
      if (ack.status === "rejected") throw new SdkError("command_rejected", ack.message ?? ack.code ?? "Command rejected", undefined, ack.code ?? undefined, commandId);
      return { commandId, ack };
    } catch (error) {
      // A timeout (or transport failure) after the body was written leaves the
      // submission outcome unknown: the daemon may have accepted it. Attach the
      // command_id so callers can resolve it via commandStatus/idempotent resubmit.
      if (error instanceof SdkError && (error.kind === "timeout" || error.kind === "transport")) throw new SdkError(error.kind === "timeout" ? "outcome_unknown" : "transport", `${error.message} (command ${commandId} may still have been accepted; poll commandStatus or resubmit the same command_id)`, error.status, error.code, commandId);
      throw error;
    }
  }

  async commandStatus(commandId: string): Promise<RingingV2CommandStatus> { await this.ensureReady(); return this.request(`${RINGING_V2_BASE_PATH}/commands/${encodeURIComponent(commandId)}`, { method: "GET" }); }

  /** Poll commandStatus until a terminal state, the deadline, or abort. */
  async waitForCommand(commandId: string, options: { timeoutMs?: number; pollMs?: number; signal?: AbortSignal } = {}): Promise<RingingV2CommandStatus> {
    const deadline = Date.now() + (options.timeoutMs ?? 30000); const pollMs = options.pollMs ?? 400;
    for (;;) {
      if (options.signal?.aborted) throw new SdkError("cancelled", "waitForCommand aborted");
      const status = await this.commandStatus(commandId);
      if (TERMINAL_COMMAND_STATES.has(status.state)) return status;
      if (Date.now() + pollMs > deadline) throw new SdkError("timeout", `Command ${commandId} did not reach a terminal state within ${options.timeoutMs ?? 30000}ms`, undefined, undefined, commandId);
      await new Promise<void>(resolve => setTimeout(resolve, pollMs));
    }
  }

  /**
   * Close the loop on an `outcome_unknown` error from `submit`: poll
   * commandStatus for the attached command id until a terminal state. Errors
   * other than `outcome_unknown` (and ids missing from it) pass through
   * untouched; if the poll itself times out the original error is rethrown —
   * the outcome is exactly as unknown as before.
   */
  async resolveOutcome(error: unknown, options: { timeoutMs?: number; pollMs?: number } = {}): Promise<RingingV2CommandStatus> {
    if (!(error instanceof SdkError) || error.kind !== "outcome_unknown" || !error.commandId) throw error;
    try {
      return await this.waitForCommand(error.commandId, options);
    } catch (resolution) {
      if (resolution instanceof SdkError && resolution.kind === "timeout" && resolution.commandId === error.commandId) throw error;
      throw resolution;
    }
  }

  async bootstrap(sessionId: string): Promise<ClientV2Bootstrap> {
    await this.ensureReady();
    const bootstrap = await this.request<ClientV2Bootstrap>(this.sessionPath(sessionId, "/bootstrap"), { method: "GET" });
    if (bootstrap.schema !== RINGING_SCHEMA || bootstrap.version !== RINGING_V2_VERSION || bootstrap.session_id !== sessionId) throw new SdkError("protocol", "Invalid Ringing v2 bootstrap response");
    return bootstrap;
  }

  async teamSnapshot(sessionId: string): Promise<ClientV2TeamResponse> {
    await this.ensureReady();
    return this.request<ClientV2TeamResponse>(this.sessionPath(sessionId, "/team"), { method: "GET" });
  }

  async timelinePage(sessionId: string, query: TimelinePageQuery = {}): Promise<TimelinePage> {
    await this.ensureReady();
    const params = new URLSearchParams();
    if (query.limit != null) params.set("limit", String(query.limit));
    if (query.beforeIndex != null) params.set("before_index", String(query.beforeIndex));
    const suffix = params.size ? `?${params}` : "";
    const page = await this.request<TimelinePage>(this.sessionPath(sessionId, `/timeline${suffix}`), { method: "GET" });
    if (page.schema !== RINGING_SCHEMA || page.version !== RINGING_V2_VERSION || page.session_id !== sessionId) throw new SdkError("protocol", "Invalid Ringing v2 timeline page");
    return page;
  }

  /** Pending approvals; the wire shape depends on the token scope (admin sees canonical ids, devices get opaque challenges). */
  async approvals(sessionId: string): Promise<ApprovalsView> {
    await this.ensureReady();
    const body = await this.request<Record<string, unknown>>(this.sessionPath(sessionId, "/approvals"), { method: "GET" });
    if (body && typeof body === "object" && "challenges" in body) return { kind: "device", challenges: body as unknown as RingingV2DeviceChallenges };
    if (body && typeof body === "object" && "pending_permission" in body) return { kind: "admin", pending: body as unknown as RingingV2PendingApprovals };
    throw new SdkError("protocol", "Unrecognised approvals response shape");
  }

  /** Device-side approval answer (consumes an opaque challenge). Admin hosts answer via command submit instead. */
  async respondApproval(sessionId: string, request: RingingV2ApprovalRespondRequest): Promise<RingingV2CommandAck> {
    await this.ensureReady();
    return this.request<RingingV2CommandAck>(this.sessionPath(sessionId, "/approvals/respond"), { method: "POST", body: JSON.stringify(request) });
  }

  async claimDriver(sessionId: string): Promise<RingingV2DriverClaimResponse> {
    await this.ensureReady();
    return this.request<RingingV2DriverClaimResponse>(this.sessionPath(sessionId, "/driver/claim"), { method: "POST", body: "{}" });
  }

  async releaseDriver(sessionId: string): Promise<RingingV2DriverReleaseResponse> {
    await this.ensureReady();
    return this.request<RingingV2DriverReleaseResponse>(this.sessionPath(sessionId, "/driver/release"), { method: "POST", body: "{}" });
  }

  /** Upload raw bytes; the daemon wire is multipart/form-data with parts `session_id`, `media_type`, `content`. */
  async uploadContent(sessionId: string, content: { bytes: Uint8Array; mediaType: string }): Promise<RingingV2ContentUploadResponse> {
    await this.ensureReady();
    const boundary = `----qaqh-sdk-${createId()}`;
    const field = (name: string, value: string) => `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${value}\r\n`;
    const head = new TextEncoder().encode(`${field("session_id", sessionId)}${field("media_type", content.mediaType)}--${boundary}\r\nContent-Disposition: form-data; name="content"; filename="blob"\r\nContent-Type: ${content.mediaType}\r\n\r\n`);
    const tail = new TextEncoder().encode(`\r\n--${boundary}--\r\n`);
    const body = new Uint8Array(head.length + content.bytes.length + tail.length);
    body.set(head, 0); body.set(content.bytes, head.length); body.set(tail, head.length + content.bytes.length);
    return this.request<RingingV2ContentUploadResponse>(`${RINGING_V2_BASE_PATH}/content`, { method: "POST", body, contentType: `multipart/form-data; boundary=${boundary}` });
  }

  /** Download content by id (`sha256:<hex>` or bare hex); optional single RFC 9110 byte range. */
  async downloadContent(contentId: string, options: { range?: string; signal?: AbortSignal } = {}): Promise<ContentDownload> {
    await this.ensureReady();
    const headers: Record<string, string> = {};
    if (options.range) headers.Range = options.range;
    const response = await this.raw(`${RINGING_V2_BASE_PATH}/content/${encodeURIComponent(contentId)}`, { method: "GET", headers, signal: options.signal });
    if (!response.ok) throw await this.errorFrom(response);
    const bytes = new Uint8Array(await response.arrayBuffer());
    return { bytes, mediaType: response.headers.get("Content-Type") ?? "application/octet-stream", status: response.status, contentRange: response.headers.get("Content-Range") };
  }

  async listDevices(): Promise<RingingV2DevicesResponse> { await this.ensureReady(); return this.request(`${RINGING_V2_DEVICES_PATH}`, { method: "GET" }); }
  async revokeDevice(deviceId: string): Promise<void> {
    await this.ensureReady();
    const response = await this.raw(`${RINGING_V2_DEVICES_PATH}/${encodeURIComponent(deviceId)}/revoke`, { method: "POST" });
    if (!response.ok) throw await this.errorFrom(response);
  }

  async issuePairToken(request: RingingV2PairTokenRequest): Promise<RingingV2PairTokenResponse> {
    return this.request<RingingV2PairTokenResponse>(`${RINGING_V2_BASE_PATH}/pairing/tokens`, { method: "POST", body: JSON.stringify(request) });
  }

  /** Consume a one-time pairing token; no Bearer credentials are involved. */
  async pair(request: RingingV2PairRequest): Promise<RingingV2PairResponse> {
    return this.request<RingingV2PairResponse>(`${RINGING_V2_BASE_PATH}/pair`, { method: "POST", body: JSON.stringify(request), authenticated: false, bearer: false });
  }

  /**
   * Bootstrap + live session watch on the per-seed single SSE stream.
   * Deltas are demuxed by `stream_key` by the caller; the watch tracks the
   * cursor and reconnects with `?since_cursor=` (no replay gap) on transport
   * drops. `reset_required` and `stream_terminated` pause the watch — the
   * caller decides whether to `resync()` (re-bootstrap) or give up.
   */
  watchSession(sessionId: string, handlers: SessionWatchHandlers, options: WatchOptions = {}): SessionWatch {
    const initial = options.sinceCursor ?? null; let cursor: CursorToken | null = initial; let serverEpoch: string | null = null;
    const setStatus = trackStatus(handlers);
    const signal = options.signal ?? new AbortController().signal;
    let closed = false; let paused = false; let attempt = 0; let timer: ReturnType<typeof setTimeout> | null = null;
    const controller = new AbortController();
    const onAbort = () => { controller.abort(); }; if (options.signal) options.signal.addEventListener("abort", onAbort, { once: true });
    const stop = (status: WatchStatus) => { closed = true; paused = true; if (timer) clearTimeout(timer); controller.abort(); if (options.signal) options.signal.removeEventListener("abort", onAbort); setStatus.set(status); };
    const resync = async () => {
      if (closed) throw new SdkError("cancelled", "Watch is closed");
      paused = false; attempt = 0;
      if (!options.skipBootstrap) {
        setStatus.set("connecting");
        const bootstrap = await this.bootstrap(sessionId);
        serverEpoch = bootstrap.server_epoch; cursor = bootstrap.snapshot_cursor ?? cursor;
        handlers.onBootstrap?.(bootstrap);
      }
      await pump();
    };
    const pump = async (): Promise<void> => {
      if (closed || paused) return;
      setStatus.set(attempt === 0 ? "connecting" : "reconnecting");
      const query = cursor ? `?since_cursor=${encodeURIComponent(cursor)}` : "";
      try {
        const response = await this.raw(this.sessionPath(sessionId, `/events${query}`), { method: "GET", headers: { Accept: "text/event-stream" }, signal: controller.signal, timeoutMs: Infinity, authenticated: true });
        if (!response.ok || !response.body) throw await this.errorFrom(response);
        attempt = 0; setStatus.set("live");
        const decoder = new SseDecoder(); const reader = response.body.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          for (const frame of decoder.push(new TextDecoder().decode(value, { stream: true }))) {
            if (frame.event === "ringing.event") {
              const envelope = JSON.parse(frame.data) as ClientV2Event;
              if (envelope.schema !== RINGING_SCHEMA || envelope.version !== RINGING_V2_VERSION || envelope.session_id !== sessionId) throw new SdkError("protocol", "Invalid Ringing v2 event envelope");
              if (serverEpoch !== null && envelope.server_epoch !== serverEpoch) { serverEpoch = envelope.server_epoch; cursor = null; throw new SdkError("protocol", `Server epoch changed to ${envelope.server_epoch}; cursor invalidated`); }
              if (envelope.cursor) cursor = envelope.cursor;
              handlers.onEvent?.(envelope);
            } else if (frame.event === "ringing.reset_required") {
              const reset = JSON.parse(frame.data) as RingingV2ResetRequired;
              cursor = reset.snapshot_cursor ?? null; paused = true;
              handlers.onReset?.(reset); setStatus.set("paused");
              return;
            } else if (frame.event === "ringing.stream_terminated") {
              const info = JSON.parse(frame.data) as { code: string; session_id: string; message: string };
              paused = true; handlers.onTerminated?.(info); setStatus.set("paused");
              return;
            } else if (frame.event === "diagnostics.memory") {
              handlers.onDiagnostics?.(JSON.parse(frame.data) as JsonValue);
            }
          }
        }
      } catch (error) {
        if (closed || controller.signal.aborted) return;
        handlers.onError?.(error instanceof SdkError ? error : new SdkError("transport", error instanceof Error ? error.message : "watch stream failed"));
      }
      if (closed || paused) return;
      const { initialMs = 1000, maxMs = 15000 } = this.options.reconnect ?? {};
      const delay = backoff(attempt++, initialMs, maxMs);
      setStatus.set("reconnecting");
      timer = setTimeout(() => { void pump(); }, delay);
    };
    void resync().catch(error => { handlers.onError?.(error instanceof SdkError ? error : new SdkError("transport", error instanceof Error ? error.message : "watch failed")); setStatus.set("paused"); });
    return {
      close: () => stop("closed"),
      resync: async () => { await resync(); },
      get status() { return setStatus.get(); }, get cursor() { return cursor; }, get serverEpoch() { return serverEpoch; },
    };
  }

  /**
   * Live timeline watch on `.../timeline/events`. Resume works via the SSE
   * `Last-Event-ID` header (`{epoch}:timeline:{seq}`), which the daemon replays
   * from. Transport drops reconnect with the same id; termination frames pause.
   */
  watchTimeline(sessionId: string, handlers: TimelineWatchHandlers, options: WatchOptions = {}): TimelineWatch {
    let lastEventId = options.sinceCursor ?? null;
    const setStatus = trackStatus(handlers);
    const controller = new AbortController();
    const onAbort = () => { controller.abort(); }; if (options.signal) options.signal.addEventListener("abort", onAbort, { once: true });
    let closed = false; let paused = false; let attempt = 0; let timer: ReturnType<typeof setTimeout> | null = null;
    const stop = (status: WatchStatus) => { closed = true; paused = true; if (timer) clearTimeout(timer); controller.abort(); if (options.signal) options.signal.removeEventListener("abort", onAbort); setStatus.set(status); };
    const pump = async (): Promise<void> => {
      if (closed || paused) return;
      setStatus.set(attempt === 0 ? "connecting" : "reconnecting");
      const headers: Record<string, string> = { Accept: "text/event-stream" };
      if (lastEventId) headers["Last-Event-ID"] = lastEventId;
      try {
        const response = await this.raw(this.sessionPath(sessionId, "/timeline/events"), { method: "GET", headers, signal: controller.signal, timeoutMs: Infinity });
        if (!response.ok || !response.body) throw await this.errorFrom(response);
        attempt = 0; setStatus.set("live");
        const decoder = new SseDecoder(); const reader = response.body.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          for (const frame of decoder.push(new TextDecoder().decode(value, { stream: true }))) {
            if (frame.event === "timeline.entry") {
              const payload = JSON.parse(frame.data) as TimelineSseFrame;
              if (payload.schema !== RINGING_SCHEMA || payload.version !== RINGING_V2_VERSION || payload.session_id !== sessionId) throw new SdkError("protocol", "Invalid Ringing v2 timeline SSE frame");
              if (frame.id) lastEventId = frame.id;
              handlers.onEntry?.(payload);
            } else if (frame.event === "ringing.stream_terminated") {
              const info = JSON.parse(frame.data) as { code: string; session_id: string; message: string };
              paused = true; handlers.onTerminated?.(info); setStatus.set("paused");
              return;
            }
          }
        }
      } catch (error) {
        if (closed || controller.signal.aborted) return;
        handlers.onError?.(error instanceof SdkError ? error : new SdkError("transport", error instanceof Error ? error.message : "timeline stream failed"));
      }
      if (closed || paused) return;
      const { initialMs = 1000, maxMs = 15000 } = this.options.reconnect ?? {};
      const delay = backoff(attempt++, initialMs, maxMs);
      setStatus.set("reconnecting");
      timer = setTimeout(() => { void pump(); }, delay);
    };
    void pump();
    return { close: () => stop("closed"), get status() { return setStatus.get(); }, get lastEventId() { return lastEventId; } };
  }

  async disconnect(): Promise<void> { if (this.stateValue === "closed") return; this.setState("closed"); if (this.leaseTimer) clearTimeout(this.leaseTimer); this.leaseTimer = null; for (const c of this.controllers) c.abort(); this.controllers.clear(); this.clientSessionId = null; }

  private async ensureReady(): Promise<void> { if (this.stateValue !== "ready") await this.connect(); }

  private sessionPath(sessionId: string, suffix: string): string { return `${RINGING_V2_BASE_PATH}/sessions/${encodeURIComponent(sessionId)}${suffix}`; }

  private scheduleRenew(ms: number): void {
    if (this.leaseTimer) clearTimeout(this.leaseTimer);
    this.leaseTimer = setTimeout(async () => {
      if (this.stateValue !== "ready") return;
      try {
        const result = await this.request<RingingV2LeaseRenewResponse>(`${RINGING_V2_BASE_PATH}/leases/renew`, { method: "POST", body: "{}" });
        if (!result.ok) throw new SdkError("protocol", "Lease renewal rejected");
        this.scheduleRenew(result.renew_interval_ms);
      } catch (error) {
        this.options.logger?.warn("Ringing lease renewal failed", error);
        if ((this.stateValue as ClientState) !== "closed") {
          this.setState("recovering"); this.openResponse = null; this.clientSessionId = null;
          try { await this.options.refreshCredentials?.().then(credentials => { this.options.credentials = credentials; }); } catch (refreshError) { this.options.logger?.warn("Credential refresh failed", refreshError); }
          try { await this.connect(); } catch { this.setState("idle"); }
        }
      }
    }, Math.max(1000, ms));
  }

  private async request<T>(path: string, init: { method: string; body?: BodyInit } & RequestOverrides): Promise<T> {
    const response = await this.raw(path, init);
    if (!response.ok) throw await this.errorFrom(response);
    try { return await response.json() as T; } catch { throw new SdkError("protocol", "Invalid JSON response"); }
  }

  /** Fetch with auth headers, per-call timeout, and controller bookkeeping; returns the raw Response. */
  private async raw(path: string, init: { method: string; body?: BodyInit; headers?: HeadersInit; contentType?: string | false; authenticated?: boolean; bearer?: boolean; timeoutMs?: number; signal?: AbortSignal }): Promise<Response> {
    const controller = new AbortController(); this.controllers.add(controller);
    const timeout = init.timeoutMs ?? this.timeoutMs;
    // Only arm a timer for finite timeouts: setTimeout(_, Infinity) clamps to
    // 1ms on Node and browsers, aborting long-lived SSE streams immediately.
    const timer = Number.isFinite(timeout) ? setTimeout(() => controller.abort(), timeout) : null;
    const done = () => { if (timer) clearTimeout(timer); this.controllers.delete(controller); };
    try {
      const headers = new Headers(init.headers);
      if (init.contentType !== false) headers.set("Content-Type", init.contentType ?? "application/json");
      if (init.bearer !== false) headers.set("Authorization", `Bearer ${this.options.credentials.token}`);
      if ((init.authenticated ?? true) && this.clientSessionId) headers.set(CLIENT_SESSION_HEADER, this.clientSessionId);
      if (init.signal) { if (init.signal.aborted) controller.abort(); else init.signal.addEventListener("abort", () => controller.abort(), { once: true }); }
      const response = await this.fetcher(`${this.endpoint}${path}`, { method: init.method, headers, body: init.body, signal: controller.signal });
      done();
      return response;
    } catch (error) {
      done();
      if (error instanceof SdkError) throw error;
      if (controller.signal.aborted) throw new SdkError("timeout", "Ringing request timed out");
      throw new SdkError("transport", error instanceof Error ? error.message : "Network error");
    }
  }

  /** Map a non-2xx response onto SdkError, preferring the daemon's `{code, message}` JSON body. */
  private async errorFrom(response: Response): Promise<SdkError> {
    let code: string | undefined; let message = `Ringing request failed (${response.status})`;
    try { const body = await response.json() as { code?: string; message?: string; error?: string }; code = body.code; if (body.message ?? body.error) message = `${message}: ${(body.message ?? body.error) as string}`; } catch { /* non-JSON error body */ }
    const kind = response.status === 401 ? "authentication" : response.status === 403 ? "authorization" : response.status === 404 ? "transport" : "transport";
    return new SdkError(kind, message, response.status, code);
  }
}

const RINGING_V2_DEVICES_PATH = `${RINGING_V2_BASE_PATH}/devices`;

function trackStatus(handlers: { onStatus?: (status: WatchStatus) => void }) {
  let status: WatchStatus = "connecting";
  return {
    set(next: WatchStatus) { if (status === next) return; status = next; handlers.onStatus?.(next); },
    get() { return status; },
  };
}
