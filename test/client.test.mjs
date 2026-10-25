import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { mkdtemp, writeFile, rm, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient, SdkError } from "../dist/core/index.js";
import { resolveCredentials, DiscoveryError } from "../dist/node/index.js";

const fixtures = new URL("../src/protocol/generated/fixtures/", import.meta.url);
const fixture = async (name) => JSON.parse(await readFile(new URL(name, fixtures), "utf8"));
const SEED = "0199a0f0-0000-7000-8000-000000000001";
const credentials = { endpoint: "http://127.0.0.1:8787", token: "fixture-token", pid: 1, protocol_version: 2 };

function jsonResponse(body, status = 200) { return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }); }
function sseResponse(chunks, { holdOpen = false } = {}) {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(chunks));
      if (!holdOpen) controller.close();
    },
  });
  return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}
function frame(event, data, id) {
  return `${id ? `id: ${id}\n` : ""}event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function mockBackend() {
  const routes = [];
  const calls = [];
  const impl = async (url, init) => {
    const target = String(url); const method = init?.method ?? "GET";
    calls.push({ url: target, method, init });
    // Behave like real fetch: an aborted signal must reject even if the route
    // handler never settles.
    const aborted = new Promise((_, reject) => {
      if (init?.signal?.aborted) reject(new DOMException("aborted", "AbortError"));
      else if (init?.signal) init.signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    });
    for (const route of routes) if (route.method === method && route.pattern.test(target)) return Promise.race([route.handler(target, init), aborted]);
    return new Response("no route", { status: 404 });
  };
  return { impl, calls, routes, add(method, pattern, handler) { routes.push({ method, pattern, handler }); } };
}

async function connectedClient(mock, overrides = {}) {
  const open = await fixture("open-response.json");
  mock.add("POST", /\/clients\/open$/, () => jsonResponse(open));
  const client = createClient({ credentials, fetch: mock.impl, ...overrides });
  await client.connect();
  return { client, open };
}

test("connect negotiates a lease, tracks state, and single-flights", async () => {
  const mock = mockBackend(); const open = await fixture("open-response.json");
  let opens = 0;
  mock.add("POST", /\/clients\/open$/, () => { opens++; return jsonResponse(open); });
  const client = createClient({ credentials, fetch: mock.impl });
  const states = []; client.onStateChange(state => states.push(state));
  const [a, b] = await Promise.all([client.connect(), client.connect()]);
  assert.equal(a, b); assert.equal(opens, 1);
  assert.equal(client.state, "ready"); assert.equal(client.sessionId, "client-session-fixture"); assert.equal(client.epoch, "fixture-epoch");
  assert.deepEqual(states, ["connecting", "ready"]);
  const openCall = mock.calls.find(call => call.url.endsWith("/clients/open"));
  assert.equal(JSON.parse(openCall.init.body).schema, "qaqh.Ringing");
  assert.equal(JSON.parse(openCall.init.body).version, 2);
  client.disconnect();
});

test("service calls carry bearer + client session headers and return typed results", async () => {
  const mock = mockBackend();
  mock.add("POST", /\/service\/daemon\.version$/, () => jsonResponse("2.0.0-fixture"));
  const { client } = await connectedClient(mock);
  const version = await client.service("daemon.version", {});
  assert.equal(version, "2.0.0-fixture");
  const call = mock.calls.at(-1);
  assert.equal(call.init.headers.get("Authorization"), "Bearer fixture-token");
  assert.equal(call.init.headers.get("X-QAQH-Client-Session-Id"), "client-session-fixture");
  client.disconnect();
});

test("submit rejects mismatched ack ids and surfaces daemon rejections", async () => {
  const mock = mockBackend();
  const wrongAck = { command_id: "not-mine", status: "accepted" };
  mock.add("POST", /\/commands\/control$/, () => jsonResponse(wrongAck));
  const { client } = await connectedClient(mock);
  await assert.rejects(() => client.submit("control", { type: "session_close" }, SEED), (error) => error instanceof SdkError && error.kind === "protocol");
  const rejectedAck = { command_id: "ok", status: "rejected", code: "not_driver", message: "another client holds the driver seat" };
  mock.routes.find(route => route.pattern.test("/x/commands/control")).handler = () => jsonResponse(rejectedAck);
  await assert.rejects(() => client.submit("control", { type: "session_close" }, SEED, "ok"), (error) => error instanceof SdkError && error.kind === "command_rejected" && error.code === "not_driver" && error.commandId === "ok");
  client.disconnect();
});

test("submit timeout maps to outcome_unknown with the command id", async () => {
  const mock = mockBackend();
  mock.add("POST", /\/commands\/control$/, () => new Promise(() => { /* never resolves */ }));
  const { client } = await connectedClient(mock, { timeoutMs: 150 });
  await assert.rejects(() => client.submit("control", { type: "session_close" }, SEED, "cmd-timeout-1"), (error) => {
    return error instanceof SdkError && error.kind === "outcome_unknown" && error.commandId === "cmd-timeout-1";
  });
  client.disconnect();
});

test("error responses surface status, code, and message from the daemon body", async () => {
  const mock = mockBackend();
  mock.add("GET", /\/devices$/, () => jsonResponse({ code: "admin_required", message: "admin scope required" }, 403));
  const { client } = await connectedClient(mock);
  await assert.rejects(() => client.listDevices(), (error) => error instanceof SdkError && error.kind === "authorization" && error.status === 403 && error.code === "admin_required" && /admin scope required/.test(error.message));
  client.disconnect();
});

test("waitForCommand polls until a terminal state", async () => {
  const mock = mockBackend();
  let polls = 0;
  mock.add("GET", /\/commands\/cmd-poll-1$/, () => jsonResponse(polls++ < 2 ? { command_id: "cmd-poll-1", state: "running", payload_fingerprint: "x" } : { command_id: "cmd-poll-1", state: "succeeded", payload_fingerprint: "x" }));
  const { client } = await connectedClient(mock);
  const status = await client.waitForCommand("cmd-poll-1", { pollMs: 5 });
  assert.equal(status.state, "succeeded"); assert.equal(polls, 3);
  client.disconnect();
});

test("approvals discriminates admin pending view from device challenges", async () => {
  const mock = mockBackend();
  const pending = await fixture("approvals-pending.json");
  const challenges = await fixture("device-challenges.json");
  let shape = "admin";
  mock.add("GET", /\/approvals$/, () => jsonResponse(shape === "admin" ? pending : challenges));
  const { client } = await connectedClient(mock);
  const admin = await client.approvals(SEED);
  assert.equal(admin.kind, "admin"); assert.equal(admin.pending.pending_permission.tool_name, "fs.write");
  shape = "device";
  const device = await client.approvals(SEED);
  assert.equal(device.kind, "device"); assert.equal(device.challenges.challenges.length, 2);
  client.disconnect();
});

test("respondApproval posts the challenge answer to the device endpoint", async () => {
  const mock = mockBackend();
  const ack = { command_id: "ack-1", status: "accepted" };
  mock.add("POST", /\/approvals\/respond$/, () => jsonResponse(ack));
  const { client } = await connectedClient(mock);
  const result = await client.respondApproval(SEED, { challenge_id: "c1", decision: "allow", payload: null });
  assert.deepEqual(result, ack);
  assert.deepEqual(JSON.parse(mock.calls.at(-1).init.body), { challenge_id: "c1", decision: "allow", payload: null });
  client.disconnect();
});

test("driver claim surfaces a busy seat without throwing", async () => {
  const mock = mockBackend();
  const busy = await fixture("driver-claim-busy.json");
  mock.add("POST", /\/driver\/claim$/, () => jsonResponse(busy));
  const { client } = await connectedClient(mock);
  const claim = await client.claimDriver(SEED);
  assert.equal(claim.accepted, false); assert.equal(claim.reason, "driver_busy"); assert.equal(claim.holder, "other-client-fixture");
  client.disconnect();
});

test("uploadContent speaks the daemon multipart wire and parses the receipt", async () => {
  const mock = mockBackend();
  const receipt = await fixture("content-upload-response.json");
  mock.add("POST", /\/content$/, (_url, init) => {
    const contentType = init.headers.get("Content-Type");
    const boundary = /boundary=(.+)$/.exec(contentType)?.[1];
    assert.ok(boundary, "multipart boundary required");
    // Mirror the daemon parser: split on the boundary, harvest named parts.
    const bytes = new Uint8Array(init.body); const text = new TextDecoder().decode(bytes);
    const delimiter = `--${boundary}`;
    const parts = text.split(delimiter).slice(1, -1);
    const fields = {};
    for (const part of parts) {
      const clean = part.replace(/^\r\n/, "").replace(/\r\n$/, "");
      const headerEnd = clean.indexOf("\r\n\r\n"); if (headerEnd < 0) continue;
      const name = /name="([^"]+)"/.exec(clean.slice(0, headerEnd))?.[1];
      if (name) fields[name] = clean.slice(headerEnd + 4);
    }
    assert.equal(fields.session_id, SEED);
    assert.equal(fields.media_type, "image/png");
    assert.equal(fields.content, "\x89PNG-fixture-bytes");
    return jsonResponse(receipt);
  });
  const { client } = await connectedClient(mock);
  const upload = await client.uploadContent(SEED, { bytes: new TextEncoder().encode("\x89PNG-fixture-bytes"), mediaType: "image/png" });
  assert.equal(upload.content_id, receipt.content_id); assert.equal(upload.truncated, false);
  client.disconnect();
});

test("downloadContent returns bytes, media type, and honors ranges", async () => {
  const mock = mockBackend();
  mock.add("GET", /\/content\/sha256%3A[0-9a-f]{16}$/, (url, init) => {
    assert.equal(init.headers.get("Range"), "bytes=0-3");
    return new Response(new Uint8Array([1, 2, 3, 4]), { status: 206, headers: { "Content-Type": "image/png", "Content-Range": "bytes 0-3/8" } });
  });
  const { client } = await connectedClient(mock);
  const download = await client.downloadContent("sha256:0011223344556677", { range: "bytes=0-3" });
  assert.deepEqual([...download.bytes], [1, 2, 3, 4]); assert.equal(download.mediaType, "image/png"); assert.equal(download.contentRange, "bytes 0-3/8"); assert.equal(download.status, 206);
  client.disconnect();
});

test("timelinePage validates the wire envelope and forwards pagination", async () => {
  const mock = mockBackend();
  const page = await fixture("timeline-page.json");
  mock.add("GET", /\/timeline\?/, (url) => { assert.ok(/limit=30/.test(url) && /before_index=5/.test(url)); return jsonResponse(page); });
  const { client } = await connectedClient(mock);
  const result = await client.timelinePage(SEED, { limit: 30, beforeIndex: 5 });
  assert.equal(result.session_id, SEED); assert.equal(result.has_more, false);
  client.disconnect();
});

test("bootstrap validates schema/version/session and returns the snapshot cursor", async () => {
  const mock = mockBackend();
  const bootstrap = await fixture("bootstrap-response.json");
  mock.add("GET", /\/bootstrap$/, () => jsonResponse(bootstrap));
  const { client } = await connectedClient(mock);
  const result = await client.bootstrap(SEED);
  assert.equal(result.server_epoch, "fixture-epoch"); assert.ok(result.snapshot_cursor);
  client.disconnect();
});

function envelope(overrides = {}) {
  return { schema: "qaqh.Ringing", version: 2, server_epoch: "fixture-epoch", session_id: SEED, event_id: "ev-1", stream_key: { kind: "channel", data: "control" }, delivery: "reliable", cursor: "cursor-1", payload: { kind: "control_delta", data: { session_id: SEED, revision: 1 } }, ...overrides };
}

test("watchSession bootstraps, demuxes events, tracks the cursor, and resumes after drops", async () => {
  const mock = mockBackend();
  const bootstrap = await fixture("bootstrap-response.json");
  mock.add("GET", /\/bootstrap$/, () => jsonResponse(bootstrap));
  const seenCursors = [];
  let connections = 0;
  mock.add("GET", /\/events/, (url) => {
    connections++;
    seenCursors.push(new URL(url).searchParams.get("since_cursor"));
    if (connections === 1) return sseResponse(frame("ringing.event", envelope()));
    return sseResponse(frame("ringing.event", envelope({ event_id: "ev-2", cursor: "cursor-2" })));
  });
  const { client } = await connectedClient(mock);
  const statuses = []; const events = []; let gotBootstrap = false;
  const watch = client.watchSession(SEED, {
    onBootstrap: () => { gotBootstrap = true; },
    onEvent: (event) => events.push(event),
    onStatus: (status) => statuses.push(status),
  });
  await waitUntil(() => events.length >= 2);
  assert.ok(gotBootstrap);
  assert.equal(events[0].event_id, "ev-1"); assert.equal(events[1].event_id, "ev-2");
  assert.equal(watch.cursor, "cursor-2");
  // First connection resumed from the bootstrap cursor, second from the last envelope cursor.
  assert.equal(seenCursors[0], bootstrap.snapshot_cursor);
  assert.equal(seenCursors[1], "cursor-1");
  watch.close(); client.disconnect();
});

test("watchSession pauses on reset_required and resumes via resync", async () => {
  const mock = mockBackend();
  const bootstrap = await fixture("bootstrap-response.json");
  mock.add("GET", /\/bootstrap$/, () => jsonResponse(bootstrap));
  let connections = 0;
  mock.add("GET", /\/events/, () => {
    connections++;
    if (connections === 1) return sseResponse(frame("ringing.reset_required", { schema: "qaqh.Ringing", version: 2, server_epoch: "fixture-epoch", session_id: SEED, reason: "timeline_rebuilt", snapshot_cursor: "cursor-after-reset" }));
    return sseResponse(frame("ringing.event", envelope({ event_id: "ev-after-resync" })));
  });
  const { client } = await connectedClient(mock);
  const resets = []; const events = []; const statuses = [];
  const watch = client.watchSession(SEED, { onReset: reset => resets.push(reset), onEvent: event => events.push(event), onStatus: status => statuses.push(status) });
  await waitUntil(() => resets.length === 1);
  assert.equal(watch.status, "paused"); assert.equal(watch.cursor, "cursor-after-reset");
  await watch.resync();
  await waitUntil(() => events.length === 1);
  assert.equal(events[0].event_id, "ev-after-resync");
  watch.close(); client.disconnect();
});

test("watchSession surfaces stream_terminated without reconnecting", async () => {
  const mock = mockBackend();
  const bootstrap = await fixture("bootstrap-response.json");
  mock.add("GET", /\/bootstrap$/, () => jsonResponse(bootstrap));
  let connections = 0;
  mock.add("GET", /\/events/, () => {
    connections++;
    return sseResponse(frame("ringing.stream_terminated", { code: "revoked", session_id: SEED, message: "client lease revoked or expired; reconnect after re-auth" }));
  });
  const { client } = await connectedClient(mock);
  const terminations = [];
  const watch = client.watchSession(SEED, { onTerminated: info => terminations.push(info) });
  await waitUntil(() => terminations.length === 1);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(connections, 1); assert.equal(terminations[0].code, "revoked"); assert.equal(watch.status, "paused");
  watch.close(); client.disconnect();
});

test("watchSession rejects envelopes from a foreign session", async () => {
  const mock = mockBackend();
  const bootstrap = await fixture("bootstrap-response.json");
  mock.add("GET", /\/bootstrap$/, () => jsonResponse(bootstrap));
  mock.add("GET", /\/events/, () => sseResponse(frame("ringing.event", envelope({ session_id: "some-other-seed" }))));
  const { client } = await connectedClient(mock);
  const errors = [];
  const watch = client.watchSession(SEED, { onError: error => errors.push(error) });
  await waitUntil(() => errors.length === 1);
  assert.equal(errors[0].kind, "protocol");
  watch.close(); client.disconnect();
});

function timelineFrame(seq) {
  const payload = { schema: "qaqh.Ringing", version: 2, server_epoch: "fixture-epoch", session_id: SEED, entry: { timeline_seq: seq, turn_id: "t1", seq: seq, kind: "turn_opened", data: { user_text: `msg-${seq}` } } };
  return frame("timeline.entry", payload, `fixture-epoch:timeline:${seq}`);
}

test("watchTimeline replays via Last-Event-ID after a drop", async () => {
  const mock = mockBackend();
  const seenIds = []; let connections = 0;
  mock.add("GET", /\/timeline\/events/, (url, init) => {
    connections++; seenIds.push(init.headers.get("Last-Event-ID"));
    if (connections === 1) return sseResponse(timelineFrame(1));
    return sseResponse(timelineFrame(2));
  });
  const { client } = await connectedClient(mock);
  const entries = [];
  const watch = client.watchTimeline(SEED, { onEntry: entry => entries.push(entry) });
  await waitUntil(() => entries.length >= 2);
  assert.equal(seenIds[0], null); assert.equal(seenIds[1], "fixture-epoch:timeline:1");
  assert.equal(watch.lastEventId, "fixture-epoch:timeline:2");
  assert.deepEqual(entries.map(entry => entry.entry.data.user_text), ["msg-1", "msg-2"]);
  watch.close(); client.disconnect();
});

test("pair consumes a one-time token without bearer credentials", async () => {
  const mock = mockBackend();
  mock.add("POST", /\/pair$/, () => jsonResponse({ device_id: "dev-1", token: "paired-token", protocol_version: 2 }));
  const client = createClient({ credentials, fetch: mock.impl });
  const result = await client.pair({ pairing_token: "one-time", device_name: "fixture-phone", platform: "linux" });
  assert.equal(result.token, "paired-token");
  const call = mock.calls.at(-1);
  assert.equal(call.init.headers.get("Authorization"), null);
  client.disconnect();
});

test("resolveCredentials validates pid liveness, protocol version, and freshness", async () => {
  const dir = await mkdtemp(join(tmpdir(), "qaqh-sdk-"));
  try {
    const doc = { endpoint: "http://127.0.0.1:8787", token: "t", pid: process.pid, protocol_version: 2 };
    const path = join(dir, "daemon.json");
    await writeFile(path, JSON.stringify(doc));
    assert.equal((await resolveCredentials({ kind: "local-discovery", path })).token, "t");
    await writeFile(path, JSON.stringify({ ...doc, pid: 99999999 }));
    await assert.rejects(() => resolveCredentials({ kind: "local-discovery", path }), (error) => error instanceof DiscoveryError && error.code === "dead_pid");
    await writeFile(path, JSON.stringify({ ...doc, protocol_version: 3 }));
    await assert.rejects(() => resolveCredentials({ kind: "local-discovery", path }), (error) => error instanceof DiscoveryError && error.code === "wrong_protocol");
    await writeFile(path, "{ not json");
    await assert.rejects(() => resolveCredentials({ kind: "local-discovery", path }), (error) => error instanceof DiscoveryError && error.code === "invalid_json");
    const old = join(dir, "stale.json");
    await writeFile(old, JSON.stringify(doc));
    const past = new Date(Date.now() - 60_000);
    await utimes(old, past, past);
    await assert.rejects(() => resolveCredentials({ kind: "local-discovery", path: old }, { maxAgeMs: 1000 }), (error) => error instanceof DiscoveryError && error.code === "stale");
    assert.equal((await resolveCredentials({ kind: "local-discovery", path: old }, { maxAgeMs: 120_000 })).token, "t");
    await assert.rejects(() => resolveCredentials({ kind: "local-discovery", path: join(dir, "missing.json") }), (error) => error instanceof DiscoveryError && error.code === "not_found");
    // pid liveness can be opted out (e.g. cross-container sharing a host pid namespace view).
    await writeFile(path, JSON.stringify({ ...doc, pid: 99999999 }));
    assert.equal((await resolveCredentials({ kind: "local-discovery", path }, { checkPid: false })).pid, 99999999);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("disconnect aborts in-flight requests and moves to closed", async () => {
  const mock = mockBackend();
  mock.add("POST", /\/service\/daemon\.version$/, () => new Promise(() => { /* hangs */ }));
  const { client } = await connectedClient(mock, { timeoutMs: 5000 });
  const pending = client.service("daemon.version", {});
  await new Promise(resolve => setTimeout(resolve, 5));
  client.disconnect();
  await assert.rejects(() => pending, (error) => error instanceof SdkError && (error.kind === "timeout" || error.kind === "cancelled"));
  assert.equal(client.state, "closed");
});

async function waitUntil(predicate, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.ok(predicate(), "condition not reached within timeout");
}
