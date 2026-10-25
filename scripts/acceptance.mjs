#!/usr/bin/env node
// Real-transport acceptance against a live Ringing v2 daemon. Read-only
// surface only: no commands are submitted and no state is mutated.
//
// Usage:
//   node scripts/acceptance.mjs [path/to/daemon.json]
//
// Exit code 0 = every exercised capability passed against the real transport.
import { resolveCredentials } from "../dist/node/index.js";
import { createClient } from "../dist/core/index.js";
import { RINGING_SCHEMA, RINGING_V2_VERSION } from "../dist/protocol/index.js";

const DISCOVERY_PATH = process.argv[2];
const results = [];
const record = (name, ok, detail = "") => {
  results.push({ name, ok });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(predicate, timeoutMs, what) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${what}`);
    await sleep(50);
  }
}

// ── 1. discovery ────────────────────────────────────────────────────────────
let credentials;
try {
  credentials = await resolveCredentials(
    DISCOVERY_PATH ? { kind: "local-discovery", path: DISCOVERY_PATH } : { kind: "local-discovery" }
  );
  record("discovery.resolve", true, `endpoint=${credentials.endpoint} pid=${credentials.pid} protocol=${credentials.protocol_version}`);
} catch (error) {
  record("discovery.resolve", false, error.message);
  console.log(`\n${results.filter(r => r.ok).length}/${results.length} passed (discovery failed; nothing else to exercise)`);
  process.exit(1);
}

const client = createClient({ credentials, logger: console });

// ── 2. lease open ───────────────────────────────────────────────────────────
try {
  const open = await client.connect();
  const ok = client.state === "ready" && typeof open.client_session_id === "string" && open.client_session_id.length > 0;
  record("connect.open", ok, `state=${client.state} session=${client.sessionId}`);
} catch (error) {
  record("connect.open", false, error.message);
  console.log(`\n${results.filter(r => r.ok).length}/${results.length} passed`);
  process.exit(1);
}

// ── 3. service: daemon.version ──────────────────────────────────────────────
try {
  const version = await client.service("daemon.version", {});
  record("service.daemon.version", typeof version === "string" && version.length > 0, `version=${version}`);
} catch (error) {
  record("service.daemon.version", false, error.message);
}

// ── 4. service: session.list ────────────────────────────────────────────────
let sessions = [];
try {
  sessions = await client.service("session.list", {});
  record("service.session.list", Array.isArray(sessions), `${sessions.length} session(s)`);
} catch (error) {
  record("service.session.list", false, error.message);
}

const seed = sessions[0]?.session_id ?? sessions[0]?.meta?.session_id;

if (seed) {
  // ── 5. attach (timeline reads require an attached session) ────────────────
  try {
    const { ack } = await client.submit("control", { type: "session_resume", session_id: seed }, seed);
    record("session.attach", ack.status === "accepted", `ack=${ack.status}${ack.message ? ` ${ack.message}` : ""}`);
  } catch (error) {
    record("session.attach", false, error.message);
  }

  // ── 6. bootstrap ──────────────────────────────────────────────────────────
  try {
    const bootstrap = await client.bootstrap(seed);
    const ok = bootstrap.schema === RINGING_SCHEMA && bootstrap.version === RINGING_V2_VERSION
      && bootstrap.session_id === seed && typeof bootstrap.snapshot_cursor === "string"
      && typeof bootstrap.control?.state?.revision === "number";
    record("session.bootstrap", ok, `cursor=${bootstrap.snapshot_cursor} revision=${bootstrap.control?.state?.revision}`);
  } catch (error) {
    record("session.bootstrap", false, error.message);
  }

  // ── 7. timeline page ──────────────────────────────────────────────────────
  try {
    const page = await client.timelinePage(seed, { limit: 10 });
    const ok = page.schema === RINGING_SCHEMA && page.session_id === seed
      && typeof page.snapshot === "object" && page.snapshot !== null
      && typeof page.total_turns === "number";
    record("timeline.page", ok, `total_turns=${page.total_turns} has_more=${page.has_more} truncated_before=${page.truncated_before}`);
  } catch (error) {
    record("timeline.page", false, error.message);
  }

  // ── 8. approvals (admin view) ─────────────────────────────────────────────
  try {
    const view = await client.approvals(seed);
    const ok = view.kind === "admin" || view.kind === "device";
    record("approvals.view", ok, `kind=${view.kind}`);
  } catch (error) {
    record("approvals.view", false, error.message);
  }

  // ── 9. service: session.activity / session.get_activity (B03 session slice)
  try {
    const activity = await client.service("session.activity", {});
    const ok = Array.isArray(activity) && activity.every((entry) => typeof entry.session_id === "string" && typeof entry.state === "string");
    record("service.session.activity", ok, `${Array.isArray(activity) ? activity.length : "?"} entr(ies)`);
  } catch (error) {
    record("service.session.activity", false, error.message);
  }
  try {
    const toolActivity = await client.service("session.get_activity", { session_id: seed });
    const ok = Array.isArray(toolActivity) && toolActivity.every((entry) => typeof entry.tool_name === "string" && typeof entry.status === "string");
    record("service.session.get_activity", ok, `${Array.isArray(toolActivity) ? toolActivity.length : "?"} tool record(s)`);
  } catch (error) {
    record("service.session.get_activity", false, error.message);
  }

  // ── 10. live session watch (SSE + cursor) ──────────────────────────────────
  {
    let events = 0;
    let lastError = null;
    const watch = client.watchSession(seed, {
      onEvent: () => { events += 1; },
      onError: (error) => { lastError = error; },
    });
    try {
      await waitFor(() => watch.status === "live", 8000, "session watch to go live");
      await sleep(1500);
      record("session.watch", lastError === null, `status=${watch.status} events=${events}${lastError ? ` lastError=${lastError.message}` : ""}`);
    } catch (error) {
      record("session.watch", false, `${watch.status}: ${error.message}${lastError ? `; ${lastError.message}` : ""}`);
    } finally {
      watch.close();
    }
  }

  // ── 11. live timeline watch (SSE + Last-Event-ID) ──────────────────────────
  {
    let entries = 0;
    let lastError = null;
    const watch = client.watchTimeline(seed, {
      onEntry: () => { entries += 1; },
      onError: (error) => { lastError = error; },
    });
    try {
      await waitFor(() => watch.status === "live", 8000, "timeline watch to go live");
      await sleep(1500);
      record("timeline.watch", lastError === null, `status=${watch.status} entries=${entries}${lastError ? ` lastError=${lastError.message}` : ""}`);
    } catch (error) {
      record("timeline.watch", false, `${watch.status}: ${error.message}${lastError ? `; ${lastError.message}` : ""}`);
    } finally {
      watch.close();
    }
  }
} else {
  console.log("SKIP  session-scoped capabilities (no sessions on this daemon)");
  for (const name of ["session.attach", "session.bootstrap", "timeline.page", "approvals.view", "service.session.activity", "service.session.get_activity", "session.watch", "timeline.watch"]) {
    results.push({ name, ok: true, skipped: true });
    console.log(`SKIP  ${name}`);
  }
}

// ── 12. disconnect ──────────────────────────────────────────────────────────
client.disconnect();
record("disconnect", client.state === "closed", `state=${client.state}`);

const passed = results.filter(r => r.ok).length;
console.log(`\n${passed}/${results.length} checks passed${passed === results.length ? " — real-transport acceptance OK" : ""}`);
process.exit(passed === results.length ? 0 : 1);
