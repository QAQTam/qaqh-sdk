# @qaqh/sdk

Typed Ringing v2 SDK for QAQ-Harness. Protocol types/constants are imported from the backend-generated `src/protocol/generated` contract artifact; do not edit generated files by hand. Regenerate only with backend `qaqh-contract-export`, then verify the manifest/hash and coverage.

Subpaths:
- `@qaqh/sdk`: typed client, watch streams, and SDK errors
- `@qaqh/sdk/protocol`: generated protocol exports and contract manifest
- `@qaqh/sdk/node`: local discovery credential resolver (explicit endpoint never falls back to local discovery)

## Client surface

`RingingClient` implements the full backend-implemented wire surface (42 capabilities in `coverage.json`):

- **Lease**: `connect` / auto lease renewal with credential refresh + recovery reconnect, `disconnect`, `onStateChange`
- **Services**: typed `service()` over the generated `ServiceMap` (24 methods incl. `session.list`/`session.meta`); dynamic names ride the same route at runtime
- **Commands**: `submit` (idempotent `command_id`; timeouts surface as `outcome_unknown` with the id attached), `commandStatus`, `waitForCommand` polling helper
- **Session watch**: `bootstrap` + `watchSession` — per-seed single SSE stream, cursor tracking with `?since_cursor=` resumption, exponential backoff reconnects, `ringing.event` / `ringing.reset_required` / `ringing.stream_terminated` / `diagnostics.memory` demux, `resync()` after reset
- **Timeline**: `timelinePage` (validated `TimelinePage`), `watchTimeline` — SSE with `Last-Event-ID` (`{epoch}:timeline:{seq}`) resume
- **Approvals**: `approvals` (discriminates admin pending view vs device opaque challenges), `respondApproval` (device side)
- **Driver seat**: `claimDriver` / `releaseDriver` (busy/not-driver surfaces as `accepted: false`, matching the daemon)
- **Content**: `uploadContent` (daemon multipart wire: `session_id`/`media_type`/`content` parts), `downloadContent` (accepts `sha256:<hex>` or bare hex, single RFC 9110 byte ranges)
- **Devices & pairing**: `listDevices`, `revokeDevice`, `issuePairToken` (admin), `pair` (one-time token, no Bearer)

`resolveCredentials` (node subpath) validates the discovery document shape, protocol version, pid liveness, and optional freshness, and reports typed `DiscoveryError` codes.

Still pending backend delivery (recorded as `pending` in `coverage.json`): daemon control endpoints, config/git/profile/skills/todo/workspace service slices, `service.subagent.spawn`, diagnostics RPC methods, timeline deep pagination.

## Development

The generated artifact was produced by `cargo run -p qaqh-contract-export -- export --out ../qaqh-sdk/src/protocol/generated` from the backend checkout; validate it with `cargo run -p qaqh-contract-export -- check --against ../qaqh-sdk/src/protocol/generated`.

- `npm run build` — compile to `dist/` and apply ESM fixups (ts-rs emits extensionless imports; `manifest.json` is copied next to the compiled output)
- `npm test` — contract hash/coverage guards plus client behavior tests over a mock transport (SSE framing, cursor resume, upload wire shape, error mapping, discovery validation)
