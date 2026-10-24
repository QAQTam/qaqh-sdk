# @qaqh/sdk

Typed Ringing v2 SDK package scaffold. Protocol types/constants are imported from the backend-generated `src/protocol/generated` contract artifact; do not edit generated files by hand. Regenerate only with backend `qaqh-contract-export`, then verify the manifest/hash and coverage.

Subpaths:
- `@qaqh/sdk`: typed client and SDK errors
- `@qaqh/sdk/protocol`: generated protocol exports and contract manifest
- `@qaqh/sdk/node`: local discovery credential resolver (explicit endpoint never falls back to local discovery)

The SDK currently implements open/renew, generated-ServiceMap service calls, command submission and status lookup, and disconnect cleanup. It does not yet implement bootstrap/watch, SSE cursor recovery, timeline, content, approvals, driver, or other service methods. Backend `coverage.json` records those as pending, so this package is not a claim of complete Electron feature coverage and must not replace the Electron client until the remaining API surface is delivered.

The generated artifact was produced by `cargo run -p qaqh-contract-export -- export --out ../qaqh-sdk/src/protocol/generated`; validate it from the backend checkout with `cargo run -p qaqh-contract-export -- check --against ../qaqh-sdk/src/protocol/generated`.
