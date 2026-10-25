import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join } from "node:path";

const contractRoot = new URL("../src/protocol/generated/", import.meta.url);
test("generated contract manifest hashes each imported artifact", async () => {
  const manifest = JSON.parse(await readFile(new URL("manifest.json", contractRoot), "utf8"));
  const files = [];
  for (const item of manifest.files) {
    const content = await readFile(new URL(item.path, contractRoot));
    assert.equal(createHash("sha256").update(content).digest("hex"), item.sha256, item.path);
    files.push({ path: item.path, sha256: item.sha256 });
  }
  files.sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
  const canonical = JSON.stringify(files);
  assert.equal(createHash("sha256").update(canonical).digest("hex"), manifest.contract_hash);
});

test("partial backend contract coverage stays explicit", async () => {
  const coverage = JSON.parse(await readFile(new URL("coverage.json", contractRoot), "utf8"));
  // 2026-10-25 契约重生成：timeline/approvals.device/content.download/session.list
  // /session.meta 已由后端 DTO 收口；仍未交付的能力必须保持显式 pending，
  // 禁止静默消失。
  for (const delivered of ["session.bootstrap-watch", "session.team", "approvals.admin", "approvals.device", "content.upload", "content.download", "timeline.snapshot-watch", "service.session.list", "service.session.meta"]) {
    assert.ok(coverage.capabilities.some((capability) => capability.id === delivered && capability.status === "implemented"), delivered);
  }
  for (const pending of ["daemon.control", "service.git.diff", "service.workspace.list"]) {
    assert.ok(coverage.capabilities.some((capability) => capability.id === pending && capability.status === "pending"), pending);
  }
});
