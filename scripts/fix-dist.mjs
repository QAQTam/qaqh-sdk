// Post-build fixups so dist actually runs under Node ESM:
// 1. ts-rs emits extensionless relative imports; append ".js" — in both .js
//    (runtime) and .d.ts (types: "./x.js" resolves to the sibling x.d.ts).
//    Without the .d.ts pass, every star re-export through the generated
//    protocol index silently vanishes for consumers under NodeNext.
// 2. `manifest.ts` imports generated/manifest.json via resolveJsonModule; copy it next to the compiled output.
import { readdir, readFile, writeFile, mkdir, cp } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const dist = fileURLToPath(new URL("../dist/", import.meta.url));
const files = [];
async function walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) await walk(path); else if (path.endsWith(".js") || path.endsWith(".d.ts")) files.push(path);
  }
}
await walk(dist);
let patched = 0;
for (const path of files) {
  const source = await readFile(path, "utf8");
  const fixed = source.replace(/(from\s+["'])(\.\.?\/[^"'.]+)(["'])/g, "$1$2.js$3");
  if (fixed !== source) { await writeFile(path, fixed); patched++; }
}
await mkdir(join(dist, "protocol/generated"), { recursive: true });
await cp(fileURLToPath(new URL("../src/protocol/generated/manifest.json", import.meta.url)), join(dist, "protocol/generated/manifest.json"));
console.log(`fix-dist: rewrote imports in ${patched} file(s); copied manifest.json`);
