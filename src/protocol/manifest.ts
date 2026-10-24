import manifest from "./generated/manifest.json" with { type: "json" };

export type Manifest = typeof manifest;
export { manifest };
