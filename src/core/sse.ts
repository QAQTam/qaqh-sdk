// Incremental `text/event-stream` decoder shared by the session and timeline
// watch streams. Chunks may split mid-line and mid-frame; only complete
// frames (terminated by an empty line) are dispatched.

export interface SseFrame { event: string; data: string; id: string | null }

const MAX_DATA_BYTES = 4 * 1024 * 1024;

export class SseDecoder {
  private buffer = "";
  private eventName = "message";
  private data = "";
  private id: string | null = null;

  push(chunk: string): SseFrame[] {
    this.buffer += chunk;
    const frames: SseFrame[] = [];
    // Split on \r\n, \n, or \r; keep the trailing partial line buffered.
    const lines = this.buffer.split(/\r\n|\r|\n/);
    this.buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (this.data.length > MAX_DATA_BYTES) throw new Error("SSE data frame exceeded 4MB safety cap");
      if (line.startsWith(":")) continue;
      if (line.startsWith("event:")) { this.eventName = line.slice(6).trim(); continue; }
      if (line.startsWith("data:")) { this.data += (this.data ? "\n" : "") + line.slice(5).replace(/^ /, ""); continue; }
      if (line.startsWith("id:")) { const value = line.slice(3).trim(); if (value) this.id = value; continue; }
      if (line === "") {
        if (this.data !== "") frames.push({ event: this.eventName, data: this.data, id: this.id });
        this.eventName = "message"; this.data = "";
      }
    }
    return frames;
  }
}
