// SPDX-License-Identifier: Apache-2.0
//
// Node unit tests for the main-thread half of the blocking SAB transport
// (pyspark_connect_web/worker/bridge.js).
//
// These tests run on Node (no browser, no Pyodide). They use a real
// SharedArrayBuffer + Atomics and a fake "worker" harness that reads and
// writes the control Int32Array + data Uint8Array byte-for-byte the way the
// Python peer (pyspark_connect_web/worker/sab_channel.py, class
// _AtomicsBackend) does. This pins the bridge to the real wire protocol
// without the slow browser e2e.
//
// The protocol constants below are mirrored BY VALUE from sab_channel.py so a
// drift in either half is caught here (we also assert bridge.js exports the
// same values via PROTOCOL).

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Bridge, PROTOCOL, installBridge } from "../../pyspark_connect_web/worker/bridge.js";

// ---- protocol constants, copied by value from sab_channel.py --------------
const C_STATE = 0;
const C_LENGTH = 1;
const C_STATUS = 2;
const C_SEQ = 3;
const C_GEN = 4;
const C_CAP = 5;
const CONTROL_SLOTS = 8; // _CONTROL_SLOTS

const S_IDLE = 0;
const S_REQ_READY = 1;
const S_RESP_CHUNK = 2;
const S_RESP_END = 3;
const S_RESP_ERROR = 4;
const S_CHUNK_ACK = 5;
const S_REALLOC_REQ = 6;

const META_ZONE = 4096; // _META_ZONE

const enc = new TextEncoder();
const dec = new TextDecoder();

// --------------------------------------------------------------------------- //
// Fake worker peer - mirrors _AtomicsBackend in sab_channel.py.
//
// It owns the SABs, writes requests exactly as Python does, and reads response
// windows / acks the CHUNK_ACK ping-pong exactly as _read_window /
// _reassemble_windows do. Where the Python side blocks on Atomics.wait, this
// JS peer polls Atomics.load in a yielding loop (the main thread under test is
// async, so we must yield to let its microtasks run).
// --------------------------------------------------------------------------- //
class FakeWorker {
  constructor(dataBytes) {
    this.controlSab = new SharedArrayBuffer(CONTROL_SLOTS * 4);
    this.dataSab = new SharedArrayBuffer(dataBytes);
    this.ctrl = new Int32Array(this.controlSab);
    this.data = new Uint8Array(this.dataSab);
    Atomics.store(this.ctrl, C_CAP, dataBytes);
  }

  // ---- little-endian u32, identical to _put_u32 / _get_u32 ----------------
  putU32(off, v) {
    this.data[off + 0] = v & 0xff;
    this.data[off + 1] = (v >> 8) & 0xff;
    this.data[off + 2] = (v >> 16) & 0xff;
    this.data[off + 3] = (v >> 24) & 0xff;
    return off + 4;
  }
  getU32(off) {
    const v =
      (this.data[off + 0] |
        (this.data[off + 1] << 8) |
        (this.data[off + 2] << 16) |
        (this.data[off + 3] << 24)) >>>
      0;
    return [v, off + 4];
  }

  // ---- write a request the way _write_request does ------------------------
  // header layout: [u32 header_len][header json][u32 body_len][body bytes]
  writeRequest(header, body) {
    const headerBytes = enc.encode(JSON.stringify(header));
    body = body || new Uint8Array(0);
    let off = 0;
    off = this.putU32(off, headerBytes.length);
    this.data.set(headerBytes, off);
    off += headerBytes.length;
    off = this.putU32(off, body.length);
    this.data.set(body, off);
    off += body.length;
    Atomics.store(this.ctrl, C_GEN, header.gen || 0);
    Atomics.store(this.ctrl, C_LENGTH, off);
    Atomics.store(this.ctrl, C_SEQ, 0);
    Atomics.store(this.ctrl, C_STATE, S_REQ_READY);
    Atomics.notify(this.ctrl, C_STATE);
  }

  // ---- read one response window, identical to _read_window ----------------
  readWindow() {
    const length = Atomics.load(this.ctrl, C_LENGTH);
    let [metaLen, off] = this.getU32(0);
    const metaRaw = this.data.slice(off, off + metaLen);
    off += metaLen;
    const meta = metaLen ? JSON.parse(dec.decode(metaRaw)) : {};
    const payload = this.data.slice(off, length);
    return { meta, payload };
  }

  readError() {
    const [metaLen, off] = this.getU32(0);
    const raw = this.data.slice(off, off + metaLen);
    return metaLen ? JSON.parse(dec.decode(raw)) : {};
  }

  // ---- poll until STATE != from (yielding so the async bridge can run) -----
  async waitState(from, timeoutMs = 2000) {
    const deadline = Date.now() + timeoutMs;
    while (true) {
      const s = Atomics.load(this.ctrl, C_STATE);
      if (s !== from) return s;
      if (Date.now() > deadline) throw new Error(`waitState timed out waiting to leave ${from}`);
      await new Promise((r) => setTimeout(r, 0));
    }
  }

  // ---- ack a window the way _reassemble_windows does (request next) -------
  ack() {
    Atomics.store(this.ctrl, C_STATE, S_CHUNK_ACK);
    Atomics.notify(this.ctrl, C_STATE);
  }

  goIdle() {
    Atomics.store(this.ctrl, C_STATE, S_IDLE);
    Atomics.notify(this.ctrl, C_STATE);
  }

  // ---- reassemble one logical payload across windows ----------------------
  // Mirrors _reassemble_windows: read first window (already present), and while
  // meta.more, ack + wait for next RESP_CHUNK. Returns {firstMeta, payload}.
  async reassemble() {
    const parts = [];
    let firstMeta = null;
    let first = true;
    while (true) {
      const { meta, payload } = this.readWindow();
      if (first) {
        firstMeta = meta;
        first = false;
      }
      parts.push(payload);
      if (!meta.more) break;
      this.ack();
      const s = await this.waitState(S_CHUNK_ACK);
      if (s !== S_RESP_CHUNK) {
        throw new Error(`unexpected state ${s} while reassembling`);
      }
    }
    // join parts
    let total = 0;
    for (const p of parts) total += p.length;
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return { firstMeta, payload: out };
  }
}

// ---- helpers to build fake fetch Responses --------------------------------
function fakeUnaryResponse({ status = 200, ok = true, headers = {}, body = new Uint8Array(0) }) {
  return {
    status,
    ok,
    headers: { forEach: (cb) => Object.entries(headers).forEach(([k, v]) => cb(v, k)) },
    arrayBuffer: async () => body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
  };
}

function fakeStreamResponse({ status = 200, ok = true, headers = {}, chunks = [], readError = null }) {
  let i = 0;
  return {
    status,
    ok,
    headers: { forEach: (cb) => Object.entries(headers).forEach(([k, v]) => cb(v, k)) },
    body: {
      getReader() {
        return {
          async read() {
            if (readError && i === readError.at) throw readError.err;
            if (i >= chunks.length) return { value: undefined, done: true };
            return { value: chunks[i++], done: false };
          },
        };
      },
    },
  };
}

// attach a bridge to a fake worker's SABs and return it
function attached(fw) {
  const b = new Bridge();
  b.attach(fw.controlSab, fw.dataSab);
  return b;
}

describe("protocol constants match sab_channel.py", () => {
  it("bridge PROTOCOL exports the same values", () => {
    expect(PROTOCOL.C_STATE).toBe(C_STATE);
    expect(PROTOCOL.C_LENGTH).toBe(C_LENGTH);
    expect(PROTOCOL.C_STATUS).toBe(C_STATUS);
    expect(PROTOCOL.C_SEQ).toBe(C_SEQ);
    expect(PROTOCOL.C_GEN).toBe(C_GEN);
    expect(PROTOCOL.C_CAP).toBe(C_CAP);
    expect(PROTOCOL.S_IDLE).toBe(S_IDLE);
    expect(PROTOCOL.S_REQ_READY).toBe(S_REQ_READY);
    expect(PROTOCOL.S_RESP_CHUNK).toBe(S_RESP_CHUNK);
    expect(PROTOCOL.S_RESP_END).toBe(S_RESP_END);
    expect(PROTOCOL.S_RESP_ERROR).toBe(S_RESP_ERROR);
    expect(PROTOCOL.S_CHUNK_ACK).toBe(S_CHUNK_ACK);
    expect(PROTOCOL.S_REALLOC_REQ).toBe(S_REALLOC_REQ);
    expect(PROTOCOL.META_ZONE).toBe(META_ZONE);
  });
});

describe("attach + control-word layout", () => {
  it("binds Int32Array control and Uint8Array data over the shared buffers", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    expect(b.ctrl).toBeInstanceOf(Int32Array);
    expect(b.data).toBeInstanceOf(Uint8Array);
    expect(b.ctrl.length).toBe(CONTROL_SLOTS);
    expect(b.data.length).toBe(64 * 1024);
    // control word published by the worker is visible to the bridge.
    expect(Atomics.load(b.ctrl, C_CAP)).toBe(64 * 1024);
  });

  it("payload capacity = data.length - META_ZONE - 4 (u32 meta_len)", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    expect(b._payloadCapacity()).toBe(64 * 1024 - META_ZONE - 4);
  });
});

describe("_readRequest parses the header+body sab_channel writes", () => {
  it("decodes the header json and copies the body", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const header = {
      kind: "unary",
      url: "https://envoy.example:8081/svc/Method",
      headers: { "content-type": "application/grpc-web+proto", "x-y": "z" },
      timeout: 30,
      gen: 7,
    };
    const body = new Uint8Array([0, 0, 0, 0, 5, 1, 2, 3, 4, 5]);
    fw.writeRequest(header, body);

    const { header: h, body: gotBody } = b._readRequest();
    expect(h).toEqual(header);
    expect(Array.from(gotBody)).toEqual(Array.from(body));
  });

  it("reads the header via a COPY, not a shared view", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const header = { kind: "unary", url: "https://h/x", headers: {}, timeout: null, gen: 1 };
    const body = new Uint8Array([9, 8, 7]);
    fw.writeRequest(header, body);
    const { body: gotBody } = b._readRequest();
    // .slice() returns a copy backed by a plain ArrayBuffer (not shared) so the
    // worker overwriting the SAB after we wake it cannot corrupt our read.
    expect(gotBody.buffer).not.toBe(b.data.buffer);
    expect(ArrayBuffer.isView(gotBody)).toBe(true);
    expect(gotBody.buffer instanceof SharedArrayBuffer).toBe(false);
    // mutate the SAB where the body lived; our copy must be unchanged.
    fw.data.fill(0xee);
    expect(Array.from(gotBody)).toEqual([9, 8, 7]);
  });
});

describe("_writeWindow framing", () => {
  it("writes [u32 meta_len][meta json][payload] and flips STATE/STATUS/LENGTH", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const payload = new Uint8Array([10, 20, 30, 40]);
    b._writeWindow(S_RESP_CHUNK, 201, { more: false, hello: "world" }, payload);

    expect(Atomics.load(fw.ctrl, C_STATE)).toBe(S_RESP_CHUNK);
    expect(Atomics.load(fw.ctrl, C_STATUS)).toBe(201);

    const { meta, payload: got } = fw.readWindow();
    expect(meta).toEqual({ more: false, hello: "world" });
    expect(Array.from(got)).toEqual([10, 20, 30, 40]);

    // C_LENGTH is the total valid byte count = 4 + metaLen + payloadLen.
    const metaLen = enc.encode(JSON.stringify({ more: false, hello: "world" })).length;
    expect(Atomics.load(fw.ctrl, C_LENGTH)).toBe(4 + metaLen + payload.length);
  });

  it("handles an empty/null payload window", () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    b._writeWindow(S_RESP_CHUNK, 200, {}, null);
    const { meta, payload } = fw.readWindow();
    expect(meta).toEqual({});
    expect(payload.length).toBe(0);
  });
});

describe("_emitMessage windowing", () => {
  it("SINGLE window when payload fits: meta.more=false, one read", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const bytes = new Uint8Array(100).map((_, i) => i & 0xff);

    const emit = b._emitMessage(200, { ok: true, headers: { a: "b" } }, bytes);
    await fw.waitState(S_IDLE); // wait for first RESP_CHUNK
    const { firstMeta, payload } = await fw.reassemble();

    expect(await emit).toBe(true);
    expect(firstMeta.more).toBe(false);
    expect(firstMeta.ok).toBe(true);
    expect(firstMeta.headers).toEqual({ a: "b" });
    expect(Array.from(payload)).toEqual(Array.from(bytes));
  });

  it("MULTI window for a payload exceeding _payloadCapacity, with CHUNK_ACK ping-pong", async () => {
    // small data SAB so capacity is tiny -> forces multiple windows.
    const dataBytes = META_ZONE + 4 + 8; // payloadCapacity == 8
    const fw = new FakeWorker(dataBytes);
    const b = attached(fw);
    expect(b._payloadCapacity()).toBe(8);

    const bytes = new Uint8Array(20).map((_, i) => (i * 3) & 0xff); // 3 windows: 8,8,4

    const emit = b._emitMessage(200, { ok: true, headers: { h: "1" } }, bytes);
    await fw.waitState(S_IDLE);
    const { firstMeta, payload } = await fw.reassemble();

    expect(await emit).toBe(true);
    // first window carried the base meta; reassembled payload is the whole thing.
    expect(firstMeta.ok).toBe(true);
    expect(firstMeta.headers).toEqual({ h: "1" });
    expect(payload.length).toBe(20);
    expect(Array.from(payload)).toEqual(Array.from(bytes));
  });

  it("returns false if the worker abandons (goes IDLE) mid-stream", async () => {
    const dataBytes = META_ZONE + 4 + 8; // capacity 8
    const fw = new FakeWorker(dataBytes);
    const b = attached(fw);
    const bytes = new Uint8Array(20); // needs >1 window

    const emit = b._emitMessage(200, {}, bytes);
    await fw.waitState(S_IDLE); // first RESP_CHUNK written
    // Worker reads first window then abandons instead of acking.
    fw.readWindow();
    fw.goIdle();
    expect(await emit).toBe(false);
  });
});

describe("handleRpc: unary fetch path", () => {
  let origFetch;
  beforeEach(() => {
    origFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = origFetch;
  });

  it("performs the fetch and writes the response body back as a single window", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const respBody = new Uint8Array([0, 0, 0, 0, 3, 0xaa, 0xbb, 0xcc]);

    const fetchMock = vi.fn(async (url, init) => {
      expect(url).toBe("https://h/svc/M");
      expect(init.method).toBe("POST");
      expect(init.headers["content-type"]).toBe("application/grpc-web+proto");
      expect(init.mode).toBe("cors");
      expect(init.credentials).toBe("omit");
      // body passed through is the copy of the request body.
      expect(Array.from(new Uint8Array(init.body))).toEqual([1, 2, 3]);
      return fakeUnaryResponse({
        status: 200,
        ok: true,
        headers: { "grpc-status": "0" },
        body: respBody,
      });
    });
    globalThis.fetch = fetchMock;

    fw.writeRequest(
      {
        kind: "unary",
        url: "https://h/svc/M",
        headers: { "content-type": "application/grpc-web+proto" },
        timeout: null,
        gen: 1,
      },
      new Uint8Array([1, 2, 3])
    );

    const done = b.handleRpc();
    await fw.waitState(S_REQ_READY); // bridge advances to RESP_CHUNK
    const { firstMeta, payload } = await fw.reassemble();
    await done;

    expect(fetchMock).toHaveBeenCalledOnce();
    expect(Atomics.load(fw.ctrl, C_STATUS)).toBe(200);
    expect(firstMeta.ok).toBe(true);
    expect(firstMeta.headers["grpc-status"]).toBe("0");
    expect(Array.from(payload)).toEqual(Array.from(respBody));
  });

  it("passes a non-200 HTTP status + headers through (HTTP error is not a transport error)", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    globalThis.fetch = vi.fn(async () =>
      fakeUnaryResponse({
        status: 503,
        ok: false,
        headers: { "grpc-status": "14" },
        body: new Uint8Array(0),
      })
    );
    fw.writeRequest(
      { kind: "unary", url: "https://h/x", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    await fw.waitState(S_REQ_READY);
    const { firstMeta, payload } = await fw.reassemble();
    await done;
    expect(Atomics.load(fw.ctrl, C_STATUS)).toBe(503);
    expect(firstMeta.ok).toBe(false);
    expect(firstMeta.headers["grpc-status"]).toBe("14");
    expect(payload.length).toBe(0);
  });

  it("ignores re-entrant handleRpc while busy and no-ops when STATE != REQ_READY", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    // STATE is IDLE -> handleRpc should be a no-op (no fetch).
    globalThis.fetch = vi.fn(async () => fakeUnaryResponse({ body: new Uint8Array(0) }));
    await b.handleRpc();
    expect(globalThis.fetch).not.toHaveBeenCalled();
  });
});

describe("handleRpc: server-streaming reader loop", () => {
  let origFetch;
  beforeEach(() => {
    origFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = origFetch;
  });

  it("emits each reader chunk as a message, then RESP_END (worker acks each)", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const chunks = [new Uint8Array([1, 1, 1]), new Uint8Array([2, 2]), new Uint8Array([3, 3, 3, 3])];
    globalThis.fetch = vi.fn(async () =>
      fakeStreamResponse({ status: 200, ok: true, headers: { srv: "yes" }, chunks })
    );

    fw.writeRequest(
      { kind: "server_stream", url: "https://h/s", headers: {}, timeout: null, gen: 1 },
      new Uint8Array([0])
    );

    const done = b.handleRpc();

    // Consume the stream the way _AtomicsBackend.server_stream does:
    // wait (leaving REQ_READY first, then CHUNK_ACK), reassemble, ack, repeat.
    const received = [];
    let waitOn = S_REQ_READY;
    let firstHeaders = null;
    while (true) {
      const s = await fw.waitState(waitOn);
      if (s === S_RESP_END) break;
      if (s === S_RESP_ERROR) throw new Error("unexpected error state");
      expect(s).toBe(S_RESP_CHUNK);
      const { firstMeta, payload } = await fw.reassemble();
      if (firstHeaders === null) firstHeaders = firstMeta.headers;
      received.push(Array.from(payload));
      fw.ack();
      waitOn = S_CHUNK_ACK;
    }
    await done;

    expect(received).toEqual([
      [1, 1, 1],
      [2, 2],
      [3, 3, 3, 3],
    ]);
    // first chunk's meta carries response headers.
    expect(firstHeaders).toEqual({ srv: "yes" });
    expect(Atomics.load(fw.ctrl, C_STATE)).toBe(S_RESP_END);
    expect(Atomics.load(fw.ctrl, C_LENGTH)).toBe(0);
  });

  it("skips empty reader chunks and still ends with RESP_END", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const chunks = [new Uint8Array(0), new Uint8Array([7, 7])];
    globalThis.fetch = vi.fn(async () => fakeStreamResponse({ chunks }));
    fw.writeRequest(
      { kind: "server_stream", url: "https://h/s", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    const received = [];
    let waitOn = S_REQ_READY;
    while (true) {
      const s = await fw.waitState(waitOn);
      if (s === S_RESP_END) break;
      expect(s).toBe(S_RESP_CHUNK);
      const { payload } = await fw.reassemble();
      received.push(Array.from(payload));
      fw.ack();
      waitOn = S_CHUNK_ACK;
    }
    await done;
    expect(received).toEqual([[7, 7]]);
  });
});

describe("error paths: _writeError", () => {
  let origFetch;
  beforeEach(() => {
    origFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = origFetch;
  });

  it("network fetch failure -> RESP_ERROR with kind 'error'", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    globalThis.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    fw.writeRequest(
      { kind: "unary", url: "https://h/x", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    const s = await fw.waitState(S_REQ_READY);
    expect(s).toBe(S_RESP_ERROR);
    const meta = fw.readError();
    expect(meta.kind).toBe("error");
    expect(meta.message).toContain("fetch failed");
    await done;
  });

  it("timeout -> AbortController fires -> RESP_ERROR with kind 'timeout'", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    // fetch that rejects with AbortError once the signal aborts.
    globalThis.fetch = vi.fn((url, init) => {
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => {
          const e = new Error("aborted");
          e.name = "AbortError";
          reject(e);
        });
      });
    });
    fw.writeRequest(
      // tiny timeout (0.01s) so the setTimeout in the bridge fires quickly.
      { kind: "unary", url: "https://h/x", headers: {}, timeout: 0.01, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    const s = await fw.waitState(S_REQ_READY, 5000);
    expect(s).toBe(S_RESP_ERROR);
    const meta = fw.readError();
    expect(meta.kind).toBe("timeout");
    expect(meta.message).toContain("timed out");
    await done;
  });

  it("explicit abort (AbortError, not timeout) -> RESP_ERROR with kind 'abort'", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    globalThis.fetch = vi.fn(async () => {
      const e = new Error("user aborted");
      e.name = "AbortError";
      throw e;
    });
    fw.writeRequest(
      { kind: "unary", url: "https://h/x", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    const s = await fw.waitState(S_REQ_READY);
    expect(s).toBe(S_RESP_ERROR);
    expect(fw.readError().kind).toBe("abort");
    await done;
  });

  it("server-stream reader error -> RESP_ERROR", async () => {
    const fw = new FakeWorker(64 * 1024);
    const b = attached(fw);
    const err = new Error("stream broke");
    globalThis.fetch = vi.fn(async () =>
      fakeStreamResponse({ chunks: [new Uint8Array([1])], readError: { at: 1, err } })
    );
    fw.writeRequest(
      { kind: "server_stream", url: "https://h/s", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    const done = b.handleRpc();
    // first chunk arrives; ack it, then the second read() throws.
    let waitOn = S_REQ_READY;
    const s1 = await fw.waitState(waitOn);
    expect(s1).toBe(S_RESP_CHUNK);
    await fw.reassemble();
    fw.ack();
    const s2 = await fw.waitState(S_CHUNK_ACK);
    expect(s2).toBe(S_RESP_ERROR);
    const meta = fw.readError();
    expect(meta.message).toContain("stream read failed");
    await done;
  });
});

describe("installBridge wiring", () => {
  it("attaches on pcw_sab and drives handleRpc on pcw_rpc", async () => {
    // Minimal EventTarget-ish fake worker.
    const listeners = [];
    const worker = {
      addEventListener: (_t, cb) => listeners.push(cb),
      emit: (data) => listeners.forEach((cb) => cb({ data })),
    };
    const fw = new FakeWorker(64 * 1024);
    const bridge = installBridge(worker);

    globalThis.fetch = vi.fn(async () =>
      fakeUnaryResponse({ status: 200, ok: true, headers: {}, body: new Uint8Array([5, 5]) })
    );

    worker.emit({ type: "pcw_sab", control: fw.controlSab, data: fw.dataSab });
    expect(bridge.ctrl).toBeInstanceOf(Int32Array);

    fw.writeRequest(
      { kind: "unary", url: "https://h/x", headers: {}, timeout: null, gen: 1 },
      new Uint8Array(0)
    );
    worker.emit({ type: "pcw_rpc" });

    await fw.waitState(S_REQ_READY);
    const { payload } = await fw.reassemble();
    expect(Array.from(payload)).toEqual([5, 5]);
    delete globalThis.fetch;
  });
});
