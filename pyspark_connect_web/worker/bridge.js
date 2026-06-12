// SPDX-License-Identifier: Apache-2.0
//
// bridge.js — main-thread half of lane 3's blocking transport.
//
// The Web Worker (running Pyodide + PySpark) cannot do `fetch` against a
// cross-origin gRPC-web endpoint and, more importantly, cannot block on an
// async result. So the worker writes its request into a SharedArrayBuffer and
// parks on `Atomics.wait`. This file runs on the *main* thread, receives the
// nudge postMessage, reads the request out of the SAB, performs the real async
// `fetch`, and writes the response bytes back into the SAB — flipping the STATE
// control word and `Atomics.notify`-ing to wake the worker.
//
// The SAB layout + state machine is the authoritative contract; it is mirrored
// in `sab_channel.py` and documented in team/findings-lane3-bridge.md. Keep the
// three in sync by VALUE.
//
// Usage (main thread / page):
//   import { installBridge } from "./bridge.js";
//   const worker = new Worker("./worker_bootstrap.js", { type: "module" });
//   installBridge(worker);   // worker posts {type:"pcw_sab"} then {type:"pcw_rpc"}

"use strict";

// ---- Control-array indices (must match sab_channel.py) --------------------
const C_STATE = 0;
const C_LENGTH = 1;
const C_STATUS = 2;
const C_SEQ = 3;
const C_GEN = 4;

// ---- STATE values ---------------------------------------------------------
const S_IDLE = 0;
const S_REQ_READY = 1;
const S_RESP_CHUNK = 2;
const S_RESP_END = 3;
const S_RESP_ERROR = 4;
const S_CHUNK_ACK = 5;

const _enc = new TextEncoder();
const _dec = new TextDecoder();

// A single bridge instance per worker. Holds the SAB views handed over by the
// worker once at startup.
class Bridge {
  constructor() {
    this.ctrl = null; // Int32Array over control SAB
    this.data = null; // Uint8Array over data SAB
    this._busy = false;
  }

  attach(controlSab, dataSab) {
    this.ctrl = new Int32Array(controlSab);
    this.data = new Uint8Array(dataSab);
  }

  // ---- little-endian u32 helpers over the data region ---------------------
  _getU32(off) {
    return (
      (this.data[off] |
        (this.data[off + 1] << 8) |
        (this.data[off + 2] << 16) |
        (this.data[off + 3] << 24)) >>>
      0
    );
  }
  _putU32(off, v) {
    this.data[off] = v & 0xff;
    this.data[off + 1] = (v >>> 8) & 0xff;
    this.data[off + 2] = (v >>> 16) & 0xff;
    this.data[off + 3] = (v >>> 24) & 0xff;
    return off + 4;
  }

  // ---- read the request the worker wrote ----------------------------------
  _readRequest() {
    let off = 0;
    const headerLen = this._getU32(off);
    off += 4;
    const headerBytes = this.data.subarray(off, off + headerLen);
    const header = JSON.parse(_dec.decode(headerBytes));
    off += headerLen;
    const bodyLen = this._getU32(off);
    off += 4;
    // Copy the body out — the worker may overwrite the SAB once we wake it.
    const body = this.data.slice(off, off + bodyLen);
    return { header, body };
  }

  // ---- write a response payload + meta, flip STATE ------------------------
  _writeResponse(state, status, meta, payload) {
    const metaBytes = _enc.encode(JSON.stringify(meta || {}));
    let off = 0;
    off = this._putU32(off, metaBytes.length);
    this.data.set(metaBytes, off);
    off += metaBytes.length;
    if (payload && payload.length) {
      this.data.set(payload, off);
      off += payload.length;
    }
    Atomics.store(this.ctrl, C_STATUS, status | 0);
    Atomics.store(this.ctrl, C_LENGTH, off);
    Atomics.store(this.ctrl, C_STATE, state);
    Atomics.notify(this.ctrl, C_STATE);
  }

  _writeError(message) {
    this._writeResponse(S_RESP_ERROR, 0, { message: String(message) }, null);
  }

  // ---- wait (on the main thread, async) for the worker to ack -------------
  // The main thread MUST NOT Atomics.wait. We poll the control word via
  // Atomics.waitAsync where available, else a microtask/timeout poll loop.
  async _awaitWorker(expect) {
    // expect: the STATE value the worker will write when it wants the next
    // chunk (S_CHUNK_ACK) or is done (S_IDLE).
    while (true) {
      const cur = Atomics.load(this.ctrl, C_STATE);
      if (cur === expect || cur === S_IDLE) return cur;
      if (typeof Atomics.waitAsync === "function") {
        const r = Atomics.waitAsync(this.ctrl, C_STATE, cur);
        if (r.async) await r.value;
        // loop re-checks
      } else {
        await new Promise((res) => setTimeout(res, 0));
      }
    }
  }

  // ---- the main entry: handle one RPC the worker just posted --------------
  async handleRpc() {
    if (this._busy) return; // one RPC at a time per worker (matches blocking)
    if (!this.ctrl) return;
    if (Atomics.load(this.ctrl, C_STATE) !== S_REQ_READY) return;
    this._busy = true;
    try {
      const { header, body } = this._readRequest();
      const init = {
        method: "POST",
        headers: { ...header.headers },
        body: body,
        // gRPC-web over fetch; cross-origin to the Envoy host.
        mode: "cors",
        credentials: "omit",
      };
      // AbortController gives us timeout parity with the worker's Atomics.wait.
      const ctl = new AbortController();
      init.signal = ctl.signal;
      let timer = null;
      if (header.timeout != null) {
        timer = setTimeout(() => ctl.abort(), header.timeout * 1000);
      }

      let resp;
      try {
        resp = await fetch(header.url, init);
      } catch (e) {
        this._writeError(`fetch failed: ${e && e.message ? e.message : e}`);
        return;
      } finally {
        if (timer) clearTimeout(timer);
      }

      if (header.kind === "unary") {
        const buf = new Uint8Array(await resp.arrayBuffer());
        this._writeResponse(S_RESP_CHUNK, resp.status, { ok: resp.ok }, buf);
        // worker reads, sets S_IDLE; nothing more to do.
        return;
      }

      // ---- server streaming ----
      // Stream the response body, writing each chunk into the SAB and waiting
      // for the worker to consume (S_CHUNK_ACK) before the next.
      const reader = resp.body.getReader();
      // First-chunk status goes out with the first RESP_CHUNK; if the stream is
      // empty we still need to flip STATE so the worker isn't stuck — handled by
      // the read loop terminating into S_RESP_END.
      let first = true;
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        if (!value || value.length === 0) continue;
        this._writeResponse(
          S_RESP_CHUNK,
          resp.status,
          first ? { ok: resp.ok } : {},
          value
        );
        first = false;
        // Wait for the worker to consume and request the next chunk.
        const ack = await this._awaitWorker(S_CHUNK_ACK);
        if (ack === S_IDLE) {
          // worker abandoned the stream (generator closed / error). Stop.
          return;
        }
      }
      // End of stream.
      Atomics.store(this.ctrl, C_LENGTH, 0);
      Atomics.store(this.ctrl, C_STATE, S_RESP_END);
      Atomics.notify(this.ctrl, C_STATE);
    } catch (e) {
      try {
        this._writeError(e && e.message ? e.message : String(e));
      } catch (_) {
        /* SAB unusable; nothing else we can do */
      }
    } finally {
      this._busy = false;
    }
  }
}

// installBridge wires a Worker's messages to a Bridge instance. The worker is
// expected to post {type:"pcw_sab", control, data} once, then {type:"pcw_rpc"}
// for each request (the nudge; all data is in the SAB).
export function installBridge(worker) {
  const bridge = new Bridge();
  worker.addEventListener("message", (ev) => {
    const msg = ev.data || {};
    if (msg.type === "pcw_sab") {
      bridge.attach(msg.control, msg.data);
    } else if (msg.type === "pcw_rpc") {
      // fire-and-forget; handleRpc drives the async fetch + SAB writeback.
      bridge.handleRpc();
    }
  });
  return bridge;
}

// Constants are exported for tests / cross-checking with sab_channel.py.
export const PROTOCOL = {
  C_STATE,
  C_LENGTH,
  C_STATUS,
  C_SEQ,
  C_GEN,
  S_IDLE,
  S_REQ_READY,
  S_RESP_CHUNK,
  S_RESP_END,
  S_RESP_ERROR,
  S_CHUNK_ACK,
};
