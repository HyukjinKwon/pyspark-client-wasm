// SPDX-License-Identifier: Apache-2.0
//
// bridge.js - main-thread half of the blocking transport.
//
// The Web Worker (running Pyodide + PySpark) cannot do `fetch` against a
// cross-origin gRPC-web endpoint and, more importantly, cannot block on an
// async result. So the worker writes its request into a SharedArrayBuffer and
// parks on `Atomics.wait`. This file runs on the *main* thread, receives the
// nudge postMessage, reads the request out of the SAB, performs the real async
// `fetch`, and writes the response bytes back into the SAB - flipping the STATE
// control word and `Atomics.notify`-ing to wake the worker.
//
// The SAB layout + state machine is the authoritative contract; it is mirrored
// in `sab_channel.py` and documented in the project notes. Keep the
// three in sync by VALUE.
//
// Large results - bounded-window transfer
// ----------------------------------------
// The data SAB has a fixed capacity. A single logical payload (a unary body or
// one server-stream chunk) larger than the payload region is written in
// successive *windows* of at most `payloadCapacity` bytes. Each window carries a
// meta flag `more:true` until the last window of that payload; the worker reads
// a window, and if `more` it acks (S_CHUNK_ACK) and waits for the next window.
// This delivers arbitrarily large results over a fixed buffer with no realloc.
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
const C_CAP = 5; // current data-SAB capacity in bytes (published by the worker)

// ---- STATE values ---------------------------------------------------------
const S_IDLE = 0;
const S_REQ_READY = 1;
const S_RESP_CHUNK = 2;
const S_RESP_END = 3;
const S_RESP_ERROR = 4;
const S_CHUNK_ACK = 5;
const S_REALLOC_REQ = 6;

// Must match _META_ZONE in sab_channel.py: reserved meta zone at the front of
// the data region. Window payload capacity = total capacity - meta zone.
const META_ZONE = 4096;

const _enc = new TextEncoder();
const _dec = new TextDecoder();

// A single bridge instance per worker. Holds the SAB views handed over by the
// worker once at startup (and re-handed on a data-SAB realloc).
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

  // Payload bytes that fit in one window of the current data SAB.
  _payloadCapacity() {
    return this.data.length - META_ZONE - 4 /* u32 meta_len */;
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
    // .slice (not .subarray): TextDecoder.decode rejects a view backed by a
    // SharedArrayBuffer ("must not be shared"); slice copies into a plain buffer.
    const headerBytes = this.data.slice(off, off + headerLen);
    const header = JSON.parse(_dec.decode(headerBytes));
    off += headerLen;
    const bodyLen = this._getU32(off);
    off += 4;
    // Copy the body out - the worker may overwrite the SAB once we wake it.
    const body = this.data.slice(off, off + bodyLen);
    return { header, body };
  }

  // ---- write one window [u32 meta_len][meta json][payload], flip STATE -----
  _writeWindow(state, status, meta, payload) {
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

  // ---- emit one logical message (unary body / one stream chunk) as 1+ -----
  // windows, waiting for the worker to ack each non-final window. `metaBase`
  // is merged into the FIRST window's meta (status/headers context). Returns
  // false if the worker abandoned the exchange (went IDLE), true otherwise.
  async _emitMessage(status, metaBase, bytes) {
    const cap = this._payloadCapacity();
    const total = bytes ? bytes.length : 0;
    let sent = 0;
    let first = true;
    do {
      const end = Math.min(sent + cap, total);
      const window = bytes ? bytes.subarray(sent, end) : null;
      const more = end < total;
      const meta = Object.assign({}, first ? metaBase || {} : {}, { more });
      this._writeWindow(S_RESP_CHUNK, status, meta, window);
      sent = end;
      first = false;
      if (more) {
        // Worker must ack this window before we write the next one.
        const ack = await this._awaitWorker(S_CHUNK_ACK);
        if (ack === S_IDLE) return false;
      }
    } while (sent < total);
    return true;
  }

  _writeError(message, kind) {
    this._writeWindow(
      S_RESP_ERROR,
      0,
      { message: String(message), kind: kind || "error" },
      null
    );
  }

  // ---- wait (on the main thread, async) for the worker to ack -------------
  // The main thread MUST NOT Atomics.wait. We poll the control word via
  // Atomics.waitAsync where available, else a microtask/timeout poll loop.
  async _awaitWorker(expect) {
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
    let timer = null;
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
      let timedOut = false;
      if (header.timeout != null) {
        timer = setTimeout(() => {
          timedOut = true;
          ctl.abort();
        }, header.timeout * 1000);
      }

      let resp;
      try {
        resp = await fetch(header.url, init);
      } catch (e) {
        // Distinguish a timeout-driven abort from a network/abort failure so
        // the worker can raise TransportTimeout vs TransportAborted.
        const isAbort = e && e.name === "AbortError";
        const kind = timedOut ? "timeout" : isAbort ? "abort" : "error";
        const msg = timedOut
          ? `fetch timed out after ${header.timeout}s`
          : `fetch failed for ${header.url}: ${e && e.message ? e.message : e}`;
        this._writeError(msg, kind);
        return;
      } finally {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
      }

      // Surface response headers so the can read grpc-status from HTTP
      // headers when a proxy puts it there (e.g. empty unary, or HTTP error
      // with a grpc-status header). Cheap to collect; the ignores unknowns.
      const headers = {};
      try {
        resp.headers.forEach((v, k) => {
          headers[k] = v;
        });
      } catch (_) {
        /* Headers not iterable in some shims; leave empty. */
      }

      if (header.kind === "unary") {
        const buf = new Uint8Array(await resp.arrayBuffer());
        // One logical message, windowed if larger than the payload region.
        // HTTP errors are NOT transport errors: pass the status + headers + any
        // body through so the raises the right grpc exception.
        await this._emitMessage(resp.status, { ok: resp.ok, headers }, buf);
        // worker reads, sets S_IDLE; nothing more to do.
        return;
      }

      // ---- server streaming ----
      // Each reader chunk is one logical message; window it, then wait for the
      // worker to consume + request the next (S_CHUNK_ACK) before reading on.
      const reader = resp.body.getReader();
      let first = true;
      while (true) {
        let value, done;
        try {
          ({ value, done } = await reader.read());
        } catch (e) {
          const isAbort = e && e.name === "AbortError";
          this._writeError(
            `stream read failed: ${e && e.message ? e.message : e}`,
            timedOut ? "timeout" : isAbort ? "abort" : "error"
          );
          return;
        }
        if (done) break;
        if (!value || value.length === 0) continue;
        const metaBase = first ? { ok: resp.ok, headers } : {};
        const cont = await this._emitMessage(resp.status, metaBase, value);
        first = false;
        if (!cont) return; // worker abandoned the stream (closed / errored)
        // Wait for the worker to consume this message and request the next.
        const ack = await this._awaitWorker(S_CHUNK_ACK);
        if (ack === S_IDLE) return;
      }
      // End of stream.
      Atomics.store(this.ctrl, C_LENGTH, 0);
      Atomics.store(this.ctrl, C_STATE, S_RESP_END);
      Atomics.notify(this.ctrl, C_STATE);
    } catch (e) {
      try {
        this._writeError(e && e.message ? e.message : String(e), "error");
      } catch (_) {
        /* SAB unusable; nothing else we can do */
      }
    } finally {
      if (timer) clearTimeout(timer);
      this._busy = false;
    }
  }
}

// installBridge wires a Worker's messages to a Bridge instance. The worker is
// expected to post {type:"pcw_sab", control, data} once (and again on a data
// realloc), then {type:"pcw_rpc"} for each request (the nudge; data in the SAB).
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
  C_CAP,
  S_IDLE,
  S_REQ_READY,
  S_RESP_CHUNK,
  S_RESP_END,
  S_RESP_ERROR,
  S_CHUNK_ACK,
  S_REALLOC_REQ,
  META_ZONE,
};

// Exported for unit testing the windowing logic without a browser.
export { Bridge };
