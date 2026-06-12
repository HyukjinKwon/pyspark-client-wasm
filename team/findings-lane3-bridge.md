<!-- SPDX-License-Identifier: Apache-2.0 -->

# Lane 3 findings - the SAB / Atomics blocking bridge

Owner: lane 3 (Pyodide runtime + Atomics/SharedArrayBuffer bridge + JupyterLite).
Status: **hardening pass complete.** v0 design + Python channel + JS glue + local
tests, PLUS (this pass): real JupyterLite-kernel integration, dynamic buffer
sizing (windowed transfer + realloc), and typed error/timeout propagation across
the SAB boundary. The browser end-to-end path is still unverified (needs a real
cross-origin-isolated browser + lane 5's Envoy) - see "needs a browser" below.
This is the riskiest seam, so the protocol is spelled out below for lane 1
(consumes `SyncChannel`) and lane 5 (serves the headers + proxy).

## HARDENING PASS (2026-06-12) - what changed

1. **JupyterLite-kernel integration (was the #1 open item; now solved).** The
   pyodide kernel runs its OWN ES-module worker (coincident-based when
   cross-origin isolated, comlink otherwise) - `worker_bootstrap.js` was a
   standalone harness that we cannot substitute. New non-invasive integration in
   two halves:
   - **Page side:** `jupyterlite/pcw_kernel_bridge.js` wraps the global `Worker`
     constructor *before* JupyterLite boots, so every kernel worker gets a
     `Bridge` (from `worker/bridge.js`) attached. It does the real cross-origin
     `fetch` + SAB writeback. It only reacts to our namespaced envelope
     `{__pcw__:{...}}` and ignores the kernel's own framing (so the two coexist).
   - **Worker side:** `worker/kernel_bootstrap.py` + a new `transport="kernel"`
     mode in `_AtomicsBackend`. `SabSyncChannel` auto-detects the kernel worker
     (Pyodide, no `js.__pcw_register_sab` hook) and posts the namespaced
     envelopes instead of `{type:"pcw_rpc"}`. **No notebook code beyond
     `pcw.install()` is required.**
   - **COOP/COEP fallback:** `jupyterlite/coi-serviceworker.js` (clean-room
     reimplementation of the known technique) injects COOP/COEP via a service
     worker + one-time reload, for header-less hosts (GitHub Pages). A hosting
     matrix is in `jupyterlite/README.md`.
2. **Dynamic buffer sizing / large results (was the 16 MiB open risk).** The
   response side now uses **bounded-window transfer**: the main thread writes
   `min(remaining, payload_capacity)` bytes per `RESP_CHUNK`, sets `meta.more`
   while bytes remain, and the worker acks each window (existing CHUNK_ACK
   ping-pong) and reassembles. A single unary body *or* one stream chunk larger
   than the data SAB is delivered across windows with NO realloc. A realloc path
   also exists for oversized *requests* (`_grow_data_sab` allocates a bigger SAB
   and re-announces it). The fixed 16 MiB ceiling is gone.
3. **Typed error + timeout propagation.** The main thread tags transport-error
   meta with `kind` (`timeout`/`abort`/`error`); the worker maps these to
   `TransportTimeout` / `TransportAborted` / `TransportError`. HTTP errors are
   NOT transport failures: the backend returns a valid `HttpResponse` with the
   non-200 `status` AND the response `headers`, so lane 1 raises the right
   `SparkConnectGrpcException` (resolving open question #2 - headers are now
   populated; see below).

New/changed files this pass: `worker/sab_channel.py` (windowing, realloc, kernel
transport, error kinds, `TransportAborted`), `worker/bridge.js` (windowed emit,
header capture, error kinds), `worker/worker_bootstrap.js` (realloc re-announce,
capacity publish), `worker/kernel_bootstrap.py` (new), `jupyterlite/
pcw_kernel_bridge.js` (new), `jupyterlite/coi-serviceworker.js` (new),
`jupyterlite/run_python_bridge.js` (Shape B now drives the real kernel execute),
`jupyterlite/README.md` (kernel wiring + hosting matrix), `tests/
test_sab_atomics_backend.py` (new - 11 tests, fake-js handshake).

## What lane 1 and lane 5 must know in one paragraph

Lane 1 calls `SyncChannel.unary(...) -> HttpResponse` and
`SyncChannel.server_stream(...) -> Iterator[bytes]` and gets **blocking**
behaviour - exactly the gRPC-python calling convention, just synchronous. Lane 1
owns grpc-web framing of the request body and parsing of the response body
(frames + trailer). Lane 3 moves bytes and never inspects them. Lane 5 must serve
the JupyterLite page with `COOP: same-origin` + `COEP: require-corp` (so
`crossOriginIsolated === true`) and configure Envoy CORS to allow the lite
origin; otherwise SharedArrayBuffer doesn't exist and nothing blocks.

## Files

| File | Side | Role |
|---|---|---|
| `pyspark_connect_web/worker/sab_channel.py` | Python (worker) | `SabSyncChannel` (the `SyncChannel`) + `_AtomicsBackend` + pluggable `SyncBackend` for tests |
| `pyspark_connect_web/worker/bridge.js` | JS (main thread) | reads request from SAB, does `fetch`, writes response back, drives streaming |
| `pyspark_connect_web/worker/worker_bootstrap.js` | JS (worker) | loads Pyodide, micropip-installs the wheel, allocates SABs, hands them to the main thread |
| `pyspark_connect_web/jupyterlite/*` | config | `jupyter-lite.json`, `_headers` (COOP/COEP), build `README.md`, `demo.ipynb` |
| `tests/test_sab_channel.py` | tests | channel behaviour against a fake backend (no browser/grpcio/net) |

## SAB layout (authoritative - mirrored by value in all three of the above)

Two SharedArrayBuffers, allocated in `worker_bootstrap.js` and shared with the
main thread via a one-time `postMessage({type:"pcw_sab", control, data})`.

### control SAB - `Int32Array`, 8 slots (32 bytes)

| Index | Const | Meaning |
|---|---|---|
| 0 | `C_STATE`  | handshake state word; the worker `Atomics.wait`s on this |
| 1 | `C_LENGTH` | byte length of the valid region currently in the data SAB |
| 2 | `C_STATUS` | HTTP status of the response (unary, and first stream chunk) |
| 3 | `C_SEQ`    | chunk sequence counter (streaming; reserved for debugging) |
| 4 | `C_GEN`    | request generation; guards against stale wakeups across RPCs |
| 5 | `C_CAP`    | current data-SAB capacity in bytes (worker publishes after alloc/realloc) |
| 6-7 | -        | reserved |

### data SAB - `Uint8Array`, default 16 MiB (now growable + windowed)

All multi-byte ints are **little-endian u32**.

```
request (worker -> main):
  [u32 header_len][header_json bytes][u32 body_len][body bytes]
  header_json = {"kind","url","headers","timeout","gen"}
  body        = lane-1-framed grpc-web request bytes (opaque to lane 3)
  -> if header+body exceeds capacity the worker reallocs a bigger data SAB
     (_grow_data_sab) and re-announces it before writing.

response window (main -> worker), per RESP_CHUNK write:
  [u32 meta_len][meta_json bytes][payload bytes]
  meta_json   = {"more":bool}  (+ first window: {"headers":{...},"ok":bool,"status" via C_STATUS})
              | {"message":"...","kind":"timeout"|"abort"|"error"}  on error
  payload     = up to (capacity - META_ZONE - 4) bytes of the logical payload
```

**Large results - SOLVED via bounded-window transfer.** A logical payload (unary
body, or one stream chunk) larger than the data region is split into successive
*windows*. Each window sets `meta.more=true` until the last window of that
payload; the worker reads a window, and while `more` it acks (CHUNK_ACK) and
waits for the next. The 16 MiB region is now just the *window* size, not a hard
result ceiling. Reassembly is exact (unit-tested with a 50 KB body over a
~512-byte window). The first window of a unary/stream-first message also carries
the response `headers` so lane 1 can read grpc-status-in-headers. `META_ZONE`
(4 KiB) is reserved at the front so meta never collides with payload regardless
of size.

## STATE machine (the Atomics handshake)

`C_STATE` values:

| Value | Const | Direction | Meaning |
|---|---|---|---|
| 0 | `S_IDLE`       | - | worker owns the buffer; safe to write a new request |
| 1 | `S_REQ_READY`  | worker->main | request written; main should `fetch` |
| 2 | `S_RESP_CHUNK` | main->worker | a payload (full response, or one stream chunk) is in the data SAB |
| 3 | `S_RESP_END`   | main->worker | stream finished, no payload |
| 4 | `S_RESP_ERROR` | main->worker | transport failure; `meta_json.{message,kind}` has detail |
| 5 | `S_CHUNK_ACK`  | worker->main | worker consumed a window/chunk, requests the next |
| 6 | `S_REALLOC_REQ`| main->worker | reserved: main asks worker to grow the data SAB |

### unary

```
worker: write request bytes
        Atomics.store(STATE, REQ_READY); Atomics.notify
        postMessage({type:"pcw_rpc"})            // nudge main (it can't wait())
        Atomics.wait(STATE, REQ_READY)           // BLOCKS the worker thread
main:   on pcw_rpc -> read request, fetch(url, {method:POST, body, headers})
        write [meta][full body]; store STATUS, LENGTH
        Atomics.store(STATE, RESP_CHUNK); Atomics.notify
worker: wakes, reads status+body -> HttpResponse
        Atomics.store(STATE, IDLE); Atomics.notify
```

### server stream

```
worker: same request start; parks on Atomics.wait(STATE, REQ_READY)
main:   fetch; for each reader.read() chunk:
          write [meta][chunk]; store STATUS (first only), LENGTH
          Atomics.store(STATE, RESP_CHUNK); Atomics.notify
          await worker ack (waitAsync on STATE leaving RESP_CHUNK)
worker: per wake on RESP_CHUNK: copy chunk, yield it to lane 1
          Atomics.store(STATE, CHUNK_ACK); Atomics.notify
          parks on Atomics.wait(STATE, CHUNK_ACK)   // next chunk
main:   at end of body: store(STATE, RESP_END); notify
worker: on RESP_END -> generator returns; store(STATE, IDLE)
```

The worker alternates the value it parks on: `REQ_READY` for the first chunk,
then `CHUNK_ACK` for each subsequent one (it is the value the worker itself
wrote, and the main thread is what moves us off it). `_wait()` in the Python
backend takes the `expect_from` value explicitly for this reason.

### timeout

`timeout` (seconds) is passed through `header.timeout`. Two enforcement points:
- **worker**: `Atomics.wait(..., ms)` returns `"timed-out"` -> `TransportTimeout`.
- **main**: an `AbortController` aborts the `fetch` after `timeout*1000` ms.

Both are armed so a hung fetch and a hung wait are each bounded. For streams the
timeout is re-armed per chunk gap (matches gRPC per-message expectation); revisit
if lane 1 wants a single overall deadline instead.

## The blocking guarantee (DECISIONS.md #5)

During a blocking RPC the worker thread sits in `Atomics.wait` and never returns
to its event loop, so Python (PySpark `.collect()`) is genuinely synchronous. The
`postMessage` nudge is queued *before* the wait, so the main thread receives it
even though the worker is blocked. This is the whole trick and the reason
COOP/COEP is non-negotiable.

## What is testable locally vs. needs a real browser

**Locally (CPython, no browser/grpcio/net) - covered by `tests/test_sab_channel.py`:**
- `SabSyncChannel` satisfies the `SyncChannel` Protocol.
- `unary` returns an `HttpResponse`; request marshalling (url join, headers,
  body, timeout) is correct; non-`HttpResponse` backend output is rejected.
- `server_stream` yields chunks in order, is a lazy generator (one pull at a
  time - important for prompt mid-stream-disconnect detection), surfaces a
  mid-stream error, handles the empty stream.
- Timeout propagation (`TransportTimeout`) for both unary and streaming, using a
  fake clock backend.
- Construction fails clearly off-Pyodide with no injected backend.

**Also covered now (CPython, fake `js`) - `tests/test_sab_atomics_backend.py`:**
- The full SAB *protocol* of `_AtomicsBackend` against a scripted main thread
  (the fake collapses two-thread Atomics into one thread - it validates window
  framing, the ack sequence, and STATE transitions, NOT OS-level blocking).
- Payload larger than the initial buffer -> multi-window reassembly (exact).
- Multi-message server stream with a mid-stream message that is itself windowed.
- Realloc path: an oversized *request* grows the data SAB and re-announces it.
- Error mapping: `kind` -> `TransportError`/`TransportTimeout`/`TransportAborted`.
- `Atomics.wait` timeout -> `TransportTimeout`; `crossOriginIsolated` guard.
- Kernel transport posts the namespaced `{__pcw__:{...}}` envelope, not the
  standalone `{type:"pcw_rpc"}`.

**Needs a real cross-origin-isolated browser (NOT covered here - lane 5 e2e):**
- The *genuine* `Atomics.wait`/`notify` blocking between worker and main thread
  (the unit fake collapses it; only a browser proves the worker truly parks and
  `.collect()` is synchronous - DECISIONS.md #5).
- `bridge.js` real `fetch` + `Atomics.waitAsync` on the main thread.
- `pcw_kernel_bridge.js` wrapping the kernel's `Worker` and the kernel worker
  picking up `transport="kernel"`; the namespaced envelope surviving alongside
  coincident/comlink framing.
- `coi-serviceworker.js` flipping `crossOriginIsolated` to true on GitHub Pages.
- `worker_bootstrap.js` Pyodide load + micropip install of the wheel.
- Window/realloc behaviour against real Arrow result sizes + throughput (the
  16 MiB window is a tuning knob, no longer a correctness ceiling).

## Open questions / coordination asks

1. **JupyterLite kernel integration - RESOLVED this pass.** We do NOT patch or
   fork the pyodide kernel. Page-side `pcw_kernel_bridge.js` wraps the global
   `Worker` (load it before the JupyterLite bundle); worker-side
   `transport="kernel"` posts a namespaced envelope the kernel's framing
   ignores. The coi-serviceworker fallback (path (b)) is provided for header-less
   hosts AND we keep header-based isolation for hosts that can set headers - the
   two are not exclusive (hosting matrix in `jupyterlite/README.md`). **ACTION
   lane 5:** the e2e/static host must inject the two `<script>` tags before the
   app bundle (template snippet in the README); confirm your build can do this.
2. **`HttpResponse.headers` - now populated.** The Atomics backend now copies the
   response HTTP headers into `HttpResponse.headers` (carried in the first
   window's meta). This makes lane 1's `_trailers_from_headers` fallback work for
   grpc-status-in-headers (empty unary, HTTP-error-with-status). Body trailers
   still ride in the body frame as before. **No action needed from lane 1** - this
   is strictly additive (you already read `resp.headers` defensively). Flag me if
   header *casing* matters: `fetch` lowercases header names, so you receive
   `grpc-status`/`grpc-message` lowercased (your `_trailers_from_headers` already
   lowercases, so we match).
3. **Reattach (DECISIONS.md #6).** Recovery is lane 1's iterator logic; lane 3
   just needs to surface a broken stream promptly. The lazy-generator test
   guards that chunks aren't buffered. When lane 1 issues `ReattachExecute`, it's
   a fresh `server_stream` call - no special bridge state. Confirm that's the
   model you expect.
4. **One in-flight RPC per worker.** The bridge enforces a single outstanding
   RPC (matches a blocked worker thread). PySpark Connect's reattachable iterator
   is sequential per query, so this should hold; flag if any code path issues
   concurrent stub calls from one worker.
5. **Data-region size / large results - SOLVED** via bounded-window transfer +
   request-side realloc (see the data-SAB section). The window size (default
   16 MiB) is now a throughput knob, not a correctness ceiling; lane 4's
   SPARK-53525 chunking and lane 5's server config no longer risk a hard failure
   on a big frame. Flag if a *single* window of 16 MiB is a memory problem on
   constrained tabs and I'll lower the default.
6. **Error taxonomy across the boundary.** Lane 3 now raises
   `TransportError`/`TransportTimeout`/`TransportAborted` (all subclasses of
   `TransportError`/`RuntimeError`) for hard transport failures, distinct from
   HTTP/gRPC errors (which flow through as a normal `HttpResponse` for lane 1 to
   turn into `SparkConnectGrpcException`). **ACTION lane 1:** confirm you let a
   raw `TransportError` propagate out of `unary`/`server_stream` (PySpark will
   surface it as the `.collect()` failure cause). If you'd rather I wrap it as a
   `SparkConnectGrpcException` with a synthetic UNAVAILABLE status so PySpark's
   reattach logic treats a dropped connection uniformly, say so - that's a
   one-line change on my side but it's your call since you own the gRPC mapping.
