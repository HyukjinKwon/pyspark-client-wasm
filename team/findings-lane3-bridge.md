<!-- SPDX-License-Identifier: Apache-2.0 -->

# Lane 3 findings — the SAB / Atomics blocking bridge

Owner: lane 3 (Pyodide runtime + Atomics/SharedArrayBuffer bridge + JupyterLite).
Status: v0 design + Python channel + JS glue + local tests landed. The browser
end-to-end path is unverified (needs a real cross-origin-isolated browser + lane
5's Envoy). This is the riskiest seam, so the protocol is spelled out below for
lane 1 (consumes `SyncChannel`) and lane 5 (serves the headers + proxy).

## What lane 1 and lane 5 must know in one paragraph

Lane 1 calls `SyncChannel.unary(...) -> HttpResponse` and
`SyncChannel.server_stream(...) -> Iterator[bytes]` and gets **blocking**
behaviour — exactly the gRPC-python calling convention, just synchronous. Lane 1
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

## SAB layout (authoritative — mirrored by value in all three of the above)

Two SharedArrayBuffers, allocated in `worker_bootstrap.js` and shared with the
main thread via a one-time `postMessage({type:"pcw_sab", control, data})`.

### control SAB — `Int32Array`, 8 slots (32 bytes)

| Index | Const | Meaning |
|---|---|---|
| 0 | `C_STATE`  | handshake state word; the worker `Atomics.wait`s on this |
| 1 | `C_LENGTH` | byte length of the valid region currently in the data SAB |
| 2 | `C_STATUS` | HTTP status of the response (unary, and first stream chunk) |
| 3 | `C_SEQ`    | chunk sequence counter (streaming; reserved for debugging) |
| 4 | `C_GEN`    | request generation; guards against stale wakeups across RPCs |
| 5-7 | —        | reserved |

### data SAB — `Uint8Array`, default 16 MiB

All multi-byte ints are **little-endian u32**.

```
request (worker -> main):
  [u32 header_len][header_json bytes][u32 body_len][body bytes]
  header_json = {"kind","url","headers","timeout","gen"}
  body        = lane-1-framed grpc-web request bytes (opaque to lane 3)

response (main -> worker), per write:
  [u32 meta_len][meta_json bytes][payload bytes]
  meta_json   = {} or {"ok":bool} or {"message": "..."} on error
  payload     = response bytes (unary: full body; stream: one chunk), opaque
```

16 MiB is the default payload region. **Open risk:** a single Arrow result chunk
larger than the data region. v0 assumes server chunking keeps individual
grpc-web frames under 16 MiB; if not, we need a length-prefixed multi-write
spill (write `min(remaining, region)` bytes per RESP_CHUNK and reassemble in the
worker). Lane 4's chunk-splitting (SPARK-53525) and lane 5's server config
affect this — flag if you see chunks near the limit.

## STATE machine (the Atomics handshake)

`C_STATE` values:

| Value | Const | Direction | Meaning |
|---|---|---|---|
| 0 | `S_IDLE`       | — | worker owns the buffer; safe to write a new request |
| 1 | `S_REQ_READY`  | worker->main | request written; main should `fetch` |
| 2 | `S_RESP_CHUNK` | main->worker | a payload (full response, or one stream chunk) is in the data SAB |
| 3 | `S_RESP_END`   | main->worker | stream finished, no payload |
| 4 | `S_RESP_ERROR` | main->worker | transport failure; `meta_json.message` has detail |
| 5 | `S_CHUNK_ACK`  | worker->main | worker consumed a chunk, requests the next |

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

**Locally (CPython, no browser/grpcio/net) — covered by `tests/test_sab_channel.py`:**
- `SabSyncChannel` satisfies the `SyncChannel` Protocol.
- `unary` returns an `HttpResponse`; request marshalling (url join, headers,
  body, timeout) is correct; non-`HttpResponse` backend output is rejected.
- `server_stream` yields chunks in order, is a lazy generator (one pull at a
  time — important for prompt mid-stream-disconnect detection), surfaces a
  mid-stream error, handles the empty stream.
- Timeout propagation (`TransportTimeout`) for both unary and streaming, using a
  fake clock backend.
- Construction fails clearly off-Pyodide with no injected backend.

**Needs a real cross-origin-isolated browser (NOT covered here — lane 5 e2e):**
- The actual `Atomics.wait`/`notify` handshake in `_AtomicsBackend`.
- `bridge.js` fetch + SAB writeback, `Atomics.waitAsync` on the main thread.
- `worker_bootstrap.js` Pyodide load + micropip install of the wheel.
- `crossOriginIsolated === true` and the `js.crossOriginIsolated` guard.
- The 16 MiB region sufficiency against real Arrow result sizes.

## Open questions / coordination asks

1. **JupyterLite kernel integration (biggest open item).** The pyodide-kernel
   runs its *own* web worker with its own message loop. `worker_bootstrap.js` is
   currently a *standalone* harness. To ship inside JupyterLite we must inject
   the SAB allocation + the `pcw_rpc` nudge handler into that kernel's worker
   (e.g. a small kernel patch or a service-worker variant). Two viable paths:
   (a) patch/extend the pyodide kernel worker; (b) use a coi-service-worker to
   set COOP/COEP and run our own bridge. Needs a decision with lanes 2/5.
2. **`_contract.py` `HttpResponse.headers`.** The Atomics backend currently
   returns `headers={}` — we do not yet copy response headers out of the SAB
   (only status). If lane 1 needs grpc-web **trailers via HTTP headers** (vs. in
   the body trailer frame), say so and I'll add a header block to the response
   meta_json. Today I assume trailers ride in the body (API_CONTRACT.md S1 step
   2 says trailers arrive as a final frame), so empty headers should be fine —
   **please confirm, lane 1.**
3. **Reattach (DECISIONS.md #6).** Recovery is lane 1's iterator logic; lane 3
   just needs to surface a broken stream promptly. The lazy-generator test
   guards that chunks aren't buffered. When lane 1 issues `ReattachExecute`, it's
   a fresh `server_stream` call — no special bridge state. Confirm that's the
   model you expect.
4. **One in-flight RPC per worker.** The bridge enforces a single outstanding
   RPC (matches a blocked worker thread). PySpark Connect's reattachable iterator
   is sequential per query, so this should hold; flag if any code path issues
   concurrent stub calls from one worker.
5. **Data-region size / large results** — see the 16 MiB note above.
