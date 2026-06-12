<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture - pyspark-connect-web

**PySpark in JupyterLite.** Run the *real* PySpark Connect Python client inside a
browser (JupyterLite/Pyodide), talking to a Spark Connect server through a
grpc-web transport. User PySpark code runs unchanged.

## The one big idea

PySpark Connect's client is pure Python above a single gRPC stub: it builds
protobuf plans and calls `self._stub.ExecutePlan(req)`. We **monkey-patch only
that stub** with a grpc-web/`fetch` transport, and make calls *blocking* via a
Web Worker + `Atomics`/`SharedArrayBuffer` bridge so `.collect()` returns data
synchronously. Plan building, DataFrame, Column, functions, retry policy, and the
reattachable-execute iterator all stay as upstream PySpark.

## End-to-end picture

```
  user PySpark code (unchanged)
        |
  pyspark.sql.connect.DataFrame / functions          <- untouched upstream
        |  builds protobuf plan
  SparkConnectClient  --patched (the components)-->  grpc-web stub (the components)
                                                  |  frames: [flags][len][msg], trailer 0x80
                                                  v
                              SyncChannel (the components): Atomics + SharedArrayBuffer
                                                  |  worker blocks; main thread does fetch()
                                                  v  HTTP/1.1 grpc-web over fetch
                              +-----------------------------------------+
                              |  Envoy (the components)                          |
                              |   :8081  grpc_web filter + CORS          |
                              |   :8000  static JupyterLite + COOP/COEP  |
                              +-----------------------------------------+
                                                  |  gRPC / HTTP2
                                                  v
                              Spark Connect server (Spark 4.x, :15002)
        ^
  Arrow IPC result batches --decode (the components)--> pandas
```

## Lane map

| Lane | Area | Files |
|------|------|-------|
| 1 | grpc-web stub + framing | `pyspark_connect_web/transport/*` |
| 2 | monkey-patch + integration | `pyspark_connect_web/__init__.py`, `patch.py`, `_contract.py` |
| 3 | Pyodide sync bridge + JupyterLite | `pyspark_connect_web/worker/*`, `jupyterlite/*` |
| 4 | Arrow result decoding | `pyspark_connect_web/arrow/*` |
| 5 | Envoy proxy + e2e + docs | `deploy/*`, `tests/e2e/*`, `docs/*`, `README.md` |

## The stub seam (frozen - the transport contract section 1)

The replacement stub exposes 10 callables with the gRPC-Python calling
convention `fn(request, *, metadata=None, timeout=None)`. Streaming methods
(`ExecutePlan`, `ReattachExecute`) return an iterator of response protos; unary
methods return one proto. Protos come from `pyspark.sql.connect.proto` - we do
not vendor copies.

Below the stub is the byte boundary: the stub calls the `SyncChannel`
(`unary` / `server_stream`), which is **synchronous** - it blocks the worker via
`Atomics.wait` until the main-thread `fetch` writes the response back through a
`SharedArrayBuffer`. That blocking is what lets `.collect()` stay synchronous.

## grpc-web wire framing (the components)

A grpc-web message is length-prefixed frames: `[1 byte flags][4 byte big-endian
length][payload]`. Flag `0x01` = compressed (rejected), `0x80` = trailer frame
whose payload is an HTTP-style header block carrying `grpc-status` /
`grpc-message`. A request can be HTTP 200 yet carry `grpc-status: 13`; the turns a non-OK trailer into `SparkConnectGrpcException`. Envoy's `grpc_web`
filter translates between this browser-friendly framing and upstream gRPC.

## Why cross-origin isolation is mandatory

`SharedArrayBuffer` - the backbone of the blocking bridge - is only available
when the page is **cross-origin isolated**. That requires the page be served
with:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Envoy's static host (`:8000`) sets both (`deploy/envoy.yaml`). The e2e harness
asserts `crossOriginIsolated === true` before doing anything else; if it is
false, the entire bridge is dead, so this is the first checklist gate.

## Reattachable execute

The client issues `ExecutePlan` + `ReattachExecute` + `ReleaseExecute`. If a
stream breaks mid-flight, PySpark's own `ExecutePlanResponseReattachableIterator`
reattaches from the last seen response - but only if our stub implements all
three calls, not just the initial stream. The e2e harness kills a stream
mid-flight and asserts the result still completes.

## Invariants

1. No `grpcio` inside `pyspark_connect_web/` - ever (not in Pyodide).
2. We patch, we do not fork pyspark.
3. Pinned pyspark range `>=4.0`.
4. COOP/COEP mandatory.
5. `.collect()` stays blocking.
6. Reattachable execute in scope.
7. Arrow results byte/row-exact vs a native reference.

## Where things deviate from a normal Spark Connect setup

* The transport is grpc-web over `fetch`, not native gRPC - hence Envoy.
* Calls are made blocking from a Web Worker; the main thread does the network.
* The native reference client (`tests/e2e/reference.py`) talks plain gRPC on
  `:15002` and *is* allowed to use `grpcio` (it is outside the package).
