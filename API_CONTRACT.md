<!-- SPDX-License-Identifier: Apache-2.0 -->

# API_CONTRACT.md — the frozen seam

This is the **stable interface between lanes**. Do not change a signature here
without announcing it in `COORDINATION.md` first — every other lane builds
against these shapes. If you must change it, edit this file *and* append a note.

The whole project hangs off one idea: **PySpark's Connect client builds plans
and talks to the server only through a gRPC stub. We replace that stub.** Nothing
upstream of the stub (DataFrame, Column, functions, plan building) is touched.

---

## 1. The stub seam (`pyspark_connect_web/transport`)

PySpark builds, in `pyspark.sql.connect.client.core.SparkConnectClient`:

```python
self._stub = grpc_lib.SparkConnectServiceStub(self._channel)
```

We install a **duck-typed replacement**. It must expose these 10 callables with
the exact gRPC-Python calling convention `fn(request, *, metadata=None,
timeout=None)`. Streaming methods return an **iterator** of response protos;
unary methods return a single response proto.

| Method | Kind | Request → Response proto |
|---|---|---|
| `ExecutePlan`        | server-streaming | `ExecutePlanRequest` → iter[`ExecutePlanResponse`] |
| `ReattachExecute`    | server-streaming | `ReattachExecuteRequest` → iter[`ExecutePlanResponse`] |
| `ReleaseExecute`     | unary | `ReleaseExecuteRequest` → `ReleaseExecuteResponse` |
| `AnalyzePlan`        | unary | `AnalyzePlanRequest` → `AnalyzePlanResponse` |
| `Config`             | unary | `ConfigRequest` → `ConfigResponse` |
| `Interrupt`          | unary | `InterruptRequest` → `InterruptResponse` |
| `AddArtifacts`       | client-streaming | iter[`AddArtifactsRequest`] → `AddArtifactsResponse` |
| `ArtifactStatus`     | unary | `ArtifactStatusesRequest` → `ArtifactStatusesResponse` |
| `ReleaseSession`     | unary | `ReleaseSessionRequest` → `ReleaseSessionResponse` |
| `FetchErrorDetails`  | unary | `FetchErrorDetailsRequest` → `FetchErrorDetailsResponse` |

Protos come from `pyspark.sql.connect.proto` (already pure-python; do **not**
vendor your own copies). Serialize with `request.SerializeToString()`, parse with
`ResponseType.FromString(bytes)`.

### What the stub does internally
1. Serialize request proto → bytes.
2. Frame as **grpc-web** (lane 1 owns framing): `[1 byte flags][4 byte big-endian
   length][message]`; trailers arrive as a final frame with flag bit `0x80`.
3. Hand the framed bytes + metadata (as HTTP headers) to the **blocking byte
   transport** (the `SyncChannel` below — lane 3 owns it).
4. Decode response frames → response protos; raise `SparkConnectGrpcException`
   (from `pyspark.errors`) on a non-OK grpc-status trailer.

### The byte-level transport boundary (lane 1 ⟷ lane 3)
Lane 1 never touches the browser. It calls this **synchronous** interface that
lane 3 implements (Atomics+SharedArrayBuffer under the hood, so it blocks):

```python
class SyncChannel(Protocol):
    def unary(self, path: str, body: bytes, headers: dict[str, str],
              timeout: float | None) -> "HttpResponse": ...
    def server_stream(self, path: str, body: bytes, headers: dict[str, str],
                       timeout: float | None) -> "Iterator[bytes]": ...
    #   yields raw grpc-web frame chunks as they arrive off the wire

@dataclass
class HttpResponse:
    status: int                 # HTTP status
    headers: dict[str, str]
    body: bytes                 # full grpc-web response body (frames + trailer)
```

`path` is the gRPC path, e.g. `"/spark.connect.SparkConnectService/ExecutePlan"`.
This Protocol lives in `pyspark_connect_web/_contract.py` (integrator-owned).

---

## 2. The patch entry point (`pyspark_connect_web`, lane 2)

```python
import pyspark_connect_web as pcw
pcw.install()        # idempotent; monkey-patches SparkConnectClient once
```

`install()` must:
- Verify the running `pyspark` version is in the supported range (see DECISIONS.md).
- Replace stub construction so `SparkConnectClient` uses lane 1's stub backed by
  lane 3's `SyncChannel`.
- Teach the connection parser a web endpoint. Agreed scheme:
  `sc://<envoy-host>:<port>/;transport=grpcweb` (and a plain
  `https://<envoy-host>` shorthand). Returns a normal `SparkSession`.

After `install()`, this must work unchanged:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

---

## 3. Arrow results (`pyspark_connect_web/arrow`, lane 4)

`ExecutePlanResponse` messages carry `arrow_batch` chunks (and, per SPARK-53525,
a batch may be split across chunks — reassemble by `row_count`/offset before
decoding). Lane 4 exposes:

```python
def decode_arrow_batches(responses: Iterable[ExecutePlanResponse]) -> "pandas.DataFrame": ...
def encode_local_relation(pdf: "pandas.DataFrame") -> bytes:  # Arrow IPC for createDataFrame
    ...
```

Decoding uses Pyodide's `pyarrow` (>=22; in the package list). If the
PySpark client's own `_to_pandas` path already works under Pyodide, lane 4's job
shrinks to *verifying* it and patching only what breaks — measure first.

---

## 4. Runtime / sync bridge (`pyspark_connect_web/worker` + `jupyterlite`, lane 3)

- Python runs in a **Web Worker**. The main thread holds a `SharedArrayBuffer`.
- A blocking call writes the request to the SAB, posts to the main thread (which
  does the real `fetch`), and `Atomics.wait()`s the worker until the response is
  written back. This is what makes `.collect()` synchronous.
- **Cross-origin isolation (COOP/COEP) is mandatory** for SAB — see DECISIONS.md.
- Provides the JS glue, the micropip-installable wheel, and the JupyterLite
  kernel config + demo notebook.

---

## 5. Proxy / e2e / docs (lane 5)

- Envoy `grpc_web` filter in front of a Spark Connect server (Spark 4.x).
- `docker compose up` brings up Connect server + Envoy with COOP/COEP headers.
- Headless-browser e2e that loads the JupyterLite page and asserts the v0 matrix
  (see DECISIONS.md "v0 done = ...").
