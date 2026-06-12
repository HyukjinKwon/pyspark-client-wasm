<!-- SPDX-License-Identifier: Apache-2.0 -->

# AGENTS.md — engineering reference for pyspark-connect-web

Read this with `API_CONTRACT.md` (the seam), `COORDINATION.md` (ownership +
notes log), and `DECISIONS.md` (invariants). For the pitch, see `README.md`.

## The one big idea
PySpark Connect's client is pure Python that builds protobuf plans and calls a
gRPC stub. We swap **only the stub** for a grpc-web transport, and make it
blocking with a Web Worker + Atomics/SharedArrayBuffer bridge. Plan building,
DataFrame, Column, functions, retry policy, and the reattachable-execute iterator
all stay as-is upstream.

```
  user PySpark code (unchanged)
        │
  pyspark.sql.connect.DataFrame / functions      ← untouched
        │  builds protobuf plan
  SparkConnectClient  ──patched──▶ our stub (lane 1, grpc-web framing)
                                       │ SyncChannel (lane 3, Atomics/SAB → fetch)
                                       ▼
                            Envoy grpc-web filter (lane 5)
                                       ▼
                            Spark Connect server (Spark 4.x)
        ▲
  Arrow IPC result batches ──decode──▶ pandas (lane 4)
```

## Where things live in pyspark (the symbols we patch — pin the version!)
- `pyspark.sql.connect.client.core.SparkConnectClient` — builds `self._stub`.
- `pyspark.sql.connect.proto` (`base_pb2`, `base_pb2_grpc`) — request/response protos.
- Connection parsing: `SparkSession.builder.remote(...)` → channel builder.
- Reattachable iterator: `ExecutePlanResponseReattachableIterator` (Spark 3.5+).
These are **private internals** — DECISIONS.md pins the supported pyspark range.

## Commands (fill in as lanes land)
```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest -q                      # unit tests (transport stubbed; no grpcio, no browser)
# e2e (lane 5): docker compose -f deploy/compose.yaml up  +  headless browser run
```

## Hard rules (see DECISIONS.md for the full list + guards)
- Never import `grpcio`. Never fork pyspark source.
- Cross-origin isolation (COOP/COEP) is required for SharedArrayBuffer.
- `.collect()` must stay synchronous. Reattach/Release must work. Arrow must be row-exact.

## Working style (mirrors how this repo's sibling project ran)
- Claim your row in COORDINATION.md before editing; own disjoint files.
- Append findings to the COORDINATION.md notes log and `team/findings-lane<N>-*.md`.
- When you fix a subtle bug, leave a guard test so it can't silently regress.
- Measure before optimizing; record honest numbers (no aspirational claims).
