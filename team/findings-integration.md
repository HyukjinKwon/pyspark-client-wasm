<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings-integration.md — real end-to-end round-trip

INTEGRATION agent, 2026-06-12. Goal: prove the Python vertical works against a
REAL Spark Connect engine (no browser, no Docker, no network) through the real
grpc-web framing, and fix what breaks. First time anything ran against a real
server instead of fakes.

## What I built (`tests/integration/`, test-only; grpcio allowed here)

- `conftest.py` — session-scoped fixture starting a REAL in-process Spark Connect
  gRPC server: a regular JVM `SparkSession` with
  `spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin`, bound to an
  ephemeral FREE port via `spark.connect.grpc.binding.port` (never 15002, held by
  a leftover JVM), `local[2]`, bundled jars, known auth token. Skips cleanly (not
  errors) when grpcio/Java/JVM-start unavailable so CI without a JVM stays green.
- `bridge.py` — `GrpcWebBridgeChannel`, a pure-Python grpc-web <-> gRPC bridge
  standing in for Envoy, implementing lane 3's `SyncChannel`. Per call:
  (1) decodes the grpc-web request body with lane 1's OWN
  `transport.framing.iter_frames`; (2) forwards raw proto bytes to the real
  server over `grpc.insecure_channel(...).unary_unary`/`.unary_stream` with
  byte-passthrough (de)serializers; (3) re-encodes each gRPC response as a
  grpc-web data frame + a `0x80` trailer frame (`encode_message`/`encode_trailers`).
  Real framing in BOTH directions. PySpark's `ChannelBuilder.metadata()` omits
  the token (native rides it in grpc call-creds, no grpc-web analogue), so the
  bridge injects `authorization: Bearer <token>`. Hop-by-hop grpc-web headers
  stripped before forwarding.
- `test_real_round_trip.py` — wires `pcw.install()` + `set_channel_factory(bridge)`,
  builds `remote("sc://localhost:<port>/;transport=grpcweb")`, runs the v0 matrix
  incl. EXACT parity vs the native Connect client on the SAME server.

## v0 matrix — what GENUINELY passes against the real server (7/7)

| Item | Result |
|---|---|
| `spark.range(10).collect()` -> 10 rows | PASS (ids 0..9) |
| `spark.sql("select 1 as x").collect()` | PASS |
| `range(100).filter.select.groupBy.agg.toPandas()` vs native | PASS — assert_frame_equal EXACT (DECISIONS.md #7) |
| `createDataFrame(pdf)` round-trips (lane4 encode_local_relation) | PASS — int/str/float; + native parity variant |
| Large result streams many ExecutePlanResponses | PASS — 200k rows; traced 55 data frames + 1 trailer in one ExecutePlan stream; aggregate parity vs native |
| Mid-stream disconnect recovers via ReattachExecute (DECISIONS.md #6) | PASS — after the fix below |

Parity byte/row-exact for these queries. Lane 4's tz/struct-mode caveat did NOT
bite (no timestamp/struct columns in the matrix) — untested, not disproven.

## REAL BUG FOUND AND FIXED

A dropped ExecutePlan stream did NOT recover via ReattachExecute — it surfaced
the error to the user. DECISIONS.md #6 was not actually satisfied.

- File: `pyspark_connect_web/transport/grpcweb.py`, `GrpcWebStub._stream_responses`.
- Root cause: when a server-stream ended with NO trailer frame (real mid-result
  disconnect), lane 1 RAISED `SparkConnectGrpcException`, assuming that triggers
  reattach. It does the opposite. PySpark's `ExecutePlanResponseReattachableIterator`
  recovers a broken stream only when the underlying iterator ends CLEANLY
  (StopIteration) before a `ResultComplete` — that is what makes `_has_next` issue
  ReattachExecute from the last response_id. Its retry path
  (`DefaultPolicy.can_retry`) only retries `grpc.RpcError` (UNAVAILABLE, or
  INTERNAL + INVALID_CURSOR.DISCONNECTED); `SparkConnectGrpcException` is not a
  `grpc.RpcError`, so the raise propagated to the user and ReattachExecute never
  fired. Verified live: cutting after 3 frames previously failed with
  "stream ... ended without a trailer frame", CALLS={ExecutePlan:1} (no reattach).
- Fix: on a trailer-less end (incl. a trailing partial frame — same severed-wire
  condition) `_stream_responses` now RETURNS (StopIteration) instead of raising.
  Present-but-non-OK trailer still raises (real server error); compressed frame
  still raises. After fix the same injection recovers all 50k rows with
  CALLS={ExecutePlan:1, ReattachExecute:1}.
- Guard tests:
  - Live: `test_real_round_trip.py::test_midstream_disconnect_recovers_via_reattach`
    (real server, FaultBridge cuts ExecutePlan mid-stream, asserts full recovery
    AND ReattachExecute was actually called).
  - Unit (corrected): in `tests/test_grpcweb_stub.py`,
    `test_server_stream_dropped_without_trailer_raises` -> `..._ends_cleanly_for_reattach`
    and `..._trailing_partial_frame_raises` -> `..._ends_cleanly_for_reattach` now
    assert clean StopIteration. These two were the only tests encoding lane 1's
    wrong "raise to reattach" assumption; the unary missing-trailer test is
    unchanged (unary still raises — no reattach for a unary call).

No change needed to `arrow/*`, `patch.py`, framing, or unary/trailer handling —
they round-tripped against the real server unchanged.

## Test-isolation bug I introduced and fixed

My session-scoped fixture set `SPARK_CONNECT_AUTHENTICATE_TOKEN` so the JVM could
read it. A leaked token flips `DefaultChannelBuilder.secure` to True
(`secure = use_ssl or token is not None`), breaking
`tests/test_install.py::test_parser_accepts_web_scheme_canonical` (asserts http,
not https) when integration ran first. Fix: fixture restores the prior env value
right after the JVM is up; clients get the token explicitly (web via bridge
metadata, native via `token=` param). Verified clean in both orderings.

## State

- 87 existing unit tests: still PASS.
- 7 new integration tests: PASS against the real server.
- Full suite (`pytest -q --ignore=tests/e2e`, no PYTHONPATH, CI-equivalent): 94 passed.
- grpcio guard: no `grpc` import anywhere in `pyspark_connect_web/` (grep clean);
  grpcio used ONLY in `tests/integration/bridge.py`.

## What still fails / untested (honest gaps)

- Timezone/struct-mode parity is UNTESTED, not proven. Lane 4 applies no
  session-tz localization or struct handling mode; v0 matrix has no timestamp/
  struct columns so it never triggered. A timestamp/nested-struct query could
  still diverge from native.
- AddArtifacts (client-streaming) NOT exercised. The bridge rejects a multi-frame
  request body loudly (read-path matrix never hits it); the lane-1 concatenate-to-
  unary lowering is unverified against a real server.
- The SAB/Atomics bridge (lane 3) is BYPASSED. The test injects the synchronous
  grpcio bridge directly via set_channel_factory, proving framing/transport/arrow/
  patch against a real engine but NOT the browser Web Worker blocking path
  (lane 3 / lane 5 headless e2e).
- Single Spark version only (4.0.0). The 4.1 chunked-arrow (SPARK-53525) path is
  handled in code but cannot be exercised here (4.0 never chunks).
