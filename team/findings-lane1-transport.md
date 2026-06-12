<!-- SPDX-License-Identifier: Apache-2.0 -->

# Lane 1 — grpc-web stub + wire framing — findings

Owner: hyukjin.kwon@databricks.com. Owns `pyspark_connect_web/transport/*`.

## What landed (v0)

| File | Purpose |
|------|---------|
| `pyspark_connect_web/transport/framing.py` | grpc-web frame encode/decode + trailer parsing |
| `pyspark_connect_web/transport/grpcweb.py` | `GrpcWebStub` — duck-typed `SparkConnectServiceStub` replacement |
| `pyspark_connect_web/transport/__init__.py` | re-exports |
| `tests/test_transport_framing.py` | 20 framing tests (standalone, no pyspark needed) |
| `tests/test_grpcweb_stub.py` | 20 stub tests (fake in-memory SyncChannel; skip gracefully w/o pyspark) |

Status: all 56 repo tests pass (`python -m pytest tests/ -q`), pyspark 4.0.0,
Python 3.11. No `grpcio`/`grpc` import anywhere in the package (verified by grep).

## framing.py
Frame = `[1 byte flags][4-byte big-endian length][payload]`.
- `encode_message(payload, *, flags=0) -> bytes`
- `iter_frames(data) -> Iterator[Frame]`; `Frame(flags, payload)` has `.is_trailer` (0x80) and `.is_compressed` (0x01).
- `parse_trailers(payload) -> dict` — lowercased keys, tolerates `\r\n`/bare `\n`/blank/garbage lines.
- Extras: `encode_trailers(status, message="")`, `split_messages_and_trailer(body) -> (list[bytes], dict|None)`.
- Compressed frames rejected on decode; request sends `grpc-accept-encoding: identity`.
- `iter_frames` raises `ValueError` on truncated header/body (no silent drops).

## grpcweb.py
`GrpcWebStub(channel: SyncChannel, base_url="", *, metadata=None, default_metadata=None)`.
- All 10 methods bound in `__init__`; convention `fn(request, *, metadata=None, timeout=None)`.
- Streaming (ExecutePlan, ReattachExecute) return iterators; unary return one proto.
- Forces `content-type: application/grpc-web+proto` + `x-grpc-web: 1` (metadata cannot override); maps metadata (k,v) tuples to lowercased headers.
- Non-OK / missing / dropped-stream trailer -> `SparkConnectGrpcException` (so client reattaches; DECISIONS.md #6). Falls back to grpc-status in HTTP headers for unary.

### Subtle bug fixed (guard tests left)
Streaming reassembly originally used a generator holding a stale offset into a `bytearray` the consumer mutated mid-iteration. Rewrote as `_take_frame(buffer)` popping one whole frame from the front in place, returning `None` on a partial frame. Guards: `test_server_stream_reassembles_frames_split_across_chunks`, `test_server_stream_single_byte_chunks`, `test_server_stream_trailing_partial_frame_raises`. Matters for SPARK-53525 (batch split across chunks).

## Contract gaps / seam notes for the integrator
1. AddArtifacts is client-streaming but `SyncChannel` has only `unary()`/`server_stream()`. grpc-web has no true client streaming. I lower `AddArtifacts(iter[req])` to: frame each request, concatenate, POST via `unary()`. No new SyncChannel method needed; matches AddArtifacts usage. Raise it if lane 3 prefers a dedicated method.
2. `SparkConnectGrpcException` import: API_CONTRACT §1 says "from pyspark.errors", but in pyspark 4.0.0 it is at `pyspark.errors.exceptions.connect` (not re-exported). I import resiliently (try pyspark.errors, fall back to connect). Doc wording slightly off; no code change needed.
3. Constructor seam aligned with lane 2 (RESOLVED). Lane 2's `patch._default_stub_factory` calls `GrpcWebStub(channel.channel, metadata=list(channel.params.items()))`. I made `metadata=` the canonical channel-default keyword (kept `default_metadata=` alias). Verified by `test_lane2_default_stub_factory_builds_real_stub` (drives lane 2's real factory with a fake channel end-to-end). If lane 2 changes the call shape, that guard fails loudly.

## Out of lane / not done
- No real network/Atomics/SAB — lane 3's SyncChannel impl. Tested against a fake in-memory channel only.
- Arrow decode of ExecutePlanResponse.arrow_batch — lane 4.
- Retry/reattach orchestration is PySpark's own ExecutePlanResponseReattachableIterator (untouched); my stub provides correct ExecutePlan/ReattachExecute/ReleaseExecute and surfaces dropped streams as errors so it can recover.
