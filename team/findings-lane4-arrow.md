<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings-lane4-arrow — Arrow result decoding + request-side Arrow

Lane 4. Owner files: `pyspark_connect_web/arrow/*`, `tests/test_arrow_results.py`.
Environment measured: Python 3.11.8, pyspark 4.0.0, pyarrow 22.0.0, pandas 2.3.3
(matches the Pyodide package list: pyarrow >= 22, pandas, protobuf >= 7).

## TL;DR

- **Decision: REIMPLEMENT the byte-level reassembly + IPC decode** (a small,
  self-contained subset), and **REUSE pyarrow's own `Table.to_pandas`** for the
  Arrow->pandas conversion. We do **not** call PySpark's `SparkConnectClient.to_pandas`.
- Request side (`encode_local_relation`) is a faithful copy of the IPC framing in
  `pyspark.sql.connect.plan.LocalRelation.plan` — proven byte-identical by test.
- SPARK-53525 chunking is handled, and degrades cleanly on pyspark 4.0.0 (whose
  proto lacks the chunk fields entirely).
- 17 tests pass, including a multi-chunk split-batch guard and integrity guards.

## Did I measure first? Yes.

PySpark 4.0.0's Arrow->pandas path lives in
`pyspark/sql/connect/client/core.py`:

- `SparkConnectClient.to_pandas(plan, observations)` (line ~932)
- `_execute_and_fetch(...)` / `_execute_and_fetch_as_iterator(...)` (line ~1539 / ~1430)
- the `arrow_batch` handling block inside the `handle_response` closure (line ~1476)

### Why NOT reuse `to_pandas` as-is

1. **Wrong input shape.** `to_pandas` takes a `pb2.Plan`, builds an
   `ExecutePlanRequest`, and calls `self._stub` itself. Our contract
   (`API_CONTRACT.md` 3) hands lane 4 an **iterable of `ExecutePlanResponse`**,
   not a client + plan. There is no live client to call `to_pandas` on.
2. **The reassembly is not callable in isolation.** The arrow-batch loop is a
   ~30-line block nested inside a closure (`handle_response`) inside
   `_execute_and_fetch_as_iterator`, interleaved with reattach/metrics/SQL-command/
   progress handling. There is no public function that takes responses -> batches.
3. **`to_pandas` makes more RPCs.** It calls `get_config_with_defaults(...)` and
   `get_configs("spark.sql.session.timeZone")` to drive struct-handling mode and
   timezone conversion — extra round trips that need a live client/config. Out of
   scope for a pure decoder.
4. **The chunking logic we need does not exist in the pinned range.** pyspark
   4.0.0's `ArrowBatch` proto has only `row_count`, `data`, `start_offset` — no
   `chunk_index` / `num_chunks_in_batch`. The SPARK-53525 reassembly code lives in
   Spark 4.1+ only. So even "copy the loop" would copy a loop that can't reassemble
   chunks. We must implement it ourselves.

### What we DO reuse

- **pyarrow IPC** (`pa.ipc.open_stream`) and **`pa.Table.to_pandas`** — the actual
  type conversion. We pass `coerce_temporal_nanoseconds=True` exactly as PySpark's
  `to_pandas` does for pyarrow >= 13, so temporal types land identically.
- The **IPC framing for the request side** is copied verbatim from
  `LocalRelation.plan` (`pa.ipc.new_stream(sink, table.schema)` then
  `write_batch` per batch). A test asserts our bytes equal that reference framing.

This keeps us inside DECISIONS.md #2 (patch, don't fork): we copy ~5 lines of
well-known IPC framing, not pyspark's plan/DataFrame/client logic.

## SPARK-53525 — Arrow result chunking (the chunked-batch case)

JIRA SPARK-53525 / PR apache/spark#52271, landed for Spark 4.1.0. When a single
Arrow IPC batch's serialized bytes exceed the gRPC message limit (notably when one
row is huge), the server splits **one batch's bytes** across several
`ExecutePlanResponse.arrow_batch` messages. Two new `proto3 optional` fields on
`ArrowBatch` drive reassembly:

| field (number) | meaning |
|---|---|
| `row_count` (1) | rows in the **fully reassembled** batch (integrity check) |
| `data` (2) | a **byte slice** of one IPC stream — NOT independently decodable |
| `start_offset` (3) | row offset where this batch begins in the overall result |
| `chunk_index` (4, optional) | 0-based position of this chunk within the batch |
| `num_chunks_in_batch` (5, optional) | total chunks the batch was split into |

A chunk is the last one when `chunk_index == num_chunks_in_batch - 1`. There is no
boolean "is_last"; the count + index encode it.

**Reassembly algorithm** (`reassemble_record_batches` in `arrow/results.py`):

```
pending = []           # buffered chunk `data`, in chunk_index order
num_records = 0        # running row offset across completed batches
for each response with an arrow_batch:
    if pending:                       # continuing a chunked batch
        assert chunk_index == len(pending)          # in order
    else:                             # first chunk of a (maybe single-chunk) batch
        assert chunk_index == 0
        if has(start_offset): assert start_offset == num_records   # no gaps
    pending.append(data)
    complete = num_chunks_in_batch in (absent, 0) or len(pending) == num_chunks_in_batch
    if not complete: continue         # wait for more chunks
    ipc = b"".join(pending); pending = []
    batches += decode pa.ipc.open_stream(ipc)
    assert decoded_rows == row_count   # integrity
    num_records += decoded_rows
assert pending == []   # not truncated mid-batch
```

The discriminator for "partial chunk vs whole batch" is
`HasField("num_chunks_in_batch")`: unset or 0 -> the response's `data` is a
complete self-contained IPC stream (classic pre-4.1 behaviour).

### Graceful degradation on pyspark 4.0.0 (CRITICAL gotcha)

pyspark 4.0.0's `ArrowBatch` proto has **no** `chunk_index` /
`num_chunks_in_batch`. Calling `msg.HasField("num_chunks_in_batch")` on that proto
raises `ValueError("unknown field")`. `arrow/results.py` wraps every presence
probe in `_has_field()`, which catches that and returns `False`. Result: on a 4.0
proto every batch is treated as whole — exactly correct, since 4.0 servers never
chunk. The same code therefore decodes both 4.0.x and future chunk-capable
streams with no version branching. There is a test for this
(`test_pre_chunking_proto_without_chunk_fields_decodes`).

## Type-mapping gotchas

- **Timestamps.** We pass `coerce_temporal_nanoseconds=True` (pyarrow >= 13) so
  `timestamp[us]` etc. coerce to pandas `datetime64[ns]` like the native client.
  **Boundary:** PySpark's full path additionally localizes `TimestampType` columns
  to `spark.sql.session.timeZone` via `_create_converter_to_pandas`. That needs a
  live client config we don't have from a bare response iterable. Lane 4 returns
  the faithful Arrow-native conversion (UTC-naive as encoded); **timezone
  localization, if required for exact parity, belongs in lane 2's integration**
  (it can wrap our DataFrame or pass the schema+tz). Flagged here so the parity
  test (DECISIONS.md #7) accounts for it.
- **Decimals.** `decimal128`/`decimal256` -> pandas `object` columns of
  `decimal.Decimal`. Round-trips exactly via pyarrow; tested.
- **Nested (list / struct / map).** Decode faithfully through `to_pandas`
  (lists -> numpy/object, structs -> dicts). PySpark's "legacy struct handling
  mode" (structs -> dict, error on duplicate field names) is again a client-config
  concern; the Arrow-native default already yields dicts, matching `legacy`/`dict`.
  Map/duplicate-field-name edge cases are deferred to lane 2 if e2e parity needs them.
- **Empty results.** An empty table serializes to an IPC stream with a schema but
  **zero record batches**. We capture `reader.schema` during reassembly so an empty
  result still returns a DataFrame with the correct column names (not a bare empty
  frame). pyarrow `to_pandas` on a 0-row table can segfault on some builds
  (SPARK-51112), so we build the empty frame by hand. Tested.

## Pyodide pyarrow version caveats

- Pyodide ships pyarrow >= 22; `coerce_temporal_nanoseconds` (>= 13) and
  `pa.ipc.open_stream`/`new_stream` are all present. We still guard the temporal
  flag behind a version check so local dev on an older wheel doesn't break.
- No pyarrow flight / dataset / compute features are used — only IPC stream
  read/write and `Table.to_pandas`/`Table.from_pandas`, all available in the
  Pyodide build.
- We never touch `grpcio`. Verified by grep over `arrow/` and the test file.

## Contract notes

- No `API_CONTRACT.md` change needed. Signatures match 3 exactly:
  `decode_arrow_batches(responses) -> pandas.DataFrame` and
  `encode_local_relation(pdf) -> bytes`.
- Added one **public helper not in the contract**: `reassemble_record_batches`
  (exported from `pyspark_connect_web.arrow`). It is purely additive — the bytes->
  `pa.RecordBatch` step exposed for testing/reuse; does not change the seam.
- **Heads-up for lane 2 (parity / DECISIONS.md #7):** timezone localization and
  struct-handling-mode are NOT applied by lane 4 (no client config available
  here). If the parity test fails on a timestamp/struct column, wrap lane 4's
  DataFrame with PySpark's `_create_converter_to_pandas` using the live session
  config, or hand lane 4 the timezone. This is the only known gap to byte-exact
  parity and is documented, not hidden.

## What works / blockers

- Works: single batch, multiple whole batches in row order, multi-chunk split
  batch reassembly (SPARK-53525), mixed chunked+whole, pre-4.1 protos, empty
  results, timestamps/decimals/nested types, encode->decode round trip, byte-exact
  match vs `LocalRelation` framing, integrity guards (out-of-order chunk,
  truncated batch, offset gap, row-count mismatch). 17/17 tests pass.
- No blockers. One documented parity boundary (timezone/struct config) handed to
  lane 2.
