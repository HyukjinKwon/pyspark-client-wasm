# SPDX-License-Identifier: Apache-2.0
"""Arrow result decoding (response side) and Arrow IPC encoding (request side).

Result side - ``decode_arrow_batches``:
    Spark Connect returns query results as a stream of ``ExecutePlanResponse``
    protos. The data-bearing ones set the ``arrow_batch`` field, whose ``data``
    holds serialized Arrow IPC-stream bytes. We consume the response iterable,
    reassemble any batch that SPARK-53525 split across several responses, decode
    each reassembled IPC stream to ``pyarrow.RecordBatch`` objects in row order,
    and build one pandas ``DataFrame``. Correctness (row/type-exactness) is the
    priority - .

Request side - ``encode_local_relation``:
    ``createDataFrame(pdf)`` ships the local data to the server as a
    ``LocalRelation`` whose ``data`` is an Arrow IPC stream. This mirrors exactly
    what ``pyspark.sql.connect.plan.LocalRelation.plan`` writes, so the produced
    bytes drop straight into ``plan.local_relation.data``.

SPARK-53525 (Arrow result chunking, Spark 4.1+):
    When a single Arrow batch's serialized bytes are too large for one gRPC
    message, the server may split them across several ``ExecutePlanResponse``
    messages. The ``ArrowBatch`` message then carries two extra optional fields:

      * ``chunk_index``         - 0-based position of this chunk within the batch
      * ``num_chunks_in_batch`` - total chunks the batch was split into

    A chunk's ``data`` is NOT independently valid Arrow; only the byte
    concatenation of all chunks (in ``chunk_index`` order) forms a decodable IPC
    stream. ``row_count`` is the row count of the fully reassembled batch and
    ``start_offset`` is the batch's starting row offset in the overall result -
    both are integrity checks. These fields are ``proto3 optional`` and DO NOT
    exist in pyspark 4.0.0's proto (they arrived in 4.1). We therefore probe them
    with ``HasField`` and degrade gracefully: when ``num_chunks_in_batch`` is
    absent or 0, every ``arrow_batch`` is a complete self-contained IPC stream
    (the classic, pre-chunking behaviour).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, List

import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd


__all__ = [
    "decode_arrow_batches",
    "encode_local_relation",
    "reassemble_record_batches",
]


def _has_field(msg: Any, name: str) -> bool:
    """``msg.HasField(name)`` that is safe when the field doesn't exist.

    pyspark 4.0.0's ``ArrowBatch`` has no ``chunk_index`` /
    ``num_chunks_in_batch`` (they are SPARK-53525, Spark 4.1+). protobuf raises
    ``ValueError`` for an unknown field name, so we treat unknown as "not set".
    This lets the same code decode results from both the pinned 4.0.x range and
    future chunk-capable servers without branching on version.
    """
    try:
        return bool(msg.HasField(name))
    except (ValueError, AttributeError):
        return False


def _chunk_index(arrow_batch: Any) -> int:
    return int(getattr(arrow_batch, "chunk_index", 0) or 0)


def _num_chunks_in_batch(arrow_batch: Any) -> int:
    if not _has_field(arrow_batch, "num_chunks_in_batch"):
        return 0
    return int(getattr(arrow_batch, "num_chunks_in_batch", 0) or 0)


def _reassemble(responses: Iterable[Any]) -> tuple[List["pa.RecordBatch"], "pa.Schema | None"]:
    """Core reassembly; returns ordered batches plus the IPC schema if seen.

    The schema is recovered from the IPC reader even when a batch carries zero
    rows (an empty result yields no ``RecordBatch`` objects but still has a
    schema), so callers can build a correctly-named empty DataFrame.
    """
    batches: List[pa.RecordBatch] = []
    schema: "pa.Schema | None" = None
    pending: List[bytes] = []  # buffered chunk `data`, in chunk_index order
    num_records = 0  # running row offset across all *completed* batches

    for resp in responses:
        if not _has_field(resp, "arrow_batch"):
            continue
        ab = resp.arrow_batch
        chunk_index = _chunk_index(ab)

        if pending:
            # Mid-batch: the next chunk must continue the sequence in order.
            if chunk_index != len(pending):
                raise ValueError(
                    f"Out-of-order arrow chunk: expected chunk_index "
                    f"{len(pending)} but got {chunk_index}."
                )
        else:
            # First chunk of a (possibly single-chunk) batch.
            if chunk_index != 0:
                raise ValueError(
                    f"Expected chunk_index 0 at the start of an arrow batch "
                    f"but got {chunk_index}."
                )
            if _has_field(ab, "start_offset") and ab.start_offset != num_records:
                raise ValueError(
                    f"Arrow batch row-offset gap: expected batch to start at "
                    f"row {num_records} but server reported start_offset "
                    f"{ab.start_offset}."
                )

        pending.append(ab.data)

        num_chunks = _num_chunks_in_batch(ab)
        is_complete = num_chunks == 0 or len(pending) == num_chunks
        if not is_complete:
            continue  # wait for the remaining chunks of this batch

        # We hold every chunk of this batch; join and decode the IPC stream.
        ipc_bytes = pending[0] if len(pending) == 1 else b"".join(pending)
        pending = []

        rows_in_batch = 0
        with pa.ipc.open_stream(ipc_bytes) as reader:
            if schema is None:
                schema = reader.schema
            for rb in reader:
                assert isinstance(rb, pa.RecordBatch)
                rows_in_batch += rb.num_rows
                batches.append(rb)

        # Integrity check against the server's declared row count for the batch.
        declared = int(getattr(ab, "row_count", rows_in_batch) or rows_in_batch)
        if declared != rows_in_batch:
            raise ValueError(
                f"Arrow batch row-count mismatch: server declared {declared} "
                f"rows but the IPC stream decoded to {rows_in_batch}."
            )
        num_records += rows_in_batch

    if pending:
        raise ValueError(
            "Truncated arrow result: response stream ended in the middle of a "
            f"chunked batch ({len(pending)} chunk(s) buffered, batch never "
            "completed)."
        )

    return batches, schema


def reassemble_record_batches(responses: Iterable[Any]) -> List["pa.RecordBatch"]:
    """Reassemble + decode the ``arrow_batch`` stream into ordered record batches.

    Walks the ``ExecutePlanResponse`` iterable once, joins SPARK-53525 chunk
    fragments back into whole IPC streams, decodes each to ``pa.RecordBatch``
    objects, and returns them in arrival (row) order. Non-data responses (schema,
    metrics, SQL-command results, progress, ...) are ignored here - this function
    is purely the bytes->batches step.

    Raises ``ValueError`` on a malformed stream: out-of-order chunk indices, a
    gap between a batch's ``start_offset`` and the running row count, an
    unterminated chunked batch at end-of-stream, or a decoded row count that
    disagrees with the server-declared ``row_count``. These mirror the integrity
    checks PySpark's own client performs and keep us byte/row-exact.
    """
    return _reassemble(responses)[0]


def decode_arrow_batches(responses: Iterable[Any]) -> "pd.DataFrame":
    """Decode a stream of ``ExecutePlanResponse`` protos to a pandas DataFrame.

    Consumes ``responses`` (an iterable of objects with the
    ``ExecutePlanResponse`` shape - either real pyspark protos or anything
    exposing ``HasField('arrow_batch')`` and an ``arrow_batch`` with
    ``data`` / ``row_count`` / optional chunk fields), reassembles SPARK-53525
    chunked batches, decodes them via pyarrow IPC, and converts the resulting
    Arrow table to pandas preserving schema and types.

    Type fidelity: we convert with ``coerce_temporal_nanoseconds=True`` (the same
    flag PySpark's ``to_pandas`` passes for pyarrow >= 13), so date/timestamp/
    duration units land on pandas' nanosecond types exactly as the native client
    produces them. See ``the project notes`` for the type-mapping
    boundary (struct handling mode, session timezone) that requires a live client
    config and so lives in the integration, not here.
    """
    import pandas as pd  # local import: pandas is a Pyodide-provided dep

    batches, schema = _reassemble(responses)

    if not batches:
        # No data rows. If we saw an IPC schema (e.g. an empty result set with a
        # known schema), preserve the column names; otherwise (a command-only
        # response with no arrow_batch at all) there is nothing to name.
        if schema is None:
            return pd.DataFrame()
        return pd.DataFrame(columns=schema.names, index=range(0))

    table = pa.Table.from_batches(batches)

    # SPARK-51112: pyarrow's to_pandas can segfault on a genuinely empty table on
    # some builds; build the empty frame by hand with the right column names.
    if table.num_rows == 0:
        return pd.DataFrame(columns=table.schema.names, index=range(0))

    to_pandas_kwargs: dict[str, Any] = {}
    # `coerce_temporal_nanoseconds` exists in pyarrow >= 13; Pyodide ships >= 22,
    # but guard anyway so local dev on an older wheel still works.
    if _pyarrow_supports_coerce_temporal_nanoseconds():
        to_pandas_kwargs["coerce_temporal_nanoseconds"] = True

    return table.to_pandas(**to_pandas_kwargs)


def _pyarrow_supports_coerce_temporal_nanoseconds() -> bool:
    try:
        major = int(pa.__version__.split(".", 1)[0])
    except (ValueError, AttributeError):
        return False
    return major >= 13


def encode_local_relation(pdf: "pd.DataFrame") -> bytes:
    """Encode a pandas DataFrame as Arrow IPC-stream bytes for the request side.

    Returns the bytes that ``createDataFrame`` puts into
    ``plan.local_relation.data``. This mirrors
    ``pyspark.sql.connect.plan.LocalRelation.plan``: build a ``pa.Table`` from the
    DataFrame, then write it to an Arrow IPC *stream* (not file) with the table's
    own schema. ``decode_arrow_batches`` reverses exactly this byte layout, so an
    ``encode_local_relation`` -> ``decode_arrow_batches`` round trip reproduces the
    input frame.

    Note: this is the faithful Arrow representation of the pandas frame. PySpark's
    ``createDataFrame`` additionally applies Spark-side type coercion (e.g.
    tz-naive datetimes -> ``TimestampType``) using session config; that coercion
    is a the concern that operates on the resulting ``pa.Table``/schema. Lane 4
    only guarantees the IPC framing is correct and reversible.
    """
    table = pa.Table.from_pandas(pdf, preserve_index=False)

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()
