# SPDX-License-Identifier: Apache-2.0
"""Tests for the Arrow result decoding + request-side Arrow encoding.

No grpcio, no browser, no server. We build Arrow record batches with pyarrow,
serialize them to IPC-stream bytes, wrap those bytes in fake
``ExecutePlanResponse``-shaped objects (and, when pyspark is importable, in real
protos too), and assert ``decode_arrow_batches`` reconstructs the exact pandas
DataFrame - including the SPARK-53525 multi-chunk / split-batch case.
"""
from __future__ import annotations

import datetime
import decimal
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pytest

from pyspark_connect_web.arrow import (
    decode_arrow_batches,
    encode_local_relation,
    reassemble_record_batches,
)


# --------------------------------------------------------------------------- #
# Fakes: ExecutePlanResponse-shaped objects with proto-like HasField semantics
# --------------------------------------------------------------------------- #
class FakeArrowBatch:
    """Mimics ``ExecutePlanResponse.ArrowBatch``.

    ``chunk_index`` / ``num_chunks_in_batch`` / ``start_offset`` are SPARK-53525
    (Spark 4.1+) ``proto3 optional`` fields. We model presence the way protobuf
    does: ``HasField`` reflects whether the field was explicitly set, and raises
    ``ValueError`` for names this build doesn't know about (``_known_fields``).
    """

    def __init__(
        self,
        data: bytes,
        row_count: int,
        *,
        start_offset: Optional[int] = None,
        chunk_index: Optional[int] = None,
        num_chunks_in_batch: Optional[int] = None,
        supports_chunking: bool = True,
    ) -> None:
        self.data = data
        self.row_count = row_count
        self.start_offset = start_offset if start_offset is not None else 0
        self.chunk_index = chunk_index if chunk_index is not None else 0
        self.num_chunks_in_batch = (
            num_chunks_in_batch if num_chunks_in_batch is not None else 0
        )
        self._set = {"data", "row_count"}
        if start_offset is not None:
            self._set.add("start_offset")
        if chunk_index is not None:
            self._set.add("chunk_index")
        if num_chunks_in_batch is not None:
            self._set.add("num_chunks_in_batch")
        # A pre-4.1 server/proto simply does not have the chunk fields.
        self._known_fields = {"data", "row_count", "start_offset"}
        if supports_chunking:
            self._known_fields |= {"chunk_index", "num_chunks_in_batch"}

    def HasField(self, name: str) -> bool:  # noqa: N802 (proto API name)
        if name not in self._known_fields:
            raise ValueError(f"unknown field {name!r}")
        return name in self._set


class FakeResponse:
    """Mimics ``ExecutePlanResponse`` carrying (or not carrying) an arrow_batch."""

    def __init__(self, arrow_batch: Optional[FakeArrowBatch]) -> None:
        self.arrow_batch = arrow_batch

    def HasField(self, name: str) -> bool:  # noqa: N802
        if name == "arrow_batch":
            return self.arrow_batch is not None
        # Other oneof members (schema, metrics, sql_command_result, ...) - all
        # absent in these fixtures.
        return False


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _ipc_stream_bytes(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _sample_table() -> pa.Table:
    return pa.table(
        {
            "i": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "f": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
            "s": pa.array(["a", "b", None, "d", "e"], pa.string()),
            "b": pa.array([True, False, True, None, False], pa.bool_()),
        }
    )


# --------------------------------------------------------------------------- #
# decode_arrow_batches: basics
# --------------------------------------------------------------------------- #
def test_decode_single_batch_roundtrip():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    responses = [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]

    got = decode_arrow_batches(responses)
    expected = table.to_pandas()
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)


def test_decode_multiple_whole_batches_preserves_row_order():
    # Two independent, self-contained IPC batches -> rows must concatenate in order.
    full = _sample_table()
    b1 = full.slice(0, 2).combine_chunks().to_batches()[0]
    b2 = full.slice(2, 3).combine_chunks().to_batches()[0]
    responses = [
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(b1), b1.num_rows, start_offset=0)),
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(b2), b2.num_rows, start_offset=2)),
    ]

    got = decode_arrow_batches(responses)
    expected = full.to_pandas()
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)


def test_decode_ignores_non_arrow_responses():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    responses = [
        FakeResponse(None),  # e.g. a schema / progress / metrics response
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows)),
        FakeResponse(None),  # e.g. result_complete
    ]

    got = decode_arrow_batches(responses)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), table.to_pandas())


def test_decode_empty_stream_returns_empty_frame():
    got = decode_arrow_batches([FakeResponse(None)])
    assert isinstance(got, pd.DataFrame)
    assert got.empty


# --------------------------------------------------------------------------- #
# decode_arrow_batches: SPARK-53525 chunked / split-batch reassembly (GUARD)
# --------------------------------------------------------------------------- #
def _split_bytes(data: bytes, n: int) -> List[bytes]:
    """Split `data` into `n` roughly-equal, contiguous, NON-empty fragments."""
    assert n >= 1
    size = len(data)
    step = max(1, (size + n - 1) // n)
    parts = [data[i : i + step] for i in range(0, size, step)]
    # Guarantee exactly n parts (pad with empty tail slices only if needed).
    while len(parts) < n:
        parts.append(b"")
    return parts[:n]


def test_decode_chunked_batch_reassembles_split_ipc_stream():
    """A single Arrow batch's IPC bytes split across N responses must reassemble.

    This is the core SPARK-53525 guard: each individual chunk's `data` is NOT a
    valid IPC stream on its own; only the in-order byte concatenation is.
    """
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    ipc = _ipc_stream_bytes(batch)

    n_chunks = 3
    fragments = _split_bytes(ipc, n_chunks)
    assert sum(len(f) for f in fragments) == len(ipc)

    # Sanity: a lone middle fragment is not independently decodable.
    with pytest.raises(Exception):
        with pa.ipc.open_stream(fragments[1]) as r:
            list(r)

    responses = [
        FakeResponse(
            FakeArrowBatch(
                frag,
                # row_count is the count of the *whole* reassembled batch.
                row_count=batch.num_rows,
                start_offset=0 if idx == 0 else None,
                chunk_index=idx,
                num_chunks_in_batch=n_chunks,
            )
        )
        for idx, frag in enumerate(fragments)
    ]

    got = decode_arrow_batches(responses)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), table.to_pandas())


def test_decode_mixed_chunked_then_whole_batches():
    """A chunked batch followed by a whole batch, with correct row offsets."""
    full = _sample_table()
    b1 = full.slice(0, 3).combine_chunks().to_batches()[0]
    b2 = full.slice(3, 2).combine_chunks().to_batches()[0]

    ipc1 = _ipc_stream_bytes(b1)
    frags = _split_bytes(ipc1, 2)
    responses = [
        FakeResponse(
            FakeArrowBatch(frags[0], b1.num_rows, start_offset=0, chunk_index=0, num_chunks_in_batch=2)
        ),
        FakeResponse(FakeArrowBatch(frags[1], b1.num_rows, chunk_index=1, num_chunks_in_batch=2)),
        # second batch is whole, starts where the first one ended (offset 3)
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(b2), b2.num_rows, start_offset=3)),
    ]

    got = decode_arrow_batches(responses)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), full.to_pandas())


def test_pre_chunking_proto_without_chunk_fields_decodes():
    """A pre-4.1 proto lacking chunk_index/num_chunks_in_batch decodes as whole."""
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    ab = FakeArrowBatch(
        _ipc_stream_bytes(batch),
        batch.num_rows,
        start_offset=0,
        supports_chunking=False,  # HasField('num_chunks_in_batch') raises ValueError
    )
    got = decode_arrow_batches([FakeResponse(ab)])
    pd.testing.assert_frame_equal(got.reset_index(drop=True), table.to_pandas())


# --------------------------------------------------------------------------- #
# decode_arrow_batches: integrity guards
# --------------------------------------------------------------------------- #
def test_out_of_order_chunk_raises():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    frags = _split_bytes(_ipc_stream_bytes(batch), 2)
    responses = [
        FakeResponse(FakeArrowBatch(frags[0], batch.num_rows, chunk_index=0, num_chunks_in_batch=2)),
        # wrong index: should be 1
        FakeResponse(FakeArrowBatch(frags[1], batch.num_rows, chunk_index=0, num_chunks_in_batch=2)),
    ]
    with pytest.raises(ValueError, match="Out-of-order"):
        reassemble_record_batches(responses)


def test_truncated_chunked_batch_raises():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    frags = _split_bytes(_ipc_stream_bytes(batch), 3)
    responses = [
        FakeResponse(FakeArrowBatch(frags[0], batch.num_rows, chunk_index=0, num_chunks_in_batch=3)),
        FakeResponse(FakeArrowBatch(frags[1], batch.num_rows, chunk_index=1, num_chunks_in_batch=3)),
        # missing chunk 2
    ]
    with pytest.raises(ValueError, match="Truncated"):
        reassemble_record_batches(responses)


def test_start_offset_gap_raises():
    full = _sample_table()
    b1 = full.slice(0, 2).combine_chunks().to_batches()[0]
    b2 = full.slice(2, 3).combine_chunks().to_batches()[0]
    responses = [
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(b1), b1.num_rows, start_offset=0)),
        # claims to start at offset 99 but we only have 2 rows so far
        FakeResponse(FakeArrowBatch(_ipc_stream_bytes(b2), b2.num_rows, start_offset=99)),
    ]
    with pytest.raises(ValueError, match="row-offset gap"):
        reassemble_record_batches(responses)


def test_row_count_mismatch_raises():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    # declared row_count (99) disagrees with the 5 rows actually in the IPC stream
    ab = FakeArrowBatch(_ipc_stream_bytes(batch), row_count=99)
    with pytest.raises(ValueError, match="row-count mismatch"):
        reassemble_record_batches([FakeResponse(ab)])


# --------------------------------------------------------------------------- #
# Type fidelity: timestamps, decimals, nested
# --------------------------------------------------------------------------- #
def test_type_fidelity_timestamp_decimal_nested():
    table = pa.table(
        {
            "ts": pa.array(
                [
                    datetime.datetime(2021, 1, 1, 12, 0, 0),
                    datetime.datetime(2022, 6, 15, 8, 30, 0),
                ],
                pa.timestamp("us"),
            ),
            "dec": pa.array(
                [decimal.Decimal("1.23"), decimal.Decimal("4.56")],
                pa.decimal128(10, 2),
            ),
            "lst": pa.array([[1, 2], [3, 4, 5]], pa.list_(pa.int64())),
            "strct": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
        }
    )
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches([FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))])

    # Exact column-by-column match against pyarrow's own to_pandas (the reference).
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)


# --------------------------------------------------------------------------- #
# encode_local_relation: round trip
# --------------------------------------------------------------------------- #
def test_encode_local_relation_roundtrips_through_decode():
    pdf = pd.DataFrame(
        {
            "i": [10, 20, 30],
            "f": [0.1, 0.2, 0.3],
            "s": ["x", "y", "z"],
            "b": [True, False, True],
        }
    )
    ipc = encode_local_relation(pdf)
    assert isinstance(ipc, bytes) and len(ipc) > 0

    # The encoded bytes are exactly one self-contained IPC stream - feed them back
    # through the result decoder as a single whole batch.
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    responses = [FakeResponse(FakeArrowBatch(ipc, table.num_rows))]
    got = decode_arrow_batches(responses)

    pd.testing.assert_frame_equal(got.reset_index(drop=True), pdf.reset_index(drop=True))


def test_encode_local_relation_matches_pyspark_localrelation_framing():
    """Bytes must match what pyspark.sql.connect.plan.LocalRelation writes."""
    pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["p", "q", "r"]})
    ours = encode_local_relation(pdf)

    table = pa.Table.from_pandas(pdf, preserve_index=False)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        for b in table.to_batches():
            writer.write_batch(b)
    reference = sink.getvalue().to_pybytes()

    assert ours == reference


def test_encode_empty_dataframe_roundtrips():
    pdf = pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="object")})
    ipc = encode_local_relation(pdf)
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    got = decode_arrow_batches([FakeResponse(FakeArrowBatch(ipc, table.num_rows))])
    assert list(got.columns) == ["a", "b"]
    assert len(got) == 0


# --------------------------------------------------------------------------- #
# Real pyspark protos when available (optional - skipped if pyspark missing)
# --------------------------------------------------------------------------- #
pyspark_proto = pytest.importorskip(
    "pyspark.sql.connect.proto.base_pb2",
    reason="pyspark not installed; fake-proto coverage already exercises the path",
)


def test_real_proto_single_batch():
    table = _sample_table()
    batch = table.combine_chunks().to_batches()[0]
    resp = pyspark_proto.ExecutePlanResponse()
    resp.arrow_batch.data = _ipc_stream_bytes(batch)
    resp.arrow_batch.row_count = batch.num_rows
    resp.arrow_batch.start_offset = 0

    got = decode_arrow_batches([resp])
    pd.testing.assert_frame_equal(got.reset_index(drop=True), table.to_pandas())


def test_real_proto_two_batches_in_order():
    full = _sample_table()
    b1 = full.slice(0, 2).combine_chunks().to_batches()[0]
    b2 = full.slice(2, 3).combine_chunks().to_batches()[0]

    r1 = pyspark_proto.ExecutePlanResponse()
    r1.arrow_batch.data = _ipc_stream_bytes(b1)
    r1.arrow_batch.row_count = b1.num_rows
    r1.arrow_batch.start_offset = 0

    r2 = pyspark_proto.ExecutePlanResponse()
    r2.arrow_batch.data = _ipc_stream_bytes(b2)
    r2.arrow_batch.row_count = b2.num_rows
    r2.arrow_batch.start_offset = 2

    got = decode_arrow_batches([r1, r2])
    pd.testing.assert_frame_equal(got.reset_index(drop=True), full.to_pandas())
