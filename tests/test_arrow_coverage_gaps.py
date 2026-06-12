# SPDX-License-Identifier: Apache-2.0
"""Coverage-gap + parity tests for ``pyspark_connect_web.arrow.results``.

Complements ``tests/test_arrow_results.py`` (owned by lane 4) without rewriting
it. Targets the branches that suite leaves uncovered and pins the
type-parity behaviour flagged as the known boundary in
``team/findings-lane4-arrow.md`` / ``team/findings-integration.md``:

  * first-chunk ``chunk_index != 0`` rejection,
  * empty-result-with-known-schema (named empty DataFrame) branch,
  * the pyarrow-version probe fallback,
  * timezone / struct / decimal / nested type-fidelity *pins* against pyarrow's
    own ``to_pandas`` (the documented decoder contract), and an explicit
    ``xfail`` recording the one genuine parity gap: lane 4 applies NO
    ``spark.sql.session.timeZone`` localization (that needs a live client config),
    so a tz-aware timestamp does NOT match a session-tz-localized native result.

No grpcio, no browser, no server.
"""
from __future__ import annotations

import datetime
import decimal

import pandas as pd
import pyarrow as pa
import pytest

from pyspark_connect_web.arrow import decode_arrow_batches, reassemble_record_batches
from pyspark_connect_web.arrow import results as arrow_results

from test_arrow_results import FakeArrowBatch, FakeResponse, _ipc_stream_bytes


# --------------------------------------------------------------------------- #
# Branch gaps in _reassemble / decode_arrow_batches
# --------------------------------------------------------------------------- #
def test_first_chunk_nonzero_index_raises():
    """The very first chunk of a batch must have chunk_index 0; otherwise the
    stream is malformed (guards results.py line 107)."""
    table = pa.table({"i": pa.array([1, 2, 3], pa.int64())})
    batch = table.combine_chunks().to_batches()[0]
    ipc = _ipc_stream_bytes(batch)
    responses = [
        FakeResponse(
            FakeArrowBatch(ipc, batch.num_rows, chunk_index=1, num_chunks_in_batch=2)
        )
    ]
    with pytest.raises(ValueError, match="Expected chunk_index 0"):
        reassemble_record_batches(responses)


def test_empty_result_with_known_schema_returns_named_empty_frame():
    """An IPC stream with a schema but zero record batches must yield an empty
    DataFrame that still carries the column names (guards results.py line 209)."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass  # write the schema but no batches
    empty_ipc = sink.getvalue().to_pybytes()

    got = decode_arrow_batches([FakeResponse(FakeArrowBatch(empty_ipc, 0))])
    assert isinstance(got, pd.DataFrame)
    assert list(got.columns) == ["a", "b"]
    assert len(got) == 0


def test_pyarrow_version_probe_handles_garbage_version(monkeypatch):
    """The coerce-temporal probe must degrade to False on an unparsable
    ``pa.__version__`` (guards results.py lines 223-224)."""
    monkeypatch.setattr(pa, "__version__", "not.a.version")
    assert arrow_results._pyarrow_supports_coerce_temporal_nanoseconds() is False


def test_pyarrow_version_probe_true_on_modern():
    # Sanity: a real version (>= 13) probes True.
    assert arrow_results._pyarrow_supports_coerce_temporal_nanoseconds() is True


def test_decode_still_works_when_coerce_unsupported(monkeypatch):
    """When the probe says coerce is unsupported, decode still returns rows
    (exercises the to_pandas-without-kwarg branch)."""
    monkeypatch.setattr(
        arrow_results, "_pyarrow_supports_coerce_temporal_nanoseconds", lambda: False
    )
    table = pa.table({"i": pa.array([1, 2, 3], pa.int64())})
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches(
        [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    )
    assert list(got["i"]) == [1, 2, 3]


# --------------------------------------------------------------------------- #
# Type fidelity PINS (decoder contract = pyarrow's own to_pandas)
# --------------------------------------------------------------------------- #
def test_decimal128_and_decimal256_roundtrip_object_decimal():
    table = pa.table(
        {
            "d128": pa.array(
                [decimal.Decimal("1.23"), decimal.Decimal("-9.99")],
                pa.decimal128(10, 2),
            ),
            "d256": pa.array(
                [decimal.Decimal("123456789.123456789"), decimal.Decimal("0")],
                pa.decimal256(40, 9),
            ),
        }
    )
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches(
        [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    )
    pd.testing.assert_frame_equal(got.reset_index(drop=True), table.to_pandas())
    assert isinstance(got["d128"].iloc[0], decimal.Decimal)


def test_nested_struct_and_map_decode_as_python_objects():
    table = pa.table(
        {
            "strct": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": None}],
                pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
            "mp": pa.array(
                [[("k1", 1), ("k2", 2)], [("k3", 3)]],
                pa.map_(pa.string(), pa.int64()),
            ),
        }
    )
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches(
        [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    )
    # Decoder contract: faithful Arrow-native conversion == pyarrow's to_pandas.
    pd.testing.assert_frame_equal(
        got.reset_index(drop=True),
        table.to_pandas(coerce_temporal_nanoseconds=True),
    )
    # struct -> dict (matches Spark "legacy"/"dict" struct-handling mode default)
    assert got["strct"].iloc[0]["x"] == 1


def test_tz_naive_timestamp_coerces_to_datetime64ns():
    """tz-naive timestamps coerce to pandas datetime64[ns] (the native flag)."""
    table = pa.table(
        {
            "ts": pa.array(
                [datetime.datetime(2021, 1, 1, 12, 0), datetime.datetime(2022, 6, 15, 8, 30)],
                pa.timestamp("us"),
            )
        }
    )
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches(
        [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    )
    assert str(got["ts"].dtype) == "datetime64[ns]"
    pd.testing.assert_frame_equal(
        got.reset_index(drop=True), table.to_pandas(coerce_temporal_nanoseconds=True)
    )


@pytest.mark.xfail(
    reason=(
        "KNOWN PARITY GAP (findings-lane4-arrow.md / findings-integration.md): "
        "lane 4 is a pure decoder and applies NO spark.sql.session.timeZone "
        "localization - that needs a live client config it does not have. A "
        "tz-aware Arrow timestamp therefore decodes to its encoded (UTC) wall "
        "clock, NOT the session-tz-localized value the native client would "
        "produce. Localization, if required for exact parity, belongs in lane 2's "
        "integration. This xfail pins the gap so it is tracked, not hidden.",
    ),
    strict=True,
)
def test_session_timezone_localization_parity_gap():
    """Demonstrate the documented tz-localization gap.

    We encode a tz-aware (UTC) timestamp and assert the decoded value would have
    been localized to a non-UTC session timezone (e.g. America/Los_Angeles) the
    way PySpark's native ``_create_converter_to_pandas`` does. Lane 4 does not do
    this, so the wall-clock differs -> this assertion fails -> xfail(strict)."""
    utc_ts = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    table = pa.table({"ts": pa.array([utc_ts], pa.timestamp("us", tz="UTC"))})
    batch = table.combine_chunks().to_batches()[0]
    got = decode_arrow_batches(
        [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    )
    decoded = pd.Timestamp(got["ts"].iloc[0])
    # What a session tz of America/Los_Angeles (UTC-8) would yield as wall clock.
    expected_la_wall = pd.Timestamp("2020-12-31 16:00:00")
    # Lane 4 keeps UTC wall clock, so naive comparison to the LA wall clock fails.
    assert decoded.tz_localize(None) == expected_la_wall
