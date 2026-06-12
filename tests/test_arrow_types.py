# SPDX-License-Identifier: Apache-2.0
"""Type-coverage tests for ``decode_arrow_batches``.

Builds pyarrow ``RecordBatch`` objects directly (no server, no grpcio, no
browser), serializes them to IPC-stream bytes, and asserts the decoder round-
trips to the expected pandas for every Spark-relevant Arrow type:

    int/long, double, boolean, string, decimal, date32,
    timestamp (tz-naive AND tz-aware), struct, list/array, map, binary,

each exercised with NULLs present.

Decoder contract (mirrors PySpark's native ``to_pandas`` path):
  * non-timestamp columns convert exactly like ``pyarrow.Table.to_pandas``
    with ``coerce_temporal_nanoseconds=True`` (the flag the native client uses
    for pyarrow >= 13);
  * tz-aware timestamp columns (Spark ``TimestampType``) are additionally
    localized to ``spark.sql.session.timeZone`` and made tz-naive, exactly as
    ``_check_series_convert_timestamps_local_tz`` does;
  * tz-naive timestamp columns (Spark ``TimestampNTZType``) are left as-is.
"""
from __future__ import annotations

import datetime
import decimal

import pandas as pd
import pyarrow as pa
import pytest

from pyspark_connect_web.arrow import decode_arrow_batches

from test_arrow_results import FakeArrowBatch, FakeResponse, _ipc_stream_bytes


SESSION_TZ = "America/Los_Angeles"  # UTC-8 in winter, a fixed non-UTC zone


def _decode(table: pa.Table, *, session_timezone: "str | None" = None) -> pd.DataFrame:
    """Encode a one-batch table to IPC bytes and decode it back to pandas."""
    batch = table.combine_chunks().to_batches()[0]
    responses = [FakeResponse(FakeArrowBatch(_ipc_stream_bytes(batch), batch.num_rows))]
    return decode_arrow_batches(responses, session_timezone=session_timezone)


def _assert_native_roundtrip(table: pa.Table) -> pd.DataFrame:
    """Decode == pyarrow's own to_pandas (the non-timestamp decoder contract)."""
    got = _decode(table)
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
    return got


# --------------------------------------------------------------------------- #
# Primitives (with nulls)
# --------------------------------------------------------------------------- #
def test_int_and_long_with_nulls():
    table = pa.table(
        {
            "i32": pa.array([1, None, -3], pa.int32()),
            "i64": pa.array([10, 20, None], pa.int64()),
        }
    )
    got = _assert_native_roundtrip(table)
    # A nullable integral column with a null is float64 (SPARK-21766 parity).
    assert str(got["i32"].dtype) == "float64"
    assert str(got["i64"].dtype) == "float64"


def test_int_and_long_without_nulls_stay_integer():
    table = pa.table(
        {
            "i32": pa.array([1, 2, 3], pa.int32()),
            "i64": pa.array([10, 20, 30], pa.int64()),
        }
    )
    got = _assert_native_roundtrip(table)
    assert str(got["i32"].dtype) == "int32"
    assert str(got["i64"].dtype) == "int64"


def test_double_with_nulls():
    table = pa.table({"d": pa.array([1.5, None, -3.25], pa.float64())})
    got = _assert_native_roundtrip(table)
    assert str(got["d"].dtype) == "float64"
    assert got["d"].iloc[1] != got["d"].iloc[1]  # NaN


def test_boolean_with_nulls():
    table = pa.table({"b": pa.array([True, None, False], pa.bool_())})
    got = _assert_native_roundtrip(table)
    assert got["b"].iloc[0] is True or got["b"].iloc[0] == True  # noqa: E712
    assert got["b"].iloc[1] is None


def test_string_with_nulls():
    table = pa.table({"s": pa.array(["alpha", None, "gamma"], pa.string())})
    got = _assert_native_roundtrip(table)
    assert got["s"].tolist() == ["alpha", None, "gamma"]


def test_binary_with_nulls():
    table = pa.table({"bin": pa.array([b"\x00\x01\x02", None, b""], pa.binary())})
    got = _assert_native_roundtrip(table)
    assert got["bin"].tolist() == [b"\x00\x01\x02", None, b""]


# --------------------------------------------------------------------------- #
# Decimal (with nulls) - decimal128 and decimal256
# --------------------------------------------------------------------------- #
def test_decimal128_with_nulls():
    table = pa.table(
        {
            "d": pa.array(
                [decimal.Decimal("1.23"), None, decimal.Decimal("-9.99")],
                pa.decimal128(10, 2),
            )
        }
    )
    got = _assert_native_roundtrip(table)
    assert isinstance(got["d"].iloc[0], decimal.Decimal)
    assert got["d"].iloc[1] is None


def test_decimal256_with_nulls():
    table = pa.table(
        {
            "d": pa.array(
                [decimal.Decimal("123456789.123456789"), None, decimal.Decimal("0")],
                pa.decimal256(40, 9),
            )
        }
    )
    got = _assert_native_roundtrip(table)
    assert isinstance(got["d"].iloc[0], decimal.Decimal)
    assert got["d"].iloc[1] is None


# --------------------------------------------------------------------------- #
# Date32 (with nulls)
# --------------------------------------------------------------------------- #
def test_date32_with_nulls():
    table = pa.table(
        {"d": pa.array([datetime.date(2021, 1, 1), None, datetime.date(1999, 12, 31)], pa.date32())}
    )
    got = _assert_native_roundtrip(table)
    assert got["d"].iloc[0] == datetime.date(2021, 1, 1)
    assert got["d"].iloc[1] is None


# --------------------------------------------------------------------------- #
# Timestamp: tz-naive (NTZ) - NOT localized
# --------------------------------------------------------------------------- #
def test_timestamp_tz_naive_with_nulls_not_localized():
    table = pa.table(
        {
            "ts": pa.array(
                [datetime.datetime(2021, 1, 1, 12, 0), None, datetime.datetime(2022, 6, 15, 8, 30)],
                pa.timestamp("us"),
            )
        }
    )
    # Even with a session timezone set, NTZ must pass through unchanged.
    got = _decode(table, session_timezone=SESSION_TZ)
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
    assert str(got["ts"].dtype) == "datetime64[ns]"
    assert pd.Timestamp(got["ts"].iloc[0]) == pd.Timestamp("2021-01-01 12:00:00")
    assert got["ts"].isna().iloc[1]


# --------------------------------------------------------------------------- #
# Timestamp: tz-aware (TimestampType) - localized to session tz, made naive
# --------------------------------------------------------------------------- #
def test_timestamp_tz_aware_with_nulls_localized_to_session_tz():
    from pyspark.sql.pandas.types import _check_series_convert_timestamps_local_tz

    values = [
        datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        None,
        datetime.datetime(2022, 7, 4, 23, 30, tzinfo=datetime.timezone.utc),
    ]
    table = pa.table({"ts": pa.array(values, pa.timestamp("us", tz="UTC"))})
    got = _decode(table, session_timezone=SESSION_TZ)

    # Result is tz-naive datetime64[ns] with the wall clock in the session zone.
    assert str(got["ts"].dtype) == "datetime64[ns]"
    assert pd.Timestamp(got["ts"].iloc[0]) == pd.Timestamp("2020-12-31 16:00:00")
    assert got["ts"].isna().iloc[1]

    # Byte-identical to PySpark's own converter on the same Arrow-derived series.
    ref = table.to_pandas(coerce_temporal_nanoseconds=True)["ts"]
    expected = _check_series_convert_timestamps_local_tz(ref, timezone=SESSION_TZ)
    pd.testing.assert_series_equal(
        got["ts"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


def test_timestamp_tz_aware_nonutc_source_label_localizes_same_instant():
    """A tz-aware column labelled with a non-UTC zone is the same absolute instant,
    so localization to the session zone yields the same wall clock as a UTC label."""
    instant = datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    table = pa.table({"ts": pa.array([instant], pa.timestamp("us", tz="America/New_York"))})
    got = _decode(table, session_timezone=SESSION_TZ)
    assert pd.Timestamp(got["ts"].iloc[0]) == pd.Timestamp("2020-12-31 16:00:00")


# --------------------------------------------------------------------------- #
# Struct (with nulls, incl. null field value and null whole struct)
# --------------------------------------------------------------------------- #
def test_struct_with_nulls():
    struct_t = pa.struct([("x", pa.int64()), ("y", pa.string())])
    table = pa.table(
        {
            "strct": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": None}, None],
                struct_t,
            )
        }
    )
    got = _decode(table)
    # Decoder contract for nested data == pyarrow to_pandas (struct -> dict, the
    # Spark "legacy"/"dict" struct-handling mode default).
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
    assert got["strct"].iloc[0]["x"] == 1
    assert got["strct"].iloc[1]["y"] is None
    assert got["strct"].iloc[2] is None


# --------------------------------------------------------------------------- #
# List / array (with nulls, incl. null element and null whole list)
# --------------------------------------------------------------------------- #
def test_list_with_nulls():
    table = pa.table(
        {"lst": pa.array([[1, 2], [3, None, 5], None], pa.list_(pa.int64()))}
    )
    got = _decode(table)
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
    assert list(got["lst"].iloc[0]) == [1, 2]
    assert got["lst"].iloc[2] is None


# --------------------------------------------------------------------------- #
# Map (with nulls, incl. null value and null whole map)
# --------------------------------------------------------------------------- #
def test_map_with_nulls():
    table = pa.table(
        {
            "mp": pa.array(
                [[("k1", 1), ("k2", None)], [("k3", 3)], None],
                pa.map_(pa.string(), pa.int64()),
            )
        }
    )
    got = _decode(table)
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
    # pyarrow renders a map as a list of (key, value) tuples.
    assert dict(got["mp"].iloc[0])["k1"] == 1
    assert got["mp"].iloc[2] is None


# --------------------------------------------------------------------------- #
# All-types-at-once smoke test (every type in one batch, with nulls)
# --------------------------------------------------------------------------- #
def test_all_types_one_batch_roundtrip():
    table = pa.table(
        {
            "i64": pa.array([1, 2], pa.int64()),
            "d": pa.array([1.5, 2.5], pa.float64()),
            "b": pa.array([True, False], pa.bool_()),
            "s": pa.array(["a", None], pa.string()),
            "dec": pa.array([decimal.Decimal("1.23"), None], pa.decimal128(10, 2)),
            "dt": pa.array([datetime.date(2021, 1, 1), None], pa.date32()),
            "ntz": pa.array([datetime.datetime(2021, 1, 1, 12, 0), None], pa.timestamp("us")),
            "bin": pa.array([b"\x00", None], pa.binary()),
            "lst": pa.array([[1, 2], None], pa.list_(pa.int64())),
            "strct": pa.array(
                [{"x": 1}, None], pa.struct([("x", pa.int64())])
            ),
        }
    )
    got = _decode(table, session_timezone=SESSION_TZ)
    expected = table.to_pandas(coerce_temporal_nanoseconds=True)
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected)
