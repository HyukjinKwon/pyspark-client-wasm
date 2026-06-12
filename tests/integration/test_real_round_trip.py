# SPDX-License-Identifier: Apache-2.0
"""End-to-end round-trip of the Python vertical against a REAL Spark Connect server.

NO browser, NO Docker, NO network beyond loopback. The path under test is::

    PySpark Connect client  ->  pcw GrpcWebStub (grpc-web frames)  ->
        GrpcWebBridgeChannel (Envoy stand-in: decode frames, gRPC out,
        re-encode frames + trailer)  ->  real Spark Connect gRPC server

This exercises lane 1's framing in BOTH directions, lane 2's patch/install, and
lane 4's Arrow decode/encode, against a real engine. The bridge uses ``grpcio``
purely as the downstream client (allowed in tests; the DECISIONS.md #1 ban is
scoped to ``pyspark_connect_web/`` only).

Covers the DECISIONS.md "v0 done" matrix:
  * ``spark.range(10).collect()`` -> 10 rows
  * groupBy/agg/toPandas EXACT parity vs the native Connect client (DECISIONS.md #7)
  * ``createDataFrame(pandas_df)`` round-trips (lane 4 ``encode_local_relation``)
  * ``spark.sql("select 1 as x").collect()``
  * large multi-response streaming result (reattachable execute happy path)
  * mid-stream disconnect recovers via ReattachExecute (DECISIONS.md #6)
"""
from __future__ import annotations

import pandas as pd
import pytest

import pyspark_connect_web as pcw

# Relative import: this module is collected as part of the `integration` package
# (tests/integration/__init__.py), so `.bridge` resolves regardless of whether
# the repo root / `tests` is on sys.path (which it is not, in CI's import mode).
from .bridge import GrpcWebBridgeChannel

# Native (real grpcio) Connect client, used as the parity ground truth.
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
import pyspark.sql.connect.functions as F


# --------------------------------------------------------------------------- #
# Wiring fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def installed_pcw():
    pcw.install()
    yield
    # Leave installed; uninstall would disturb other modules in the same run.


@pytest.fixture()
def web_spark(connect_server, installed_pcw):
    """A SparkSession whose stub is pcw's grpc-web transport over the bridge."""
    host, port, token = connect_server
    target = f"{host}:{port}"
    auth = [("authorization", f"Bearer {token}")]

    bridges = []

    def factory(_endpoint):
        ch = GrpcWebBridgeChannel(target, extra_metadata=auth)
        bridges.append(ch)
        return ch

    pcw.set_channel_factory(factory)
    spark = ConnectSparkSession.builder.remote(
        f"sc://localhost:{port}/;transport=grpcweb"
    ).getOrCreate()
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        for ch in bridges:
            ch.close()
        pcw.set_channel_factory(None)


@pytest.fixture()
def native_spark(connect_server):
    """The stock native Connect client (real grpcio) — parity ground truth."""
    host, port, token = connect_server
    # Pass the token explicitly (the fixture no longer leaks it via env).
    spark = ConnectSparkSession.builder.remote(
        f"sc://localhost:{port}/;token={token}"
    ).getOrCreate()
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# v0 matrix
# --------------------------------------------------------------------------- #
def test_range_collect_returns_10_rows(web_spark):
    rows = web_spark.range(10).collect()
    assert len(rows) == 10
    assert [r["id"] for r in rows] == list(range(10))


def test_sql_select_literal(web_spark):
    rows = web_spark.sql("select 1 as x").collect()
    assert len(rows) == 1
    assert rows[0]["x"] == 1


def _grouped_query(spark):
    return (
        spark.range(100)
        .filter(F.col("id") % 2 == 0)
        .select((F.col("id") % 5).alias("k"), F.col("id"))
        .groupBy("k")
        .agg(F.count("*").alias("c"), F.sum("id").alias("s"))
        .orderBy("k")
    )


def test_groupby_agg_topandas_parity_vs_native(web_spark, native_spark):
    """DECISIONS.md #7: byte/row-exact parity vs a native Connect client on the
    same server."""
    web_pdf = _grouped_query(web_spark).toPandas()
    native_pdf = _grouped_query(native_spark).toPandas()
    pd.testing.assert_frame_equal(
        web_pdf.reset_index(drop=True), native_pdf.reset_index(drop=True)
    )
    # Sanity: the query actually produced grouped rows.
    assert list(web_pdf.columns) == ["k", "c", "s"]
    assert len(web_pdf) == 5


def test_create_dataframe_round_trips(web_spark):
    """Exercises lane 4 ``encode_local_relation`` via createDataFrame."""
    pdf = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
            "c": [1.5, 2.5, 3.5],
        }
    )
    df = web_spark.createDataFrame(pdf)
    got = df.toPandas()
    pd.testing.assert_frame_equal(got, pdf)


def test_create_dataframe_parity_vs_native(web_spark, native_spark):
    pdf = pd.DataFrame({"a": [10, 20, 30], "b": ["p", "q", "r"]})
    web_got = web_spark.createDataFrame(pdf).orderBy("a").toPandas()
    native_got = native_spark.createDataFrame(pdf).orderBy("a").toPandas()
    pd.testing.assert_frame_equal(
        web_got.reset_index(drop=True), native_got.reset_index(drop=True)
    )


def test_large_result_streams_multiple_responses(web_spark, native_spark):
    """A larger result streams as many ExecutePlanResponses (reattachable execute
    happy path), reassembled by lane 1's chunk buffer + lane 4's Arrow decode."""
    n = 200_000
    web_pdf = web_spark.range(n).select((F.col("id") * 2).alias("v")).toPandas()
    assert len(web_pdf) == n
    assert web_pdf["v"].iloc[:3].tolist() == [0, 2, 4]
    assert int(web_pdf["v"].iloc[-1]) == (n - 1) * 2
    # parity on the aggregate
    web_sum = int(web_spark.range(n).select(F.sum("id").alias("t")).collect()[0]["t"])
    native_sum = int(
        native_spark.range(n).select(F.sum("id").alias("t")).collect()[0]["t"]
    )
    assert web_sum == native_sum == n * (n - 1) // 2


def test_midstream_disconnect_recovers_via_reattach(connect_server, installed_pcw):
    """GUARD DECISIONS.md #6: a stream cut mid-result must recover via
    ReattachExecute and still return every row.

    Regression guard for the bug fixed in ``transport/grpcweb.py``: lane 1 used to
    *raise* ``SparkConnectGrpcException`` when a stream ended without a trailer.
    PySpark's reattachable iterator only recovers when the underlying iterator
    ends *cleanly* (StopIteration) before ResultComplete (its retry path only
    retries ``grpc.RpcError``), so the raise propagated to the user and reattach
    never fired. Verified here against a real server: we inject a cut, then assert
    full recovery AND that ReattachExecute was actually called.
    """
    host, port, token = connect_server
    target = f"{host}:{port}"
    auth = [("authorization", f"Bearer {token}")]

    seen = {"ExecutePlan": 0, "ReattachExecute": 0}

    class FaultBridge(GrpcWebBridgeChannel):
        cut_after_frames = 3
        _did_cut = False

        def server_stream(self, path, body, headers, timeout):
            method = path.rsplit("/", 1)[-1]
            seen[method] = seen.get(method, 0) + 1
            inner = super().server_stream(path, body, headers, timeout)
            if method == "ExecutePlan" and not FaultBridge._did_cut:
                emitted = 0
                for chunk in inner:
                    yield chunk
                    emitted += 1
                    if emitted >= self.cut_after_frames:
                        FaultBridge._did_cut = True
                        # Abandon the stream WITHOUT a trailer -> simulated drop.
                        return
            else:
                for chunk in inner:
                    yield chunk

    bridges = []

    def factory(_endpoint):
        ch = FaultBridge(target, extra_metadata=auth)
        bridges.append(ch)
        return ch

    pcw.set_channel_factory(factory)
    spark = ConnectSparkSession.builder.remote(
        f"sc://localhost:{port}/;transport=grpcweb"
    ).getOrCreate()
    try:
        n = 50_000
        rows = spark.range(n).select(F.col("id").alias("v")).toPandas()
        assert len(rows) == n
        assert rows["v"].tolist() == list(range(n))
        # The cut happened and recovery went through ReattachExecute.
        assert FaultBridge._did_cut is True
        assert seen["ReattachExecute"] >= 1
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        for ch in bridges:
            ch.close()
        pcw.set_channel_factory(None)
