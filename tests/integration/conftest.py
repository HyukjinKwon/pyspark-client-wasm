# SPDX-License-Identifier: Apache-2.0
"""Fixtures: a real local Spark Connect gRPC server on a free, non-15002 port.

Starts a *regular* JVM SparkSession with the Spark Connect plugin loaded, which
brings up the Connect gRPC service in-process (no Docker, no network, bundled
jars). We bind it to an ephemeral free port (never 15002, which a leftover JVM
holds) via ``spark.connect.grpc.binding.port`` and authenticate with a known
token so both the native reference client and our grpc-web bridge can reach it.
"""
from __future__ import annotations

import os
import socket
import uuid

import pytest


def _free_port() -> int:
    """Pick a currently-free TCP port (never 15002)."""
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        if port != 15002:
            return port


@pytest.fixture(scope="session")
def connect_server():
    """Start an in-process Spark Connect server; yield (host, port, token).

    Session-scoped: the JVM is expensive to start, so we reuse it across tests.
    """
    # The bridge needs grpcio (test-only downstream client). If it's missing,
    # skip rather than error — the unit suite already covers the framing logic.
    try:
        import grpc  # noqa: F401
    except Exception:  # pragma: no cover
        pytest.skip("grpcio not available; skipping real-server integration tests")

    # The server needs a JVM. If none is on PATH, skip cleanly so CI without Java
    # is green rather than red.
    import shutil

    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("no JVM (java) available; skipping real-server integration tests")

    port = _free_port()
    token = str(uuid.uuid4())

    # The Connect server reads the expected auth token from this env var at
    # startup. We restore the prior value after the JVM is up so this fixture
    # does NOT leak a token into ``os.environ`` for the rest of the pytest
    # session — a leaked token flips ``DefaultChannelBuilder.secure`` to True and
    # breaks unrelated unit tests (test isolation). Both clients are handed the
    # token explicitly instead (web via bridge metadata, native via ``token=``).
    _prev_token = os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
    os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
    # Make sure we don't accidentally inherit a remote that would route the
    # *regular* session through Connect.
    os.environ.pop("SPARK_REMOTE", None)
    os.environ.pop("SPARK_LOCAL_REMOTE", None)

    try:
        from pyspark.sql import SparkSession as JVMSparkSession
    except Exception as e:  # pragma: no cover
        pytest.skip(f"pyspark not importable: {e}")

    try:
        spark = (
            JVMSparkSession.builder.master("local[2]")
            .config("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
            .config("spark.connect.grpc.binding.port", str(port))
            .config("spark.connect.authenticate.token", token)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .appName("pcw-integration-connect-server")
            .getOrCreate()
        )
        # Wait until the gRPC port actually accepts connections before yielding.
        _wait_port_open("127.0.0.1", port, timeout=60.0)
    except Exception as e:  # pragma: no cover - environment without a usable JVM
        if _prev_token is None:
            os.environ.pop("SPARK_CONNECT_AUTHENTICATE_TOKEN", None)
        else:
            os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = _prev_token
        pytest.skip(f"could not start local Spark Connect server: {e}")

    # JVM has the token now; stop leaking it into the process environment so
    # other test modules see a clean env. Clients receive the token explicitly.
    if _prev_token is None:
        os.environ.pop("SPARK_CONNECT_AUTHENTICATE_TOKEN", None)
    else:
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = _prev_token

    try:
        yield ("localhost", port, token)
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _wait_port_open(host: str, port: int, timeout: float) -> None:
    import time

    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as e:  # not up yet
            last_err = e
            time.sleep(0.25)
    raise TimeoutError(f"Connect gRPC port {host}:{port} never opened: {last_err}")
