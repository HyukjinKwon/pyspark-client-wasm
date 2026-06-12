# SPDX-License-Identifier: Apache-2.0
"""pyspark-connect-web - PySpark in JupyterLite.

Public API:

    import pyspark_connect_web as pcw
    pcw.install()                      # idempotent monkey-patch of SparkConnectClient
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote("sc://host:8081/;transport=grpcweb").getOrCreate()

Lane 2 owns this file and ``patch.py``. The heavy lifting lives in ``patch.py``;
this module is the stable public surface (``install`` + ``__version__``).
"""
from __future__ import annotations

# PySpark's ``pyspark.sql.connect`` stack does ``import grpc`` at module load,
# but grpcio is absent in Pyodide. Register a stub *before*
# anything pulls in pyspark so those imports resolve. No-op if real grpcio is
# present (local dev / CI parity), so we never shadow the genuine library.
from ._grpc_shim import install_grpc_shim as _install_grpc_shim

_install_grpc_shim()

from .patch import (
    SUPPORTED_PYSPARK_RANGE,
    UnsupportedPySparkError,
    install,
    is_installed,
    set_channel_factory,
    set_stub_factory,
    uninstall,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "install",
    "uninstall",
    "is_installed",
    "set_channel_factory",
    "set_stub_factory",
    "SUPPORTED_PYSPARK_RANGE",
    "UnsupportedPySparkError",
]
