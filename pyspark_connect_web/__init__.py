# SPDX-License-Identifier: Apache-2.0
"""pyspark-connect-web — PySpark in JupyterLite.

Public API:

    import pyspark_connect_web as pcw
    pcw.install()                      # idempotent monkey-patch of SparkConnectClient
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote("sc://host:8081/;transport=grpcweb").getOrCreate()

Lane 2 owns this file and ``patch.py``. The heavy lifting lives in ``patch.py``;
this module is the stable public surface (``install`` + ``__version__``).
"""
from __future__ import annotations

from .patch import (
    SUPPORTED_PYSPARK_RANGE,
    UnsupportedPySparkError,
    install,
    is_installed,
    set_channel_factory,
    set_stub_factory,
    uninstall,
)

__version__ = "0.0.1.dev0"

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
