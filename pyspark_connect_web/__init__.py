# SPDX-License-Identifier: Apache-2.0
"""pyspark-connect-web — PySpark in JupyterLite.

Public API:

    import pyspark_connect_web as pcw
    pcw.install()                      # idempotent monkey-patch of SparkConnectClient
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote("sc://host:8081/;transport=grpcweb").getOrCreate()

Lane 2 owns this file and `patch.py`. The body below is a placeholder skeleton
to be filled per API_CONTRACT.md §2 — kept importable so other lanes can wire up.
"""
from __future__ import annotations

__version__ = "0.0.1.dev0"

_INSTALLED = False


def install() -> None:
    """Monkey-patch pyspark.sql.connect to use the grpc-web transport. Idempotent."""
    global _INSTALLED
    if _INSTALLED:
        return
    raise NotImplementedError(
        "lane 2: implement install() per API_CONTRACT.md §2 "
        "(version-guard pyspark, swap the stub, teach the connection parser)."
    )
