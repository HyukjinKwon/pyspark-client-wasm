# SPDX-License-Identifier: Apache-2.0
"""Shared types for the lane seam. Integrator-owned — do not edit without a
COORDINATION.md note. Lanes import these so transport (1), patch (2), and the
sync bridge (3) agree on shapes.

This file is copied into ``pyspark_connect_web/_contract.py`` by lane 2 during
package scaffolding; kept at repo root too so it is reviewable as the contract.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Protocol, runtime_checkable

GRPC_SERVICE = "spark.connect.SparkConnectService"


def grpc_path(method: str) -> str:
    """e.g. grpc_path("ExecutePlan") -> /spark.connect.SparkConnectService/ExecutePlan"""
    return f"/{GRPC_SERVICE}/{method}"


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


@runtime_checkable
class SyncChannel(Protocol):
    """Blocking byte transport. Lane 3 implements (Atomics+SharedArrayBuffer);
    lane 1 consumes. Must block the calling (worker) thread until bytes arrive."""

    def unary(
        self, path: str, body: bytes, headers: dict[str, str], timeout: float | None
    ) -> HttpResponse: ...

    def server_stream(
        self, path: str, body: bytes, headers: dict[str, str], timeout: float | None
    ) -> Iterator[bytes]:
        """Yield raw grpc-web frame bytes as they arrive."""
        ...
