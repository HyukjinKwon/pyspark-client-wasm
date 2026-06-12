# SPDX-License-Identifier: Apache-2.0
"""Shared seam types — see /API_CONTRACT.md. Mirrors /_contract_seam.py.
Integrator-owned; do not edit without a COORDINATION.md note."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Protocol, runtime_checkable

GRPC_SERVICE = "spark.connect.SparkConnectService"


def grpc_path(method: str) -> str:
    return f"/{GRPC_SERVICE}/{method}"


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


@runtime_checkable
class SyncChannel(Protocol):
    def unary(
        self, path: str, body: bytes, headers: dict[str, str], timeout: float | None
    ) -> HttpResponse: ...

    def server_stream(
        self, path: str, body: bytes, headers: dict[str, str], timeout: float | None
    ) -> Iterator[bytes]: ...
