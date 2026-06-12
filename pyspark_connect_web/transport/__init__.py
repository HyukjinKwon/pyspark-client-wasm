# SPDX-License-Identifier: Apache-2.0
"""Lane 1: grpc-web stub + wire framing.

This package owns the byte-level grpc-web protocol: frame encode/decode
(``framing``) and the duck-typed ``SparkConnectServiceStub`` replacement
(``grpcweb.GrpcWebStub``). It never imports ``grpcio`` and never touches the
browser - it talks to the blocking ``SyncChannel`` (see ``_contract.py``).
"""
from __future__ import annotations

from .framing import (
    TRAILER_FLAG,
    Frame,
    encode_message,
    iter_frames,
    parse_trailers,
)
from .grpcweb import GrpcWebStub

__all__ = [
    "TRAILER_FLAG",
    "Frame",
    "encode_message",
    "iter_frames",
    "parse_trailers",
    "GrpcWebStub",
]
