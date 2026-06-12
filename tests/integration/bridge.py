# SPDX-License-Identifier: Apache-2.0
"""A pure-Python grpc-web <-> gRPC bridge — the test-only stand-in for Envoy.

In production the path is::

    GrpcWebStub (lane1)  -- grpc-web frames -->  Envoy grpc_web filter  -- gRPC -->  Spark Connect

Envoy is a C++ proxy that translates the grpc-web wire format (length-prefixed
frames over HTTP, trailers carried in a final ``0x80`` frame) into real HTTP/2
gRPC and back. There is no Envoy in this hermetic test, so this module reproduces
exactly that translation in Python, using ``grpcio`` purely as the downstream
gRPC client (allowed in tests).

``GrpcWebBridgeChannel`` implements lane 3's :class:`SyncChannel` protocol
(``unary`` / ``server_stream``) so it drops straight into
``pcw.set_channel_factory(...)``. For each call it:

1. Receives the grpc-web framed ``body`` produced by ``GrpcWebStub`` and decodes
   the data frames using lane 1's *own* ``transport/framing`` (real framing in).
2. Translates the grpc-web HTTP headers into gRPC metadata and forwards the raw
   proto bytes to the real Connect server over ``grpcio`` with byte-passthrough
   (de)serializers.
3. Re-encodes each gRPC response message as a grpc-web data frame and appends a
   ``0x80`` trailer frame carrying ``grpc-status`` / ``grpc-message`` — exactly
   what ``GrpcWebStub`` expects back from Envoy (real framing out).

This exercises lane 1's framing in BOTH directions against a real server.
"""
from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple

import grpc  # allowed in tests only (DECISIONS.md #1 is scoped to the package)

from pyspark_connect_web._contract import HttpResponse
from pyspark_connect_web.transport.framing import (
    TRAILER_FLAG,
    encode_message,
    encode_trailers,
    iter_frames,
)

# grpc-web request headers that must NOT be forwarded as gRPC metadata: they are
# HTTP/transport-level, and grpcio rejects reserved (``:``-prefixed) or
# content-type keys. Everything else (user_agent, session params, authorization)
# is real gRPC metadata the server expects.
_HOP_BY_HOP = {
    "content-type",
    "accept",
    "x-grpc-web",
    "grpc-accept-encoding",
    "grpc-encoding",
    "te",
    "host",
    "user-agent",  # grpc sets its own; PySpark passes user_agent as metadata key
}


class GrpcWebBridgeChannel:
    """grpc-web framed bytes in, real gRPC out, grpc-web framed bytes back.

    Parameters
    ----------
    target:
        ``host:port`` of the real Spark Connect gRPC server.
    extra_metadata:
        Channel-level gRPC metadata to inject on every call — notably the
        ``authorization`` bearer token, which PySpark's ``ChannelBuilder.metadata()``
        deliberately omits (it rides in grpc call-credentials in the native client,
        which has no grpc-web analogue). We add it here so the server authenticates.
    """

    def __init__(
        self,
        target: str,
        extra_metadata: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        self._target = target
        self._extra_metadata = list(extra_metadata or [])
        # A single insecure channel; the server runs in local insecure mode.
        self._channel = grpc.insecure_channel(target)

    # -- SyncChannel protocol -------------------------------------------------

    def unary(
        self,
        path: str,
        body: bytes,
        headers: Dict[str, str],
        timeout: Optional[float],
    ) -> HttpResponse:
        request_bytes = self._single_request_bytes(path, body)
        metadata = self._to_metadata(headers)
        callable_ = self._channel.unary_unary(
            path,
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        try:
            resp_bytes, call = callable_.with_call(
                request_bytes, metadata=metadata, timeout=timeout
            )
        except grpc.RpcError as e:
            # Map a downstream gRPC error to a grpc-web HTTP 200 + error trailer,
            # exactly as Envoy would. GrpcWebStub turns the trailer into a
            # SparkConnectGrpcException.
            return HttpResponse(
                status=200,
                headers={},
                body=encode_trailers(int(e.code().value[0]), e.details() or ""),
            )
        out = bytearray()
        out += encode_message(resp_bytes)
        out += encode_trailers(int(call.code().value[0]), call.details() or "")
        return HttpResponse(status=200, headers={}, body=bytes(out))

    def server_stream(
        self,
        path: str,
        body: bytes,
        headers: Dict[str, str],
        timeout: Optional[float],
    ) -> Iterator[bytes]:
        request_bytes = self._single_request_bytes(path, body)
        metadata = self._to_metadata(headers)
        callable_ = self._channel.unary_stream(
            path,
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        return self._stream_frames(callable_, request_bytes, metadata, timeout)

    def close(self) -> None:
        try:
            self._channel.close()
        except Exception:
            pass

    # -- helpers --------------------------------------------------------------

    def _stream_frames(
        self,
        callable_: "grpc.UnaryStreamMultiCallable",
        request_bytes: bytes,
        metadata: List[Tuple[str, str]],
        timeout: Optional[float],
    ) -> Iterator[bytes]:
        """Yield grpc-web frame chunks as the gRPC stream produces messages.

        Each downstream message becomes a data frame chunk; the stream's final
        gRPC status becomes a trailer frame chunk. This mirrors how Envoy streams
        grpc-web bytes "as they arrive off the wire" (lane 3 contract).
        """
        call = callable_(request_bytes, metadata=metadata, timeout=timeout)
        try:
            for msg_bytes in call:
                yield encode_message(msg_bytes)
        except grpc.RpcError as e:
            yield encode_trailers(int(e.code().value[0]), e.details() or "")
            return
        # OK stream: emit the terminating trailer frame.
        yield encode_trailers(int(call.code().value[0]), call.details() or "")

    @staticmethod
    def _single_request_bytes(path: str, body: bytes) -> bytes:
        """Decode the grpc-web request body to the single inner proto's bytes.

        Uses lane 1's real ``iter_frames`` so we exercise its framing on the
        request side too. PySpark always sends exactly one request message per
        unary/server-stream call; AddArtifacts (client-stream) is lowered by the
        stub to a concatenation of frames, which we reject loudly here because the
        v0 matrix never hits it and forwarding a concatenation as one gRPC message
        would be wrong.
        """
        payloads = [f.payload for f in iter_frames(body) if not (f.flags & TRAILER_FLAG)]
        if len(payloads) != 1:
            raise ValueError(
                f"bridge expected exactly one request frame for {path}, "
                f"got {len(payloads)} (client-streaming is out of v0 scope)"
            )
        return payloads[0]

    def _to_metadata(self, headers: Dict[str, str]) -> List[Tuple[str, str]]:
        md: List[Tuple[str, str]] = []
        for key, value in headers.items():
            lkey = str(key).lower()
            if lkey in _HOP_BY_HOP or lkey.startswith(":"):
                continue
            md.append((lkey, str(value)))
        md.extend(self._extra_metadata)
        return md
