# SPDX-License-Identifier: Apache-2.0
"""A duck-typed, grpcio-free replacement for ``SparkConnectServiceStub``.

PySpark's Connect client builds protobuf plans and calls a gRPC stub::

    self._stub = grpc_lib.SparkConnectServiceStub(self._channel)
    resp_iter = self._stub.ExecutePlan(req, metadata=md, timeout=t)

We install :class:`GrpcWebStub` in place of that stub. It speaks the
gRPC-Python *calling convention* - ``fn(request, *, metadata=None,
timeout=None)`` - but the wire is **grpc-web over a blocking byte transport**
(lane 3's ``SyncChannel``), never grpcio.

Per the calling convention (API_CONTRACT.md section 1):
  * server-streaming methods (``ExecutePlan``, ``ReattachExecute``) return an
    **iterator** of response protos;
  * unary methods return a single response proto;
  * ``AddArtifacts`` is client-streaming (``iter[req] -> resp``) - see the note
    in ``_call_client_stream`` about the ``SyncChannel`` seam.

On a non-OK ``grpc-status`` trailer we raise
``pyspark.errors ... SparkConnectGrpcException``.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .._contract import HttpResponse, SyncChannel, grpc_path
from .framing import (
    TRAILER_FLAG,
    encode_message,
    iter_frames,
    parse_trailers,
)

# This module may be imported directly (tests, lane wiring) before the package
# ``__init__`` runs, and the pyspark imports below transitively do ``import
# grpc``. Install the grpcio stub first so they resolve without grpcio (Pyodide).
from .._grpc_shim import install_grpc_shim as _install_grpc_shim

_install_grpc_shim()

# ``SparkConnectGrpcException`` moved between ``pyspark.errors`` and
# ``pyspark.errors.exceptions.connect`` across versions. API_CONTRACT.md names
# ``pyspark.errors``; import resiliently so we work across the pinned range
# (pyspark>=4.0,<4.2) without forking anything.
try:  # pragma: no cover - import shim, exercised indirectly
    from pyspark.errors import SparkConnectGrpcException  # type: ignore
except Exception:  # pragma: no cover
    from pyspark.errors.exceptions.connect import (  # type: ignore
        SparkConnectGrpcException,
    )

# Protos. Imported lazily-but-once from the real pyspark package; we never
# vendor copies (DECISIONS.md #2).
from pyspark.sql.connect import proto as _pb  # type: ignore


Metadata = Sequence[Tuple[str, str]]

# grpc-web required request headers (DECISIONS.md #1: no grpcio, fetch only).
_CONTENT_TYPE = "application/grpc-web+proto"
_BASE_HEADERS = {
    "content-type": _CONTENT_TYPE,
    "x-grpc-web": "1",
    # We send uncompressed frames and accept identity responses.
    "grpc-accept-encoding": "identity",
    "accept": _CONTENT_TYPE,
}

# grpc status code 0 == OK. Anything else is an error trailer.
_GRPC_STATUS_OK = "0"


def _headers_from_metadata(metadata: Optional[Metadata]) -> Dict[str, str]:
    """Merge gRPC metadata (list of (k, v) tuples) onto the grpc-web headers.

    gRPC metadata keys are case-insensitive ASCII; we lowercase them to match
    HTTP header conventions. Caller-supplied values win over our defaults only
    if they collide on a non-required header - but we always force the grpc-web
    content-type/marker back on so the proxy routes correctly.
    """
    headers: Dict[str, str] = dict(_BASE_HEADERS)
    if metadata:
        for key, value in metadata:
            headers[str(key).lower()] = str(value)
    # Required markers are non-negotiable regardless of metadata.
    headers["content-type"] = _CONTENT_TYPE
    headers["x-grpc-web"] = "1"
    return headers


def _raise_for_trailers(
    trailers: Optional[Dict[str, str]],
    *,
    http_status: Optional[int] = None,
    path: str = "",
) -> None:
    """Raise ``SparkConnectGrpcException`` unless trailers report grpc-status 0.

    grpc-web carries the real status in trailers even on HTTP 200. We treat a
    *missing* trailer as an error too, because a well-formed server response
    always terminates with one - its absence means the stream was cut.
    """
    if trailers is None:
        raise SparkConnectGrpcException(
            f"grpc-web: no trailer frame in response for {path} "
            f"(http_status={http_status}); stream truncated"
        )
    status = trailers.get("grpc-status")
    if status is None or status == _GRPC_STATUS_OK:
        # Missing grpc-status on a present trailer is treated as OK only if
        # there was one explicitly == 0; otherwise be strict.
        if status is None:
            raise SparkConnectGrpcException(
                f"grpc-web: trailer for {path} has no grpc-status: {trailers!r}"
            )
        return
    message = trailers.get("grpc-message", "")
    raise SparkConnectGrpcException(
        message=f"grpc-status {status}: {message}" if message else f"grpc-status {status}",
        reason=status,
    )


class GrpcWebStub:
    """Duck-typed replacement for ``SparkConnectServiceStub``.

    Constructed with a lane-3 :class:`SyncChannel` and a base URL. Exposes the
    10 service methods from API_CONTRACT.md section 1, each following the gRPC-Python
    calling convention ``fn(request, *, metadata=None, timeout=None)``.
    """

    def __init__(
        self,
        channel: SyncChannel,
        base_url: str = "",
        *,
        metadata: Optional[Metadata] = None,
        default_metadata: Optional[Metadata] = None,
    ) -> None:
        # SEAM (lane1<->lane2): lane 2's patch._default_stub_factory constructs
        # us as ``GrpcWebStub(sync_channel, metadata=[...])`` where ``metadata``
        # is the channel-level (auth/UA) header pairs from ChannelBuilder. We
        # accept ``metadata`` as the canonical channel-default keyword, and keep
        # ``default_metadata`` as a backward-compatible alias. See
        # team/findings-lane1-transport.md.
        self._channel = channel
        # base_url is informational here: ``path`` is the gRPC path the
        # SyncChannel resolves against its own configured host. We keep it so a
        # channel implementation that wants an absolute URL can read it.
        self._base_url = base_url.rstrip("/")
        chan_md = metadata if metadata is not None else default_metadata
        self._default_metadata: Tuple[Tuple[str, str], ...] = (
            tuple(chan_md) if chan_md else ()
        )
        # Bind the typed method callables (mirrors grpcio's stub __init__).
        self.ExecutePlan = self._make_server_stream(
            "ExecutePlan", _pb.ExecutePlanResponse
        )
        self.ReattachExecute = self._make_server_stream(
            "ReattachExecute", _pb.ExecutePlanResponse
        )
        self.ReleaseExecute = self._make_unary(
            "ReleaseExecute", _pb.ReleaseExecuteResponse
        )
        self.AnalyzePlan = self._make_unary("AnalyzePlan", _pb.AnalyzePlanResponse)
        self.Config = self._make_unary("Config", _pb.ConfigResponse)
        self.Interrupt = self._make_unary("Interrupt", _pb.InterruptResponse)
        self.AddArtifacts = self._make_client_stream(
            "AddArtifacts", _pb.AddArtifactsResponse
        )
        self.ArtifactStatus = self._make_unary(
            "ArtifactStatus", _pb.ArtifactStatusesResponse
        )
        self.ReleaseSession = self._make_unary(
            "ReleaseSession", _pb.ReleaseSessionResponse
        )
        self.FetchErrorDetails = self._make_unary(
            "FetchErrorDetails", _pb.FetchErrorDetailsResponse
        )

    # -- header assembly ---------------------------------------------------

    def _merge_metadata(self, metadata: Optional[Metadata]) -> List[Tuple[str, str]]:
        merged: List[Tuple[str, str]] = list(self._default_metadata)
        if metadata:
            merged.extend(metadata)
        return merged

    # -- method factories --------------------------------------------------

    def _make_unary(
        self, method: str, response_cls: Any
    ) -> Callable[..., Any]:
        path = grpc_path(method)

        def call(
            request: Any,
            *,
            metadata: Optional[Metadata] = None,
            timeout: Optional[float] = None,
        ) -> Any:
            return self._call_unary(path, request, response_cls, metadata, timeout)

        call.__name__ = method
        return call

    def _make_server_stream(
        self, method: str, response_cls: Any
    ) -> Callable[..., Iterator[Any]]:
        path = grpc_path(method)

        def call(
            request: Any,
            *,
            metadata: Optional[Metadata] = None,
            timeout: Optional[float] = None,
        ) -> Iterator[Any]:
            return self._call_server_stream(
                path, request, response_cls, metadata, timeout
            )

        call.__name__ = method
        return call

    def _make_client_stream(
        self, method: str, response_cls: Any
    ) -> Callable[..., Any]:
        path = grpc_path(method)

        def call(
            request_iterator: Iterable[Any],
            *,
            metadata: Optional[Metadata] = None,
            timeout: Optional[float] = None,
        ) -> Any:
            return self._call_client_stream(
                path, request_iterator, response_cls, metadata, timeout
            )

        call.__name__ = method
        return call

    # -- transport implementations ----------------------------------------

    def _call_unary(
        self,
        path: str,
        request: Any,
        response_cls: Any,
        metadata: Optional[Metadata],
        timeout: Optional[float],
    ) -> Any:
        headers = _headers_from_metadata(self._merge_metadata(metadata))
        body = encode_message(request.SerializeToString())
        resp: HttpResponse = self._channel.unary(path, body, headers, timeout)

        messages, trailers = self._decode_body(resp.body, path=path)
        # Some servers/proxies put grpc-status in HTTP headers instead of a
        # trailer frame for unary calls; fall back to those if no trailer frame.
        if trailers is None:
            trailers = self._trailers_from_headers(resp.headers)
        _raise_for_trailers(trailers, http_status=resp.status, path=path)

        if not messages:
            raise SparkConnectGrpcException(
                f"grpc-web: OK trailer but no message frame for unary {path}"
            )
        # A unary response is exactly one message; take the first, ignore extras
        # defensively (a compliant server sends one).
        return response_cls.FromString(messages[0])

    def _call_server_stream(
        self,
        path: str,
        request: Any,
        response_cls: Any,
        metadata: Optional[Metadata],
        timeout: Optional[float],
    ) -> Iterator[Any]:
        headers = _headers_from_metadata(self._merge_metadata(metadata))
        body = encode_message(request.SerializeToString())
        chunk_iter = self._channel.server_stream(path, body, headers, timeout)
        return self._stream_responses(chunk_iter, response_cls, path=path)

    def _call_client_stream(
        self,
        path: str,
        request_iterator: Iterable[Any],
        response_cls: Any,
        metadata: Optional[Metadata],
        timeout: Optional[float],
    ) -> Any:
        # CONTRACT NOTE (lane1): the SyncChannel seam (API_CONTRACT.md section 1) only
        # defines unary() and server_stream() - there is no client-streaming
        # entry point. grpc-web itself has no true client streaming either; the
        # canonical lowering is to concatenate all request frames into one body
        # and POST it as a unary call. We do exactly that: drain the request
        # iterator, frame each message, and send the concatenation via unary().
        # This matches how AddArtifacts is used (a bounded set of artifact
        # chunks) and needs no new SyncChannel method.
        headers = _headers_from_metadata(self._merge_metadata(metadata))
        body = b"".join(
            encode_message(req.SerializeToString()) for req in request_iterator
        )
        resp: HttpResponse = self._channel.unary(path, body, headers, timeout)

        messages, trailers = self._decode_body(resp.body, path=path)
        if trailers is None:
            trailers = self._trailers_from_headers(resp.headers)
        _raise_for_trailers(trailers, http_status=resp.status, path=path)

        if not messages:
            raise SparkConnectGrpcException(
                f"grpc-web: OK trailer but no message frame for {path}"
            )
        return response_cls.FromString(messages[0])

    # -- decoding helpers --------------------------------------------------

    @staticmethod
    def _decode_body(
        body: bytes, *, path: str
    ) -> Tuple[List[bytes], Optional[Dict[str, str]]]:
        """Decode a complete grpc-web body into (message payloads, trailers)."""
        messages: List[bytes] = []
        trailers: Optional[Dict[str, str]] = None
        for frame in iter_frames(body):
            if frame.is_compressed:
                raise SparkConnectGrpcException(
                    f"grpc-web: compressed frame not supported (path={path})"
                )
            if frame.is_trailer:
                trailers = parse_trailers(frame.payload)
            else:
                messages.append(frame.payload)
        return messages, trailers

    def _stream_responses(
        self,
        chunk_iter: Iterator[bytes],
        response_cls: Any,
        *,
        path: str,
    ) -> Iterator[Any]:
        """Reframe a stream of raw byte chunks into response protos.

        Lane 3 yields raw grpc-web bytes "as they arrive off the wire" - a
        chunk may contain several frames, a single frame, or split a frame
        across a boundary. We buffer until we have whole frames, decode message
        frames into protos, and on the trailer frame validate grpc-status,
        raising on a non-OK status.

        **Dropped-stream recovery (DECISIONS.md #6).** A stream that ends with
        *no trailer frame* (or with a trailing partial frame) is a broken
        connection mid-result. We must NOT raise here: PySpark's
        ``ExecutePlanResponseReattachableIterator`` only recovers a broken stream
        when the underlying iterator ends *cleanly* (``StopIteration``) before a
        ``ResultComplete`` response - that is what makes it issue
        ``ReattachExecute`` from the last ``response_id``. Its retry path, by
        contrast, only retries ``grpc.RpcError`` (UNAVAILABLE / INTERNAL+
        INVALID_CURSOR), which our :class:`SparkConnectGrpcException` is not - so
        raising here would surface the drop to the user instead of recovering it.
        Therefore on a trailer-less end we simply *return* (StopIteration) and let
        the reattach machinery refetch the rest. Verified end-to-end by the
        integration fault-injection test
        (``tests/integration/test_real_round_trip.py::test_midstream_disconnect_recovers_via_reattach``).

        We DO still raise on a present-but-non-OK trailer (a real server error)
        and on a compressed frame (unsupported).
        """
        buffer = bytearray()
        saw_trailer = False
        trailers: Optional[Dict[str, str]] = None

        for chunk in chunk_iter:
            if not chunk:
                continue
            buffer.extend(chunk)
            # Drain every *complete* frame currently buffered. ``_take_frame``
            # mutates ``buffer`` in place (deletes the consumed prefix), so we
            # re-check from the front each iteration - no stale offset bug.
            while True:
                frame = _take_frame(buffer)
                if frame is None:
                    break  # only a partial frame remains; wait for more bytes
                if frame.is_compressed:
                    raise SparkConnectGrpcException(
                        f"grpc-web: compressed frame not supported (path={path})"
                    )
                if frame.is_trailer:
                    saw_trailer = True
                    trailers = parse_trailers(frame.payload)
                else:
                    yield response_cls.FromString(frame.payload)

        if not saw_trailer:
            # Broken stream (no terminating trailer; a trailing partial frame in
            # ``buffer`` means the same thing - the connection was cut mid-frame).
            # End the iterator cleanly so the reattachable iterator recovers via
            # ReattachExecute. See the method docstring for why we must not raise.
            return
        _raise_for_trailers(trailers, path=path)

    @staticmethod
    def _trailers_from_headers(headers: Dict[str, str]) -> Optional[Dict[str, str]]:
        """Read grpc-status/message from HTTP headers as a fallback.

        Some grpc-web deployments surface status as response headers (notably
        for empty unary responses). Returns None if no grpc-status header.
        """
        lowered = {str(k).lower(): v for k, v in (headers or {}).items()}
        if "grpc-status" not in lowered:
            return None
        out = {"grpc-status": str(lowered["grpc-status"])}
        if "grpc-message" in lowered:
            out["grpc-message"] = str(lowered["grpc-message"])
        return out


def _take_frame(buffer: bytearray):
    """Pop and return the first whole :class:`Frame` from *buffer*, in place.

    Deletes the consumed bytes (header + body) from the front of ``buffer`` and
    returns the decoded ``Frame``. Returns ``None`` if ``buffer`` does not yet
    hold a complete frame (partial header or partial body) - the caller keeps
    buffering more chunks. This keeps the streaming reassembly bug-free across
    arbitrary chunk boundaries (SPARK-53525-style splits).
    """
    from .framing import HEADER_LEN, _HEADER, Frame  # local import: cohesion

    n = len(buffer)
    if n < HEADER_LEN:
        return None
    flags, length = _HEADER.unpack_from(buffer, 0)
    end = HEADER_LEN + length
    if end > n:
        return None  # body not fully arrived yet
    payload = bytes(buffer[HEADER_LEN:end])
    del buffer[:end]
    return Frame(flags, payload)


__all__ = ["GrpcWebStub"]
