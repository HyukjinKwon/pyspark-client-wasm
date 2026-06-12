# SPDX-License-Identifier: Apache-2.0
"""Lane 3 unit tests: exercise SabSyncChannel against a fake sync backend.

No browser, no Pyodide, no grpcio, no network. The fake :class:`FakeBackend`
implements the same synchronous ``SyncBackend`` protocol the real Atomics
backend does, so these tests cover the channel's request marshalling, the
HttpResponse contract for ``unary``, in-order chunk delivery for
``server_stream``, and timeout propagation — everything that is testable off the
browser.
"""
from __future__ import annotations

import time
from typing import Iterator

import pytest

from pyspark_connect_web._contract import HttpResponse, SyncChannel, grpc_path
from pyspark_connect_web.worker import (
    SabSyncChannel,
    TransportError,
    TransportTimeout,
)


# --------------------------------------------------------------------------- #
# A fake synchronous backend. It records what the channel handed down and
# returns scripted responses. It is fully synchronous (blocking) — matching the
# real backend's contract — but does no I/O.
# --------------------------------------------------------------------------- #
class FakeBackend:
    def __init__(self, *, unary_response=None, stream_chunks=None,
                 raise_on_unary=None, stream_error_after=None,
                 stream_sleep=0.0, unary_sleep=0.0):
        self.calls: list[dict] = []
        self._unary_response = unary_response
        self._stream_chunks = stream_chunks or []
        self._raise_on_unary = raise_on_unary
        self._stream_error_after = stream_error_after
        self._stream_sleep = stream_sleep
        self._unary_sleep = unary_sleep

    def unary(self, request: dict) -> HttpResponse:
        self.calls.append(request)
        if self._unary_sleep:
            time.sleep(self._unary_sleep)
        if self._raise_on_unary is not None:
            raise self._raise_on_unary
        return self._unary_response

    def server_stream(self, request: dict) -> Iterator[bytes]:
        self.calls.append(request)
        for i, chunk in enumerate(self._stream_chunks):
            if self._stream_sleep:
                time.sleep(self._stream_sleep)
            if self._stream_error_after is not None and i == self._stream_error_after:
                raise TransportError("simulated mid-stream transport failure")
            yield chunk


BASE = "https://envoy.example:8081"
EXEC_PATH = grpc_path("ExecutePlan")
ANALYZE_PATH = grpc_path("AnalyzePlan")


# --------------------------------------------------------------------------- #
# Construction / environment
# --------------------------------------------------------------------------- #
def test_channel_satisfies_protocol():
    ch = SabSyncChannel(BASE, backend=FakeBackend(unary_response=HttpResponse(200)))
    assert isinstance(ch, SyncChannel)


def test_no_backend_outside_pyodide_raises():
    # On CPython (not Pyodide) with no injected backend, construction must fail
    # clearly rather than silently producing a dead channel.
    with pytest.raises(TransportError):
        SabSyncChannel(BASE)


def test_base_url_trailing_slash_normalized():
    backend = FakeBackend(unary_response=HttpResponse(200))
    ch = SabSyncChannel(BASE + "/", backend=backend)
    ch.unary(ANALYZE_PATH, b"x", {}, None)
    assert backend.calls[0]["url"] == BASE + ANALYZE_PATH


# --------------------------------------------------------------------------- #
# unary
# --------------------------------------------------------------------------- #
def test_unary_returns_http_response():
    resp = HttpResponse(status=200, headers={"grpc-status": "0"}, body=b"\x00\x00\x00\x00\x05hello")
    backend = FakeBackend(unary_response=resp)
    ch = SabSyncChannel(BASE, backend=backend)

    out = ch.unary(ANALYZE_PATH, b"req-bytes", {"x-meta": "v"}, 30.0)

    assert isinstance(out, HttpResponse)
    assert out.status == 200
    assert out.body == b"\x00\x00\x00\x00\x05hello"
    # the request the channel built for the backend
    call = backend.calls[0]
    assert call["kind"] == "unary"
    assert call["url"] == BASE + ANALYZE_PATH
    assert call["path"] == ANALYZE_PATH
    assert call["body"] == b"req-bytes"
    assert call["headers"] == {"x-meta": "v"}
    assert call["timeout"] == 30.0


def test_unary_rejects_non_httpresponse_from_backend():
    class Bad:
        def unary(self, request):
            return ("not", "an", "HttpResponse")

        def server_stream(self, request):
            yield b""

    ch = SabSyncChannel(BASE, backend=Bad())
    with pytest.raises(TransportError):
        ch.unary(ANALYZE_PATH, b"", {}, None)


def test_unary_propagates_transport_error():
    backend = FakeBackend(raise_on_unary=TransportError("boom"))
    ch = SabSyncChannel(BASE, backend=backend)
    with pytest.raises(TransportError, match="boom"):
        ch.unary(ANALYZE_PATH, b"", {}, None)


def test_unary_propagates_timeout():
    backend = FakeBackend(raise_on_unary=TransportTimeout("slow"))
    ch = SabSyncChannel(BASE, backend=backend)
    with pytest.raises(TransportTimeout):
        ch.unary(ANALYZE_PATH, b"", {}, 0.01)


def test_path_must_be_absolute():
    ch = SabSyncChannel(BASE, backend=FakeBackend(unary_response=HttpResponse(200)))
    with pytest.raises(TransportError):
        ch.unary("spark.connect/NoSlash", b"", {}, None)


# --------------------------------------------------------------------------- #
# server_stream
# --------------------------------------------------------------------------- #
def test_server_stream_yields_chunks_in_order():
    chunks = [b"frame-0", b"frame-1", b"frame-2"]
    backend = FakeBackend(stream_chunks=chunks)
    ch = SabSyncChannel(BASE, backend=backend)

    got = list(ch.server_stream(EXEC_PATH, b"plan", {"h": "1"}, None))

    assert got == chunks
    call = backend.calls[0]
    assert call["kind"] == "server_stream"
    assert call["url"] == BASE + EXEC_PATH
    assert call["body"] == b"plan"


def test_server_stream_is_lazy_generator():
    # The channel must not buffer the whole stream up front: lane 1 needs chunks
    # as they arrive so a mid-stream disconnect (DECISIONS.md #6) is prompt.
    consumed = []

    class Recording(FakeBackend):
        def server_stream(self, request):
            for c in [b"a", b"b", b"c"]:
                consumed.append(c)
                yield c

    ch = SabSyncChannel(BASE, backend=Recording())
    it = ch.server_stream(EXEC_PATH, b"", {}, None)
    assert consumed == []          # nothing pulled yet
    assert next(it) == b"a"
    assert consumed == [b"a"]      # exactly one chunk pulled
    assert next(it) == b"b"
    assert consumed == [b"a", b"b"]


def test_server_stream_mid_stream_error_surfaces():
    # Two good chunks then a transport failure — the consumer sees the first two
    # then the exception, modelling a broken stream that lane 1 recovers via
    # ReattachExecute.
    backend = FakeBackend(stream_chunks=[b"a", b"b", b"c"], stream_error_after=2)
    ch = SabSyncChannel(BASE, backend=backend)
    it = ch.server_stream(EXEC_PATH, b"", {}, None)
    assert next(it) == b"a"
    assert next(it) == b"b"
    with pytest.raises(TransportError, match="mid-stream"):
        next(it)


def test_server_stream_empty():
    backend = FakeBackend(stream_chunks=[])
    ch = SabSyncChannel(BASE, backend=backend)
    assert list(ch.server_stream(EXEC_PATH, b"", {}, None)) == []


# --------------------------------------------------------------------------- #
# A fake that models the timeout semantics the Atomics backend enforces, so the
# timeout contract is tested without a browser.
# --------------------------------------------------------------------------- #
def test_stream_timeout_semantics_with_fake_clock():
    class TimingBackend:
        """Raises TransportTimeout if cumulative chunk delay exceeds timeout."""

        def __init__(self, per_chunk, n_chunks):
            self.per_chunk = per_chunk
            self.n_chunks = n_chunks

        def unary(self, request):  # pragma: no cover - not used here
            raise NotImplementedError

        def server_stream(self, request):
            timeout = request["timeout"]
            elapsed = 0.0
            for i in range(self.n_chunks):
                elapsed += self.per_chunk
                if timeout is not None and elapsed > timeout:
                    raise TransportTimeout(f"stream exceeded {timeout}s")
                yield f"chunk-{i}".encode()

    ch = SabSyncChannel(BASE, backend=TimingBackend(per_chunk=1.0, n_chunks=10))
    it = ch.server_stream(EXEC_PATH, b"", {}, 2.5)
    assert next(it) == b"chunk-0"
    assert next(it) == b"chunk-1"
    with pytest.raises(TransportTimeout):
        next(it)
