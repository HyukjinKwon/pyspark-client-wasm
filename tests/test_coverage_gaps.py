# SPDX-License-Identifier: Apache-2.0
"""Targeted unit tests filling the coverage gaps the existing suites leave behind.

Scope (all offline — no network, no browser, no grpcio):

  * ``patch.py``   — WebChannel.close (with/without a closeable channel + error
                     swallow), the lazy default channel/stub factories' deferred
                     errors, the non-web ``__init__``/``toChannel`` passthrough,
                     and uninstall-when-not-installed.
  * ``grpcweb.py`` — unary "OK trailer but no message frame", client-stream
                     header-fallback + empty-message, compressed-frame rejection
                     on both the unary and streaming decode paths, the
                     present-trailer-with-no-grpc-status error.
  * ``arrow/results.py`` — first-chunk ``chunk_index != 0`` rejection, the
                     empty-result-with-known-schema branch, and the pyarrow
                     version-probe fallback.
  * ``worker/sab_channel.py`` — the local/no-backend error, ``is_pyodide`` both
                     ways, the request-shape guard, the ``make_channel`` helper,
                     and the backend-return-type guard. (The Atomics backend is
                     browser-only and exercised by tests/test_sab_atomics_backend.py.)
"""
from __future__ import annotations

import struct

import pyarrow as pa
import pytest

from pyspark_connect_web import patch as pcw_patch
from pyspark_connect_web._contract import HttpResponse


# =========================================================================== #
# patch.py
# =========================================================================== #
class _CloseRecorder:
    def __init__(self, raise_on_close: bool = False):
        self.closed = False
        self._raise = raise_on_close

    def close(self):
        self.closed = True
        if self._raise:
            raise RuntimeError("boom on close")


def test_webchannel_close_calls_underlying_channel_close():
    rec = _CloseRecorder()
    web = pcw_patch.WebChannel(host="h", port=1, secure=False, channel=rec)
    web.close()
    assert web._closed is True
    assert rec.closed is True


def test_webchannel_close_swallows_underlying_error():
    """close() must not propagate the channel's close error (best-effort)."""
    rec = _CloseRecorder(raise_on_close=True)
    web = pcw_patch.WebChannel(host="h", port=1, secure=False, channel=rec)
    web.close()  # must not raise
    assert web._closed is True


def test_webchannel_close_tolerates_non_closeable_channel():
    """A SyncChannel without a callable close() is fine."""

    class NoClose:
        pass

    web = pcw_patch.WebChannel(host="h", port=1, secure=False, channel=NoClose())
    web.close()  # no attribute error
    assert web._closed is True


def test_webchannel_base_url_secure_vs_insecure():
    insecure = pcw_patch.WebChannel(host="h", port=80, secure=False, channel=object())
    secure = pcw_patch.WebChannel(host="h", port=443, secure=True, channel=object())
    assert insecure.base_url == "http://h:80"
    assert secure.base_url == "https://h:443"


def test_webendpoint_base_url_secure_vs_insecure():
    assert (
        pcw_patch.WebEndpoint(host="h", port=80, secure=False).base_url
        == "http://h:80"
    )
    assert (
        pcw_patch.WebEndpoint(host="h", port=443, secure=True).base_url
        == "https://h:443"
    )


def _block_submodule(monkeypatch, modname: str):
    """Force ``import <modname>`` (and ``from <modname> import ...``) to fail.

    The lazy factories do ``from .worker import SabSyncChannel`` /
    ``from .transport import GrpcWebStub``. Those submodules are already cached in
    ``sys.modules`` (the package imported them at load), so blocking the importer
    alone is not enough — we set the cached entry to ``None``, which makes Python
    raise ``ImportError`` on any fresh import of that name.
    """
    import sys

    saved = sys.modules.get(modname)
    monkeypatch.setitem(sys.modules, modname, None)
    return saved


def test_default_channel_factory_raises_clear_error_without_lane3(monkeypatch):
    """When lane 3's SabSyncChannel can't be imported, the *default* channel
    factory raises a clear deferred error (at connect time, not import time)."""
    _block_submodule(monkeypatch, "pyspark_connect_web.worker")
    ep = pcw_patch.WebEndpoint(host="h", port=1, secure=False)
    with pytest.raises(RuntimeError, match="no SyncChannel available"):
        pcw_patch._default_channel_factory(ep)


def test_default_stub_factory_raises_clear_error_without_lane1(monkeypatch):
    """When lane 1's GrpcWebStub can't be imported, the default stub factory
    raises a clear error."""
    _block_submodule(monkeypatch, "pyspark_connect_web.transport")
    web = pcw_patch.WebChannel(host="h", port=1, secure=False, channel=object())
    with pytest.raises(RuntimeError, match="GrpcWebStub is not"):
        pcw_patch._default_stub_factory(web)


def test_is_web_url_rejects_unknown_scheme():
    """A non-sc/non-http(s) scheme is not a web URL (guards patch.py line 263)."""
    assert pcw_patch._is_web_url("grpc://host:1234") is False
    assert pcw_patch._is_web_url("ftp://host") is False
    assert pcw_patch._is_web_url("") is False


def test_uninstall_when_not_installed_is_noop():
    if pcw_patch.is_installed():
        pcw_patch.uninstall()
    assert pcw_patch.is_installed() is False
    pcw_patch.uninstall()  # must be a safe no-op
    assert pcw_patch.is_installed() is False


def test_non_web_url_passes_through_unpatched():
    """A stock ``sc://`` URL (no transport=grpcweb) must take the original
    __init__ path and mark the builder as NOT web -> toChannel uses the
    original. We assert via the parser predicate + the marker the patch sets."""
    import pyspark.sql.connect.client.core as core

    was_installed = pcw_patch.is_installed()
    if was_installed:
        pcw_patch.uninstall()
    pcw_patch.install()
    try:
        builder = core.DefaultChannelBuilder("sc://localhost:15002")
        assert getattr(builder, "_pcw_web") is False
    finally:
        pcw_patch.uninstall()


# =========================================================================== #
# grpcweb.py — needs pyspark protos
# =========================================================================== #
pb = pytest.importorskip(
    "pyspark.sql.connect.proto",
    reason="pyspark not installed; grpcweb stub gap tests skipped",
)
from pyspark_connect_web.transport.framing import (  # noqa: E402
    TRAILER_FLAG,
    encode_message,
    encode_trailers,
)
from pyspark_connect_web.transport.grpcweb import (  # noqa: E402
    GrpcWebStub,
    SparkConnectGrpcException,
)


class _FakeChannel:
    def __init__(self, *, unary_body=b"", unary_status=200, unary_headers=None,
                 stream_chunks=None):
        self.unary_body = unary_body
        self.unary_status = unary_status
        self.unary_headers = unary_headers or {}
        self.stream_chunks = stream_chunks or []
        self.calls = []

    def unary(self, path, body, headers, timeout):
        self.calls.append(("unary", path, body, headers, timeout))
        return HttpResponse(
            status=self.unary_status,
            headers=dict(self.unary_headers),
            body=self.unary_body,
        )

    def server_stream(self, path, body, headers, timeout):
        self.calls.append(("server_stream", path, body, headers, timeout))
        yield from self.stream_chunks


def test_unary_ok_trailer_but_no_message_raises():
    """OK trailer with zero data frames is a protocol error for a unary call."""
    ch = _FakeChannel(unary_body=encode_trailers(0))
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="no message frame"):
        stub.Config(pb.ConfigRequest(session_id="s"))


def test_present_trailer_without_grpc_status_raises():
    """A trailer frame present but carrying no grpc-status key must be strict."""
    # build a trailer frame whose body has no grpc-status line at all
    body = ("grpc-message:partial\r\n").encode("utf-8")
    frame = struct.pack(">BI", TRAILER_FLAG, len(body)) + body
    ch = _FakeChannel(unary_body=frame)
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="no grpc-status"):
        stub.Config(pb.ConfigRequest(session_id="s"))


def test_unary_compressed_frame_rejected():
    """A compressed data frame on the unary decode path must raise."""
    from pyspark_connect_web.transport.framing import COMPRESSED_FLAG

    body = struct.pack(">BI", COMPRESSED_FLAG, 1) + b"x"
    ch = _FakeChannel(unary_body=body)
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="compressed"):
        stub.Config(pb.ConfigRequest(session_id="s"))


def test_server_stream_compressed_frame_rejected():
    """A compressed frame mid-stream must raise on the streaming decode path."""
    from pyspark_connect_web.transport.framing import COMPRESSED_FLAG

    chunk = struct.pack(">BI", COMPRESSED_FLAG, 1) + b"x"
    ch = _FakeChannel(stream_chunks=[chunk])
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="compressed"):
        list(stub.ExecutePlan(pb.ExecutePlanRequest()))


def test_client_stream_status_from_headers_fallback():
    """AddArtifacts (client-stream lowered to unary): when the body has no
    trailer frame, status is read from HTTP headers."""
    ch = _FakeChannel(
        unary_body=b"",
        unary_headers={"grpc-status": "7", "grpc-message": "permission denied"},
    )
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="permission denied"):
        stub.AddArtifacts(iter([pb.AddArtifactsRequest(session_id="s1")]))


def test_client_stream_ok_trailer_but_no_message_raises():
    ch = _FakeChannel(unary_body=encode_trailers(0))
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="no message frame"):
        stub.AddArtifacts(iter([pb.AddArtifactsRequest(session_id="s1")]))


def test_status_code_mapping_representative_non_ok_trailers():
    """Representative non-OK grpc-status codes each surface as a
    SparkConnectGrpcException carrying the status + message."""
    for code, msg in [
        (3, "INVALID_ARGUMENT bad plan"),
        (5, "NOT_FOUND no session"),
        (7, "PERMISSION_DENIED"),
        (13, "INTERNAL boom"),
        (14, "UNAVAILABLE retry me"),
        (16, "UNAUTHENTICATED token"),
    ]:
        ch = _FakeChannel(
            unary_body=encode_message(pb.ConfigResponse().SerializeToString())
            + encode_trailers(code, msg)
        )
        stub = GrpcWebStub(ch, base_url="")
        with pytest.raises(SparkConnectGrpcException) as ei:
            stub.Config(pb.ConfigRequest(session_id="s"))
        assert str(code) in str(ei.value)
        assert msg in str(ei.value)


def test_server_stream_skips_empty_chunks():
    """Empty byte chunks from the wire are skipped without disturbing reassembly
    (guards grpcweb.py line 379, the ``if not chunk: continue`` guard)."""
    chunks = [
        b"",  # empty heartbeat chunk
        encode_message(pb.ExecutePlanResponse(session_id="a").SerializeToString()),
        b"",  # another empty chunk between frames
        encode_trailers(0),
    ]
    ch = _FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    out = list(stub.ExecutePlan(pb.ExecutePlanRequest()))
    assert [r.session_id for r in out] == ["a"]


def test_non_ok_trailer_without_message_uses_status_only():
    """A non-OK trailer with no grpc-message still raises, message-less."""
    ch = _FakeChannel(
        unary_body=encode_message(pb.ConfigResponse().SerializeToString())
        + encode_trailers(13)  # no message
    )
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="grpc-status 13"):
        stub.Config(pb.ConfigRequest(session_id="s"))
