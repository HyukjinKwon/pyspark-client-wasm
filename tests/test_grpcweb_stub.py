# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the grpc-web stub (lane 1).

Uses a FAKE in-memory SyncChannel that returns canned framed bytes — no
network, no grpcio, no browser. Real pyspark protos are used when pyspark is
installed; otherwise proto-dependent tests skip gracefully (framing is covered
standalone in test_transport_framing.py).
"""
from __future__ import annotations

import struct
from typing import Dict, Iterator, List, Optional, Tuple

import pytest

from pyspark_connect_web._contract import HttpResponse, grpc_path
from pyspark_connect_web.transport.framing import encode_message, encode_trailers

# --- guard DECISIONS.md #1: never import grpcio, even transitively here ------
pytest.importorskip  # noqa: B018  (kept for clarity; real skips are below)

try:
    from pyspark.sql.connect import proto as pb  # type: ignore
    from pyspark_connect_web.transport.grpcweb import (
        GrpcWebStub,
        SparkConnectGrpcException,
    )

    HAVE_PYSPARK = True
except Exception:  # pragma: no cover - environment without pyspark
    HAVE_PYSPARK = False

pytestmark = pytest.mark.skipif(
    not HAVE_PYSPARK, reason="pyspark not installed; proto-dependent stub tests skipped"
)


# --------------------------------------------------------------------------
# Fake SyncChannel
# --------------------------------------------------------------------------
class FakeChannel:
    """In-memory SyncChannel. Records calls; returns pre-seeded bytes.

    Satisfies the lane-3 contract: ``unary`` returns an ``HttpResponse`` with a
    full grpc-web body; ``server_stream`` yields raw frame-byte chunks.
    """

    def __init__(
        self,
        *,
        unary_body: bytes = b"",
        unary_status: int = 200,
        unary_headers: Optional[Dict[str, str]] = None,
        stream_chunks: Optional[List[bytes]] = None,
    ) -> None:
        self.unary_body = unary_body
        self.unary_status = unary_status
        self.unary_headers = unary_headers or {}
        self.stream_chunks = stream_chunks or []
        self.calls: List[Tuple[str, str, bytes, Dict[str, str], Optional[float]]] = []

    def unary(self, path, body, headers, timeout) -> HttpResponse:
        self.calls.append(("unary", path, body, headers, timeout))
        return HttpResponse(
            status=self.unary_status, headers=dict(self.unary_headers), body=self.unary_body
        )

    def server_stream(self, path, body, headers, timeout) -> Iterator[bytes]:
        self.calls.append(("server_stream", path, body, headers, timeout))
        yield from self.stream_chunks


# --------------------------------------------------------------------------
# Helpers to build canned server responses from real protos
# --------------------------------------------------------------------------
def _config_response_body() -> bytes:
    resp = pb.ConfigResponse(session_id="sess-123")
    return encode_message(resp.SerializeToString()) + encode_trailers(0)


def _execute_response(idx_session: str) -> "pb.ExecutePlanResponse":
    return pb.ExecutePlanResponse(session_id=idx_session)


# --------------------------------------------------------------------------
# Header / metadata mapping
# --------------------------------------------------------------------------
def test_required_grpcweb_headers_present():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="https://envoy.example")
    stub.Config(pb.ConfigRequest(session_id="s"))
    _, path, _, headers, _ = ch.calls[0]
    assert path == grpc_path("Config")
    assert headers["content-type"] == "application/grpc-web+proto"
    assert headers["x-grpc-web"] == "1"


def test_metadata_mapped_to_headers():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    stub.Config(
        pb.ConfigRequest(session_id="s"),
        metadata=[("x-databricks-token", "abc"), ("Authorization", "Bearer xyz")],
    )
    headers = ch.calls[0][3]
    assert headers["x-databricks-token"] == "abc"
    # keys lowercased like HTTP headers
    assert headers["authorization"] == "Bearer xyz"


def test_metadata_cannot_override_required_markers():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    stub.Config(
        pb.ConfigRequest(session_id="s"),
        metadata=[("content-type", "text/plain"), ("x-grpc-web", "0")],
    )
    headers = ch.calls[0][3]
    assert headers["content-type"] == "application/grpc-web+proto"
    assert headers["x-grpc-web"] == "1"


def test_default_metadata_applied():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="", default_metadata=[("user-agent", "pcw/0")])
    stub.Config(pb.ConfigRequest(session_id="s"))
    assert ch.calls[0][3]["user-agent"] == "pcw/0"


def test_lane2_construction_shape_metadata_kw():
    """GUARD (lane1<->lane2 seam): lane 2's patch._default_stub_factory builds
    us as ``GrpcWebStub(sync_channel, metadata=[...])``. That exact call must
    apply the channel-level metadata to every request's headers."""
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, metadata=[("authorization", "Bearer t0ken")])
    stub.Config(pb.ConfigRequest(session_id="s"))
    assert ch.calls[0][3]["authorization"] == "Bearer t0ken"


def test_per_call_metadata_extends_channel_metadata():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, metadata=[("authorization", "Bearer t0ken")])
    stub.Config(
        pb.ConfigRequest(session_id="s"), metadata=[("x-request-id", "r1")]
    )
    headers = ch.calls[0][3]
    assert headers["authorization"] == "Bearer t0ken"
    assert headers["x-request-id"] == "r1"


def test_timeout_passed_through():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    stub.Config(pb.ConfigRequest(session_id="s"), timeout=12.5)
    assert ch.calls[0][4] == 12.5


# --------------------------------------------------------------------------
# Unary round-trips
# --------------------------------------------------------------------------
def test_unary_round_trip_returns_proto():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    resp = stub.Config(pb.ConfigRequest(session_id="s"))
    assert isinstance(resp, pb.ConfigResponse)
    assert resp.session_id == "sess-123"


def test_unary_serializes_request_into_frame():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    req = pb.ConfigRequest(session_id="my-session")
    stub.Config(req)
    body = ch.calls[0][2]
    # flags(0) + len + serialized request
    assert body[0] == 0
    length = struct.unpack(">I", body[1:5])[0]
    assert body[5:] == req.SerializeToString()
    assert length == len(req.SerializeToString())


def test_all_ten_methods_exist_and_are_callable():
    ch = FakeChannel()
    stub = GrpcWebStub(ch, base_url="")
    for name in [
        "ExecutePlan",
        "ReattachExecute",
        "ReleaseExecute",
        "AnalyzePlan",
        "Config",
        "Interrupt",
        "AddArtifacts",
        "ArtifactStatus",
        "ReleaseSession",
        "FetchErrorDetails",
    ]:
        assert callable(getattr(stub, name)), name


# --------------------------------------------------------------------------
# Error / trailer handling
# --------------------------------------------------------------------------
def test_non_ok_trailer_raises_grpc_exception():
    body = encode_trailers(13, "internal error from server")
    ch = FakeChannel(unary_body=body)
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException) as ei:
        stub.Config(pb.ConfigRequest(session_id="s"))
    assert "13" in str(ei.value)
    assert "internal error from server" in str(ei.value)


def test_missing_trailer_raises():
    # data frame but no trailer at all -> truncated stream
    body = encode_message(pb.ConfigResponse().SerializeToString())
    ch = FakeChannel(unary_body=body)
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="no trailer"):
        stub.Config(pb.ConfigRequest(session_id="s"))


def test_grpc_status_in_http_headers_fallback():
    # empty body, status carried in HTTP headers (some grpc-web deployments)
    ch = FakeChannel(
        unary_body=b"",
        unary_headers={"grpc-status": "7", "grpc-message": "permission denied"},
    )
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(SparkConnectGrpcException, match="permission denied"):
        stub.Config(pb.ConfigRequest(session_id="s"))


# --------------------------------------------------------------------------
# Server streaming (ExecutePlan / ReattachExecute)
# --------------------------------------------------------------------------
def test_server_stream_yields_protos_in_order():
    chunks = [
        encode_message(_execute_response("a").SerializeToString()),
        encode_message(_execute_response("b").SerializeToString()),
        encode_trailers(0),
    ]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    out = list(stub.ExecutePlan(pb.ExecutePlanRequest()))
    assert [r.session_id for r in out] == ["a", "b"]
    assert all(isinstance(r, pb.ExecutePlanResponse) for r in out)


def test_server_stream_reassembles_frames_split_across_chunks():
    """GUARD (SPARK-53525-style): a frame split across arbitrary chunk
    boundaries must still decode. Regression guard for the buffer-reassembly
    bug where a generator held a stale offset into a mutating buffer."""
    full = (
        encode_message(_execute_response("x").SerializeToString())
        + encode_message(_execute_response("y").SerializeToString())
        + encode_trailers(0)
    )
    # Slice the whole byte stream into awkward 3-byte chunks.
    chunks = [full[i : i + 3] for i in range(0, len(full), 3)]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    out = list(stub.ReattachExecute(pb.ReattachExecuteRequest()))
    assert [r.session_id for r in out] == ["x", "y"]


def test_server_stream_single_byte_chunks():
    """Hardest reassembly case: one byte at a time."""
    full = (
        encode_message(_execute_response("solo").SerializeToString())
        + encode_trailers(0)
    )
    chunks = [full[i : i + 1] for i in range(len(full))]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    out = list(stub.ExecutePlan(pb.ExecutePlanRequest()))
    assert [r.session_id for r in out] == ["solo"]


def test_server_stream_error_trailer_raises_after_messages():
    chunks = [
        encode_message(_execute_response("ok1").SerializeToString()),
        encode_trailers(14, "UNAVAILABLE: server going away"),
    ]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    it = stub.ExecutePlan(pb.ExecutePlanRequest())
    assert next(it).session_id == "ok1"  # message delivered before error
    with pytest.raises(SparkConnectGrpcException, match="UNAVAILABLE"):
        next(it)


def test_server_stream_dropped_without_trailer_raises():
    """GUARD DECISIONS.md #6: a stream that ends with no trailer is a dropped
    connection — must surface as an error so the client reattaches, not silently
    end."""
    chunks = [encode_message(_execute_response("partial").SerializeToString())]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    it = stub.ExecutePlan(pb.ExecutePlanRequest())
    assert next(it).session_id == "partial"
    with pytest.raises(SparkConnectGrpcException, match="without a trailer"):
        next(it)


def test_server_stream_trailing_partial_frame_raises():
    full = encode_message(_execute_response("z").SerializeToString())
    # append a 2-byte partial header that never completes, and no trailer
    chunks = [full + b"\x00\x00"]
    ch = FakeChannel(stream_chunks=chunks)
    stub = GrpcWebStub(ch, base_url="")
    it = stub.ExecutePlan(pb.ExecutePlanRequest())
    assert next(it).session_id == "z"
    with pytest.raises(SparkConnectGrpcException, match="trailing"):
        next(it)


# --------------------------------------------------------------------------
# Client streaming (AddArtifacts) — lowered to a single unary POST
# --------------------------------------------------------------------------
def test_client_stream_concatenates_requests():
    resp = pb.AddArtifactsResponse()
    body = encode_message(resp.SerializeToString()) + encode_trailers(0)
    ch = FakeChannel(unary_body=body)
    stub = GrpcWebStub(ch, base_url="")
    reqs = [
        pb.AddArtifactsRequest(session_id="s1"),
        pb.AddArtifactsRequest(session_id="s2"),
    ]
    out = stub.AddArtifacts(iter(reqs))
    assert isinstance(out, pb.AddArtifactsResponse)
    # both requests framed into one body
    sent = ch.calls[0][2]
    expected = b"".join(encode_message(r.SerializeToString()) for r in reqs)
    assert sent == expected


# --------------------------------------------------------------------------
# Calling convention: keyword-only metadata/timeout
# --------------------------------------------------------------------------
def test_lane2_default_stub_factory_builds_real_stub():
    """GUARD (lane1<->lane2 seam, end-to-end): drive lane 2's real
    ``_default_stub_factory`` with a fake SyncChannel and confirm it produces a
    working GrpcWebStub whose Config call round-trips. This catches constructor-
    signature drift between the lanes (it broke once: metadata= vs base_url)."""
    patch = pytest.importorskip("pyspark_connect_web.patch")
    ch = FakeChannel(unary_body=_config_response_body())
    web_channel = patch.WebChannel(
        host="h", port=8081, secure=False, channel=ch, params={"user-agent": "pcw/0"}
    )
    stub = patch._default_stub_factory(web_channel)
    resp = stub.Config(pb.ConfigRequest(session_id="s"))
    assert resp.session_id == "sess-123"
    # channel-level params surfaced as headers
    assert ch.calls[0][3]["user-agent"] == "pcw/0"


def test_metadata_and_timeout_are_keyword_only():
    ch = FakeChannel(unary_body=_config_response_body())
    stub = GrpcWebStub(ch, base_url="")
    with pytest.raises(TypeError):
        # positional metadata must be rejected (gRPC-Python convention)
        stub.Config(pb.ConfigRequest(session_id="s"), [("k", "v")])  # type: ignore
