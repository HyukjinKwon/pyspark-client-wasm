# SPDX-License-Identifier: Apache-2.0
"""Unit tests for grpc-web framing (lane 1). No network, no grpcio, no browser."""
from __future__ import annotations

import struct

import pytest

from pyspark_connect_web.transport import framing
from pyspark_connect_web.transport.framing import (
    HEADER_LEN,
    TRAILER_FLAG,
    Frame,
    encode_message,
    encode_trailers,
    iter_frames,
    parse_trailers,
    split_messages_and_trailer,
)


def test_encode_message_layout():
    out = encode_message(b"hello")
    # [flags=0][len=5 big-endian][payload]
    assert out[0] == 0
    assert struct.unpack(">I", out[1:5])[0] == 5
    assert out[5:] == b"hello"
    assert len(out) == HEADER_LEN + 5


def test_encode_empty_message():
    out = encode_message(b"")
    assert out == b"\x00\x00\x00\x00\x00"
    frames = list(iter_frames(out))
    assert frames == [Frame(0, b"")]


def test_encode_message_rejects_non_bytes():
    with pytest.raises(TypeError):
        encode_message("not bytes")  # type: ignore[arg-type]


def test_round_trip_single():
    payload = b"\x01\x02\x03\xff"
    (frame,) = list(iter_frames(encode_message(payload)))
    assert frame.flags == 0
    assert frame.payload == payload
    assert not frame.is_trailer
    assert not frame.is_compressed


def test_round_trip_multiple_concatenated():
    payloads = [b"a", b"", b"bb", b"ccc"]
    buf = b"".join(encode_message(p) for p in payloads)
    got = [f.payload for f in iter_frames(buf)]
    assert got == payloads


def test_trailer_flag_detection():
    buf = encode_trailers(0)
    (frame,) = list(iter_frames(buf))
    assert frame.is_trailer
    assert frame.flags & TRAILER_FLAG


def test_parse_trailers_ok():
    body = b"grpc-status:0\r\n"
    assert parse_trailers(body) == {"grpc-status": "0"}


def test_parse_trailers_with_message_and_spaces():
    body = b"grpc-status: 13\r\ngrpc-message: boom went the server\r\n"
    parsed = parse_trailers(body)
    assert parsed["grpc-status"] == "13"
    assert parsed["grpc-message"] == "boom went the server"


def test_parse_trailers_lowercases_keys():
    body = b"Grpc-Status:5\r\nGrpc-Message:nope\r\n"
    parsed = parse_trailers(body)
    assert parsed == {"grpc-status": "5", "grpc-message": "nope"}


def test_parse_trailers_tolerates_bare_newlines_and_blanks():
    body = b"grpc-status:0\n\ngrpc-message:fine\n"
    parsed = parse_trailers(body)
    assert parsed == {"grpc-status": "0", "grpc-message": "fine"}


def test_parse_trailers_ignores_garbage_lines():
    body = b"not a header line\r\ngrpc-status:0\r\n"
    assert parse_trailers(body) == {"grpc-status": "0"}


def test_encode_trailers_round_trips_through_parse():
    buf = encode_trailers(7, "something broke")
    (frame,) = list(iter_frames(buf))
    assert frame.is_trailer
    assert parse_trailers(frame.payload) == {
        "grpc-status": "7",
        "grpc-message": "something broke",
    }


def test_full_body_messages_then_trailer():
    body = (
        encode_message(b"msg1")
        + encode_message(b"msg2")
        + encode_trailers(0)
    )
    messages, trailers = split_messages_and_trailer(body)
    assert messages == [b"msg1", b"msg2"]
    assert trailers == {"grpc-status": "0"}


def test_split_no_trailer_returns_none():
    body = encode_message(b"only-data")
    messages, trailers = split_messages_and_trailer(body)
    assert messages == [b"only-data"]
    assert trailers is None


def test_iter_frames_truncated_header_raises():
    # 3 bytes is less than the 5-byte header.
    with pytest.raises(ValueError, match="truncated grpc-web frame header"):
        list(iter_frames(b"\x00\x00\x00"))


def test_iter_frames_truncated_body_raises():
    # declares length 10 but only provides 2 payload bytes
    buf = struct.pack(">BI", 0, 10) + b"ab"
    with pytest.raises(ValueError, match="truncated grpc-web frame body"):
        list(iter_frames(buf))


def test_split_rejects_compressed_frame():
    compressed = struct.pack(">BI", framing.COMPRESSED_FLAG, 1) + b"x"
    with pytest.raises(ValueError, match="compressed"):
        split_messages_and_trailer(compressed)


def test_large_length_big_endian():
    # 70000 bytes payload to confirm 4-byte big-endian length works past 16 bits
    payload = b"\xab" * 70000
    (frame,) = list(iter_frames(encode_message(payload)))
    assert frame.payload == payload
    assert len(frame.payload) == 70000
