# SPDX-License-Identifier: Apache-2.0
"""grpc-web wire framing.

A grpc-web message is a sequence of length-prefixed frames::

    [1 byte flags][4 byte big-endian length][payload ...]

The low bit of ``flags`` (``0x01``) marks a compressed payload (we never emit
compressed frames and reject them on decode). The high bit (``0x80``) marks the
**trailer** frame, whose payload is an HTTP/1-style header block::

    grpc-status: 0\r\ngrpc-message: ...\r\n

The trailer is how grpc-web carries the gRPC status out-of-band from the HTTP
status — a request can be HTTP 200 yet carry ``grpc-status: 13``. Lane 1 decodes
both data frames and the trailer; the stub (``grpcweb.py``) turns a non-OK
trailer into ``SparkConnectGrpcException``.

References: grpc-web protocol
https://github.com/grpc/grpc-web/blob/master/doc/web-spec.md
"""
from __future__ import annotations

import struct
from typing import Dict, Iterator, NamedTuple, Tuple

# Flag bits in the first byte of a frame header.
COMPRESSED_FLAG = 0x01  # payload is compressed (unsupported here)
TRAILER_FLAG = 0x80  # this frame is the trailer block, not a message

_HEADER = struct.Struct(">BI")  # 1-byte flags + 4-byte big-endian length
HEADER_LEN = _HEADER.size  # == 5


class Frame(NamedTuple):
    """A decoded grpc-web frame: its flag byte and raw payload bytes."""

    flags: int
    payload: bytes

    @property
    def is_trailer(self) -> bool:
        return bool(self.flags & TRAILER_FLAG)

    @property
    def is_compressed(self) -> bool:
        return bool(self.flags & COMPRESSED_FLAG)


def encode_message(payload: bytes, *, flags: int = 0) -> bytes:
    """Frame a single (already-serialized) message as a grpc-web data frame.

    ``flags`` defaults to 0 (uncompressed data frame). Callers that need a
    trailer frame pass ``flags=TRAILER_FLAG`` with an encoded trailer body.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError(f"payload must be bytes, got {type(payload).__name__}")
    return _HEADER.pack(flags & 0xFF, len(payload)) + bytes(payload)


def iter_frames(data: bytes) -> Iterator[Frame]:
    """Yield successive :class:`Frame` from a concatenated grpc-web byte buffer.

    Works for both a complete response body and a single streamed chunk that
    happens to contain whole frames. A trailing partial frame (header or body
    incomplete) raises ``ValueError`` rather than silently dropping bytes — the
    stub layer is responsible for buffering partial chunks before calling this.
    """
    mv = memoryview(data)
    n = len(mv)
    off = 0
    while off < n:
        if off + HEADER_LEN > n:
            raise ValueError(
                f"truncated grpc-web frame header at offset {off}: "
                f"{n - off} byte(s) left, need {HEADER_LEN}"
            )
        flags, length = _HEADER.unpack_from(mv, off)
        start = off + HEADER_LEN
        end = start + length
        if end > n:
            raise ValueError(
                f"truncated grpc-web frame body at offset {off}: "
                f"declared {length} byte(s), only {n - start} available"
            )
        yield Frame(flags, bytes(mv[start:end]))
        off = end


def encode_trailers(status: int, message: str = "") -> bytes:
    """Build a trailer **frame** (header + body) for a given grpc-status.

    Mainly used by tests/fakes that synthesize server responses; the real
    server produces these. Body uses ``\r\n``-separated lowercase header lines,
    matching grpc-web. ``grpc-message`` is percent-style left verbatim here
    (callers rarely need escaping for ASCII messages).
    """
    lines = [f"grpc-status:{status}"]
    if message:
        lines.append(f"grpc-message:{message}")
    body = ("\r\n".join(lines) + "\r\n").encode("utf-8")
    return encode_message(body, flags=TRAILER_FLAG)


def parse_trailers(payload: bytes) -> Dict[str, str]:
    """Parse a trailer frame *payload* into a lowercased ``{key: value}`` dict.

    Accepts the standard ``\r\n``-separated block; tolerates bare ``\n`` and
    trailing blank lines. Keys are lowercased and stripped; values are stripped
    of surrounding whitespace. Lines without a ``:`` are ignored.
    """
    text = payload.decode("utf-8", errors="replace")
    out: Dict[str, str] = {}
    for raw_line in text.replace("\r\n", "\n").split("\n"):
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        out[key.strip().lower()] = value.strip()
    return out


def split_messages_and_trailer(
    data: bytes,
) -> Tuple[list[bytes], Dict[str, str] | None]:
    """Convenience: split a full response body into data payloads + trailers.

    Returns ``(message_payloads, trailers_or_None)``. Compressed frames raise
    ``ValueError`` (we never negotiate compression). If no trailer frame is
    present (e.g. a single streamed chunk), ``trailers`` is ``None``.
    """
    messages: list[bytes] = []
    trailers: Dict[str, str] | None = None
    for frame in iter_frames(data):
        if frame.is_compressed:
            raise ValueError("grpc-web compressed frames are not supported")
        if frame.is_trailer:
            trailers = parse_trailers(frame.payload)
        else:
            messages.append(frame.payload)
    return messages, trailers
