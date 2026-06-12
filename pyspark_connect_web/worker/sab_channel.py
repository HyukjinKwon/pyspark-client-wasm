# SPDX-License-Identifier: Apache-2.0
"""Blocking ``SyncChannel`` backed by SharedArrayBuffer + Atomics.

This module is the Python half of lane 3's bridge. It implements the
``SyncChannel`` protocol (see ``pyspark_connect_web/_contract.py``):

    class SyncChannel(Protocol):
        def unary(self, path, body, headers, timeout) -> HttpResponse: ...
        def server_stream(self, path, body, headers, timeout) -> Iterator[bytes]: ...

The whole point of this class is that ``unary``/``server_stream`` *block the
calling thread* until bytes arrive, so PySpark's synchronous ``.collect()`` keeps
working unchanged (DECISIONS.md #5). In the browser the calling thread is a Web
Worker; blocking it with ``Atomics.wait`` is fine and is the only place
``Atomics.wait`` is even allowed.

Two backends, selected at runtime:

* **Pyodide / browser** (``sys.platform == "emscripten"`` or the ``js`` module is
  importable): :class:`_AtomicsBackend` drives the SAB + ``Atomics.wait``
  handshake against ``bridge.js`` on the main thread. The wire layout is
  documented in ``team/findings-lane3-bridge.md``.

* **Local / tests** (anything else): a pluggable :class:`SyncBackend` you inject.
  ``tests/test_sab_channel.py`` injects a fake so the full API is exercisable
  with no browser, no ``grpcio``, and no network.

Both backends speak the same small synchronous protocol so the channel logic
(framing of the request, streaming chunk reassembly, timeout handling) is shared
and tested once.
"""
from __future__ import annotations

import sys
from typing import Iterator, Optional, Protocol, runtime_checkable

from pyspark_connect_web._contract import HttpResponse


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TransportError(RuntimeError):
    """A request failed at the transport (browser/fetch/SAB) layer.

    This is *not* a gRPC-status error — those are carried in the response body's
    trailer frame and are lane 1's concern. This is a hard transport failure
    (network error, main thread gone, SAB protocol violation, isolation missing).
    """


class TransportTimeout(TransportError):
    """The blocking wait exceeded the caller-supplied ``timeout`` (seconds)."""


# --------------------------------------------------------------------------- #
# Backend protocol — the seam between SabSyncChannel and "how bytes move"
# --------------------------------------------------------------------------- #
@runtime_checkable
class SyncBackend(Protocol):
    """A synchronous request->response(s) byte mover.

    The channel calls exactly one of these per RPC. Implementations MUST block
    until they have something to return; this is what makes ``.collect()`` block.

    ``request`` is an opaque dict the channel hands down:
        {
          "kind":    "unary" | "server_stream",
          "path":    str,                 # gRPC path
          "body":    bytes,               # already grpc-web framed by lane 1
          "headers": dict[str, str],
          "timeout": float | None,        # seconds
        }
    """

    def unary(self, request: dict) -> HttpResponse:
        """Block, then return the full response (status, headers, body bytes)."""
        ...

    def server_stream(self, request: dict) -> Iterator[bytes]:
        """Yield raw response chunks (grpc-web frame bytes) as they arrive.

        The generator MUST block between yields until the next chunk (or the
        end-of-stream signal) is available.
        """
        ...


# --------------------------------------------------------------------------- #
# Environment detection
# --------------------------------------------------------------------------- #
def is_pyodide() -> bool:
    """True when running inside Pyodide/Emscripten (i.e. the browser worker).

    Detection is twofold per the lane brief: the Emscripten platform tag, or the
    importability of the Pyodide-injected ``js`` module. Either is sufficient.
    """
    if sys.platform == "emscripten":
        return True
    try:
        import js  # noqa: F401  (Pyodide-injected host bridge)

        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# The channel
# --------------------------------------------------------------------------- #
class SabSyncChannel:
    """``SyncChannel`` implementation. Delegates byte movement to a backend.

    Parameters
    ----------
    base_url:
        Origin the gRPC ``path`` is resolved against, e.g.
        ``"https://envoy.example:8081"``. The backend turns ``base_url + path``
        into the actual fetch URL.
    backend:
        A :class:`SyncBackend`. If ``None``, one is auto-selected:
        :class:`_AtomicsBackend` under Pyodide, otherwise we raise — local
        callers (tests) MUST inject a fake, because there is no network here.
    sab:
        Optional pre-allocated control/data SharedArrayBuffer pair for the
        Atomics backend (``(control_sab, data_sab)``). Normally created by
        ``worker_bootstrap.js`` and passed in; exposed for advanced wiring.
    """

    def __init__(
        self,
        base_url: str,
        backend: Optional[SyncBackend] = None,
        *,
        sab: Optional[tuple] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        if backend is None:
            if is_pyodide():
                backend = _AtomicsBackend(self.base_url, sab=sab)
            else:
                raise TransportError(
                    "SabSyncChannel has no backend: not running under Pyodide and "
                    "no SyncBackend was injected. Local/test callers must pass a "
                    "backend (see tests/test_sab_channel.py)."
                )
        self._backend = backend

    # -- SyncChannel protocol ------------------------------------------------ #
    def unary(
        self,
        path: str,
        body: bytes,
        headers: dict[str, str],
        timeout: float | None,
    ) -> HttpResponse:
        request = self._request("unary", path, body, headers, timeout)
        resp = self._backend.unary(request)
        if not isinstance(resp, HttpResponse):
            raise TransportError(
                f"backend.unary returned {type(resp)!r}, expected HttpResponse"
            )
        return resp

    def server_stream(
        self,
        path: str,
        body: bytes,
        headers: dict[str, str],
        timeout: float | None,
    ) -> Iterator[bytes]:
        request = self._request("server_stream", path, body, headers, timeout)
        # Delegate straight to the backend generator. We intentionally do not
        # buffer: lane 1's reattachable iterator wants chunks as they arrive so a
        # mid-stream disconnect (DECISIONS.md #6) is observable promptly.
        return self._backend.server_stream(request)

    # -- internals ----------------------------------------------------------- #
    def _request(
        self,
        kind: str,
        path: str,
        body: bytes,
        headers: dict[str, str],
        timeout: float | None,
    ) -> dict:
        if not path.startswith("/"):
            raise TransportError(f"gRPC path must start with '/': {path!r}")
        return {
            "kind": kind,
            "url": self.base_url + path,
            "path": path,
            "body": bytes(body),
            "headers": dict(headers or {}),
            "timeout": timeout,
        }


# --------------------------------------------------------------------------- #
# Pyodide backend: the SharedArrayBuffer + Atomics.wait dance
# --------------------------------------------------------------------------- #
#
# SAB layout (mirrors bridge.js / worker_bootstrap.js — see
# team/findings-lane3-bridge.md for the authoritative spec). Two buffers:
#
#   control_sab : Int32Array, fixed small size. Indices:
#       [0] STATE   — handshake flag the worker Atomics.wait()s on
#       [1] LENGTH  — byte length of the current data-region payload
#       [2] STATUS  — HTTP status (unary) or stream sentinel
#       [3] SEQ     — monotonically increasing chunk sequence (streaming)
#       [4] GEN     — request generation (guards against stale wakeups)
#
#   data_sab    : Uint8Array, large (default 16 MiB). Carries, in order:
#       request out:  [u32 header_len][header json][u32 body_len][body bytes]
#       response in:  [u32 meta_len][meta json][payload bytes]
#
# STATE values (Atomics handshake):
#   0 IDLE          worker owns the buffer; may write a request
#   1 REQ_READY     worker -> main: request written, please fetch
#   2 RESP_CHUNK    main -> worker: a chunk/full response is in the data region
#   3 RESP_END      main -> worker: stream finished (no payload)
#   4 RESP_ERROR    main -> worker: transport error; meta json has {message}
#
# Handshake (unary):
#   worker: write request, Atomics.store(STATE, REQ_READY), Atomics.notify,
#           postMessage({}), then Atomics.wait(STATE, REQ_READY).
#   main:   fetch, write meta+body, store STATUS+LENGTH,
#           Atomics.store(STATE, RESP_CHUNK), Atomics.notify.
#   worker: wakes, reads, store(STATE, IDLE).
#
# Handshake (server stream): same start; main loops reader.read(), and for each
#   chunk writes payload, store(STATE, RESP_CHUNK), notify; worker copies the
#   chunk, stores STATE back to a "consumed" value (we reuse REQ_READY as the
#   "ready for next chunk" ack) and waits again. At EOS main stores RESP_END.
#
# This is the riskiest seam; the Python side below is written so the *protocol
# constants* are the single source of truth shared with the JS files by value.
# --------------------------------------------------------------------------- #

# Control array indices
_C_STATE = 0
_C_LENGTH = 1
_C_STATUS = 2
_C_SEQ = 3
_C_GEN = 4
_CONTROL_SLOTS = 8  # round up; leaves room for future fields

# STATE values
_S_IDLE = 0
_S_REQ_READY = 1
_S_RESP_CHUNK = 2
_S_RESP_END = 3
_S_RESP_ERROR = 4

# ack value the worker writes to request the *next* streaming chunk
_S_CHUNK_ACK = 5

_DEFAULT_DATA_BYTES = 16 * 1024 * 1024  # 16 MiB payload region


class _AtomicsBackend:
    """Pyodide-only backend. Imports ``js`` lazily so this module imports fine
    on CPython (where the import would fail) — keeping the file unit-testable.

    The heavy lifting (allocating SABs, performing fetch, framing the response)
    is split between this class and ``bridge.js`` / ``worker_bootstrap.js``. This
    class owns: writing the request into the data region, the ``Atomics.wait``
    blocking loop, and reassembling streamed chunks into the iterator lane 1
    consumes. ``bridge.js`` owns: fetch + writing responses back.
    """

    def __init__(self, base_url: str, *, sab: Optional[tuple] = None) -> None:
        # Imported here, not at module top, so CPython/test imports never hit it.
        import js  # noqa: F401
        import json

        self._js = js
        self._json = json
        self._base_url = base_url

        if not getattr(js, "crossOriginIsolated", False):
            # DECISIONS.md #4: SAB requires COOP/COEP. Fail loudly and early —
            # Atomics.wait on a non-shared buffer would either throw or, worse,
            # silently busy-spin. The demo asserts this too.
            raise TransportError(
                "crossOriginIsolated is false: the page must be served with "
                "COOP: same-origin and COEP: require-corp for SharedArrayBuffer. "
                "See DECISIONS.md #4."
            )

        # worker_bootstrap.js normally allocates and hands these in. Allocate
        # defensively if not provided (e.g. a bare worker without bootstrap).
        if sab is not None:
            control_sab, data_sab = sab
        else:
            control_sab = js.SharedArrayBuffer.new(_CONTROL_SLOTS * 4)
            data_sab = js.SharedArrayBuffer.new(_DEFAULT_DATA_BYTES)

        self._control_sab = control_sab
        self._data_sab = data_sab
        self._ctrl = js.Int32Array.new(control_sab)
        self._data = js.Uint8Array.new(data_sab)
        self._gen = 0

        # Hand the SABs to the main thread once so bridge.js can attach views.
        # worker_bootstrap.js wires postMessage; we just announce them.
        if hasattr(js, "__pcw_register_sab"):
            js.__pcw_register_sab(control_sab, data_sab)

    # -- request marshalling ------------------------------------------------- #
    def _write_request(self, request: dict) -> None:
        Atomics = self._js.Atomics
        header = {
            "kind": request["kind"],
            "url": request["url"],
            "headers": request["headers"],
            "timeout": request["timeout"],
            "gen": self._gen,
        }
        header_bytes = self._json.dumps(header).encode("utf-8")
        body = request["body"]

        off = 0
        off = self._put_u32(off, len(header_bytes))
        off = self._put_bytes(off, header_bytes)
        off = self._put_u32(off, len(body))
        off = self._put_bytes(off, body)

        Atomics.store(self._ctrl, _C_GEN, self._gen)
        Atomics.store(self._ctrl, _C_LENGTH, off)
        Atomics.store(self._ctrl, _C_SEQ, 0)
        Atomics.store(self._ctrl, _C_STATE, _S_REQ_READY)
        Atomics.notify(self._ctrl, _C_STATE)
        # Nudge the main thread (it cannot Atomics.wait): a 0-length message is
        # enough; bridge.js reads everything from the SAB.
        self._js.postMessage({"type": "pcw_rpc"})

    def _put_u32(self, off: int, value: int) -> int:
        self._data[off + 0] = value & 0xFF
        self._data[off + 1] = (value >> 8) & 0xFF
        self._data[off + 2] = (value >> 16) & 0xFF
        self._data[off + 3] = (value >> 24) & 0xFF
        return off + 4

    def _get_u32(self, off: int) -> tuple[int, int]:
        v = (
            self._data[off + 0]
            | (self._data[off + 1] << 8)
            | (self._data[off + 2] << 16)
            | (self._data[off + 3] << 24)
        )
        return v, off + 4

    def _put_bytes(self, off: int, b: bytes) -> int:
        self._data.set(self._js.Uint8Array.new(list(b)), off)
        return off + len(b)

    def _read_payload(self) -> bytes:
        length = self._js.Atomics.load(self._ctrl, _C_LENGTH)
        # Read meta_len + meta json + payload from the data region.
        meta_len, off = self._get_u32(0)
        # meta json currently unused by the channel beyond status; skip it.
        off += meta_len
        payload = bytes(self._data.subarray(off, off + (length - off)).to_py())
        return payload

    # -- blocking wait ------------------------------------------------------- #
    def _wait(self, expect_from: int, timeout: float | None) -> int:
        """Block until STATE moves away from ``expect_from``; return new STATE.

        ``timeout`` is in seconds; Atomics.wait wants milliseconds. ``"timed-out"``
        from Atomics.wait raises :class:`TransportTimeout`.
        """
        Atomics = self._js.Atomics
        ms = float("inf") if timeout is None else max(0.0, timeout) * 1000.0
        while True:
            state = Atomics.load(self._ctrl, _C_STATE)
            if state != expect_from:
                return state
            res = Atomics.wait(self._ctrl, _C_STATE, expect_from, ms)
            if res == "timed-out":
                raise TransportTimeout(
                    f"blocking transport timed out after {timeout}s"
                )
            # "ok" / "not-equal": loop re-checks STATE.

    # -- SyncBackend --------------------------------------------------------- #
    def unary(self, request: dict) -> HttpResponse:
        self._gen += 1
        self._write_request(request)
        state = self._wait(_S_REQ_READY, request.get("timeout"))
        try:
            if state == _S_RESP_ERROR:
                raise TransportError(self._read_error())
            if state not in (_S_RESP_CHUNK, _S_RESP_END):
                raise TransportError(f"unexpected SAB state {state} for unary")
            status = self._js.Atomics.load(self._ctrl, _C_STATUS)
            body = self._read_payload() if state == _S_RESP_CHUNK else b""
            return HttpResponse(status=status, headers={}, body=body)
        finally:
            self._js.Atomics.store(self._ctrl, _C_STATE, _S_IDLE)
            self._js.Atomics.notify(self._ctrl, _C_STATE)

    def server_stream(self, request: dict) -> Iterator[bytes]:
        self._gen += 1
        timeout = request.get("timeout")
        self._write_request(request)
        Atomics = self._js.Atomics
        # The worker and main thread ping-pong STATE. After the request the
        # worker is parked on REQ_READY; for every subsequent chunk it parks on
        # CHUNK_ACK (the value it itself wrote to request the next chunk). The
        # state bridge.js moves us *off of* therefore alternates.
        wait_on = _S_REQ_READY
        try:
            while True:
                state = self._wait(wait_on, timeout)
                if state == _S_RESP_END:
                    return
                if state == _S_RESP_ERROR:
                    raise TransportError(self._read_error())
                if state != _S_RESP_CHUNK:
                    raise TransportError(f"unexpected SAB state {state} in stream")
                chunk = self._read_payload()
                yield chunk
                # Ack: request the next chunk, then park on CHUNK_ACK.
                Atomics.store(self._ctrl, _C_STATE, _S_CHUNK_ACK)
                Atomics.notify(self._ctrl, _C_STATE)
                wait_on = _S_CHUNK_ACK
                timeout = request.get("timeout")
        finally:
            Atomics.store(self._ctrl, _C_STATE, _S_IDLE)
            Atomics.notify(self._ctrl, _C_STATE)

    def _read_error(self) -> str:
        try:
            length = self._js.Atomics.load(self._ctrl, _C_LENGTH)
            meta_len, off = self._get_u32(0)
            meta = bytes(self._data.subarray(off, off + meta_len).to_py())
            obj = self._json.loads(meta.decode("utf-8"))
            return str(obj.get("message", "transport error"))
        except Exception:
            return "transport error (and failed to read error detail from SAB)"


# Convenience for callers that want a no-arg factory in the worker.
def make_channel(base_url: str, **kw) -> SabSyncChannel:
    """Build a :class:`SabSyncChannel` for ``base_url`` (Pyodide auto-backend)."""
    return SabSyncChannel(base_url, **kw)


__all__ = [
    "SabSyncChannel",
    "SyncBackend",
    "TransportError",
    "TransportTimeout",
    "is_pyodide",
    "make_channel",
]
