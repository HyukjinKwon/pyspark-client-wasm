# SPDX-License-Identifier: Apache-2.0
"""Blocking ``SyncChannel`` backed by SharedArrayBuffer + Atomics.

This module is the Python half of the bridge. It implements the
``SyncChannel`` protocol (see ``pyspark_connect_web/_contract.py``):

    class SyncChannel(Protocol):
        def unary(self, path, body, headers, timeout) -> HttpResponse: ...
        def server_stream(self, path, body, headers, timeout) -> Iterator[bytes]: ...

The whole point of this class is that ``unary``/``server_stream`` *block the
calling thread* until bytes arrive, so PySpark's synchronous ``.collect()`` keeps
working unchanged. In the browser the calling thread is a Web
Worker; blocking it with ``Atomics.wait`` is fine and is the only place
``Atomics.wait`` is even allowed.

Two backends, selected at runtime:

* **Pyodide / browser** (``sys.platform == "emscripten"`` or the ``js`` module is
  importable): :class:`_AtomicsBackend` drives the SAB + ``Atomics.wait``
  handshake against ``bridge.js`` on the main thread. The wire layout is
  documented in ``the project notes``.

* **Local / tests** (anything else): a pluggable :class:`SyncBackend` you inject.
  ``tests/test_sab_channel.py`` injects a fake so the full API is exercisable
  with no browser, no ``grpcio``, and no network.

Both backends speak the same small synchronous protocol so the channel logic
(framing of the request, streaming chunk reassembly, timeout handling) is shared
and tested once.

Large results / dynamic buffer sizing
-------------------------------------
The data SAB has a fixed capacity (default 16 MiB) and cannot be resized
in place (``SharedArrayBuffer.grow`` requires a growable buffer + re-sharing,
which is racy across an already-blocked worker). Instead the response side uses
**bounded-window transfer**: the main thread writes ``min(remaining, capacity)``
bytes per ``RESP_CHUNK``, marks ``meta.more`` while bytes remain, and the worker
acknowledges each window (the existing CHUNK_ACK ping-pong) and reassembles. A
single logical payload - a unary body, or one server-stream chunk - that exceeds
the data region is therefore delivered across several windows with no realloc.

A *realloc* negotiation is also supported for the rare case where the host wants
to enlarge the buffer for throughput (e.g. a known-huge result): the worker can
allocate a larger data SAB and hand it to the main thread via the
``__pcw_register_sab`` hook before the next RPC. The windowing path is the
correctness guarantee; realloc is a throughput optimisation layered on top. See
``_reassemble_windows`` / ``_grow_data_sab``.
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

    This is *not* a gRPC-status error - those are carried in the response body's
    trailer frame and are the concern. This is a hard transport failure
    (network error, main thread gone, SAB protocol violation, isolation missing).

    Lane 1 (``GrpcWebStub``) lets this propagate; PySpark surfaces it as the
    cause of a failed ``.collect()``. For HTTP-level failures we still hand the components a valid :class:`HttpResponse` (with the non-200 ``status`` and any
    ``grpc-status`` headers) so it can raise the *correct*
    ``SparkConnectGrpcException`` rather than an opaque transport error - see
    :meth:`_AtomicsBackend.unary`.
    """


class TransportTimeout(TransportError):
    """The blocking wait exceeded the caller-supplied ``timeout`` (seconds)."""


class TransportAborted(TransportError):
    """The fetch was aborted (client abort / navigation / explicit cancel)."""


# --------------------------------------------------------------------------- #
# Backend protocol - the seam between SabSyncChannel and "how bytes move"
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
          "body":    bytes,               # already grpc-web framed by the "headers": dict[str, str],
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
        :class:`_AtomicsBackend` under Pyodide, otherwise we raise - local
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
        # buffer: the reattachable iterator wants chunks as they arrive so a
        # mid-stream disconnect is observable promptly.
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
# Protocol constants (single source of truth, mirrored by value in bridge.js
# and worker_bootstrap.js - see the project notes).
# --------------------------------------------------------------------------- #
# Control array indices
_C_STATE = 0
_C_LENGTH = 1
_C_STATUS = 2
_C_SEQ = 3
_C_GEN = 4
_C_CAP = 5  # current data-SAB capacity in bytes (set by worker after alloc)
_CONTROL_SLOTS = 8  # round up; leaves room for future fields

# STATE values
_S_IDLE = 0
_S_REQ_READY = 1
_S_RESP_CHUNK = 2
_S_RESP_END = 3
_S_RESP_ERROR = 4
# ack value the worker writes to request the *next* window/chunk
_S_CHUNK_ACK = 5
# main -> worker: the next logical payload needs more capacity than we have; the
# worker may grow the data SAB and re-announce it, then ack. (Optional path; the
# windowing path below works without it.)
_S_REALLOC_REQ = 6

_DEFAULT_DATA_BYTES = 16 * 1024 * 1024  # 16 MiB payload region

# Reserve a fixed header zone at the front of the data region for the response
# meta JSON, so a window's *payload* capacity is deterministic regardless of how
# large the meta happens to be. 4 KiB is ample for ``{"more":..,"status":..}``.
_META_ZONE = 4096


class _AtomicsBackend:
    """Pyodide-only backend. Imports ``js`` lazily so this module imports fine
    on CPython (where the import would fail) - keeping the file unit-testable.

    The heavy lifting (allocating SABs, performing fetch, framing the response)
    is split between this class and ``bridge.js`` / ``worker_bootstrap.js``. This
    class owns: writing the request into the data region, the ``Atomics.wait``
    blocking loop, **reassembling windowed payloads**, and turning streamed
    chunks into the iterator the consumes. ``bridge.js`` owns: fetch + writing
    response windows back.
    """

    def __init__(
        self,
        base_url: str,
        *,
        sab: Optional[tuple] = None,
        transport: str = "auto",
    ) -> None:
        # Imported here, not at module top, so CPython/test imports never hit it.
        import js  # noqa: F401
        import json

        self._js = js
        self._json = json
        self._base_url = base_url
        # How we announce the SAB to the main thread and nudge it per RPC:
        #   "standalone": worker_bootstrap.js harness - js.__pcw_register_sab +
        #                 postMessage({type:"pcw_rpc"}).
        #   "kernel":     JupyterLite pyodide kernel worker - post a namespaced
        #                 envelope postMessage({__pcw__:{type:"sab"|"rpc",...}})
        #                 so the kernel's comlink/coincident framing ignores it
        #                 and pcw_kernel_bridge.js (page side) picks it up.
        #   "auto":       "standalone" if js.__pcw_register_sab exists, else
        #                 "kernel" (the kernel worker has no such hook).
        if transport == "auto":
            transport = "standalone" if hasattr(js, "__pcw_register_sab") else "kernel"
        self._transport = transport

        if not getattr(js, "crossOriginIsolated", False):
            # : SAB requires COOP/COEP. Fail loudly and early -
            # Atomics.wait on a non-shared buffer would either throw or, worse,
            # silently busy-spin. The demo asserts this too.
            raise TransportError(
                "crossOriginIsolated is false: the page must be served with "
                "COOP: same-origin and COEP: credentialless for SharedArrayBuffer. "
                "See ."
            )

        # worker_bootstrap.js normally allocates and hands these in. Allocate
        # defensively if not provided (e.g. a bare worker without bootstrap).
        if sab is not None:
            control_sab, data_sab = sab
        else:
            control_sab = js.SharedArrayBuffer.new(_CONTROL_SLOTS * 4)
            data_sab = js.SharedArrayBuffer.new(_DEFAULT_DATA_BYTES)

        self._control_sab = control_sab
        self._ctrl = js.Int32Array.new(control_sab)
        self._gen = 0
        self._attach_data(data_sab)

    def _attach_data(self, data_sab) -> None:
        """(Re)bind the data SAB views and publish its capacity to the control
        word so the main thread (bridge.js) knows the window size to use."""
        self._data_sab = data_sab
        self._data = self._js.Uint8Array.new(data_sab)
        self._capacity = int(data_sab.byteLength)
        self._js.Atomics.store(self._ctrl, _C_CAP, self._capacity)
        # Announce to the main thread so the page-side Bridge attaches the same
        # buffer. The mechanism differs by transport (see __init__).
        if self._transport == "kernel":
            self._post(
                {
                    "__pcw__": {
                        "type": "sab",
                        "control": self._control_sab,
                        "data": data_sab,
                    }
                }
            )
        elif hasattr(self._js, "__pcw_register_sab"):
            # getattr (not attribute syntax): a `.__pcw_register_sab` identifier
            # inside this class body is name-mangled by Python to
            # `_AtomicsBackend__pcw_register_sab`, which the JS global does not
            # have -> AttributeError. The string form is not mangled.
            getattr(self._js, "__pcw_register_sab")(self._control_sab, data_sab)

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

        # A request larger than the data region is not supported by windowing on
        # the *request* side (the main thread reads it in one shot before fetch).
        # Grow if needed so even a big createDataFrame LocalRelation goes through.
        needed = 4 + len(header_bytes) + 4 + len(body)
        if needed > self._capacity:
            self._grow_data_sab(needed)

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
        # Nudge the main thread (it cannot Atomics.wait): the message carries no
        # data; the Bridge reads everything from the SAB. The envelope shape
        # differs by transport so the kernel's own message framing ignores ours.
        if self._transport == "kernel":
            self._post({"__pcw__": {"type": "rpc"}})
        else:
            self._post({"type": "pcw_rpc"})

    def _post(self, payload: dict) -> None:
        """postMessage a dict to the main thread as a plain JS object.

        A Python dict crosses the worker postMessage boundary as a PyProxy, which
        structuredClone cannot serialize ("could not be cloned"). Convert it to a
        real JS object (deep); JsProxy values like SharedArrayBuffers pass through.
        """
        try:
            from pyodide.ffi import to_js
        except Exception:
            # Not under Pyodide (unit tests with a fake js): post as-is so the
            # fake records the plain dict for assertions.
            self._js.postMessage(payload)
            return
        self._js.postMessage(
            to_js(payload, dict_converter=self._js.Object.fromEntries)
        )

    def _grow_data_sab(self, min_bytes: int) -> None:
        """Allocate a larger data SAB (next power-of-two >= ``min_bytes``) and
        re-announce it. Safe to call only when the worker owns the buffer
        (STATE == IDLE, i.e. between RPCs / before writing a request)."""
        new_cap = self._capacity
        while new_cap < min_bytes:
            new_cap *= 2
        self._attach_data(self._js.SharedArrayBuffer.new(new_cap))

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

    # -- response window reading -------------------------------------------- #
    def _read_window(self) -> tuple[dict, bytes]:
        """Read one response window: parse its meta JSON + payload bytes.

        Wire layout per window (main -> worker), written by bridge.js:
            [u32 meta_len][meta json][payload bytes]
        ``meta`` may carry ``{"more": bool}`` when the logical payload spans
        multiple windows. ``length`` (C_LENGTH) is the total valid byte count.
        """
        length = self._js.Atomics.load(self._ctrl, _C_LENGTH)
        meta_len, off = self._get_u32(0)
        meta_raw = bytes(self._data.subarray(off, off + meta_len).to_py())
        off += meta_len
        meta = self._json.loads(meta_raw.decode("utf-8")) if meta_len else {}
        payload = bytes(self._data.subarray(off, off + (length - off)).to_py())
        return meta, payload

    def _reassemble_windows(self, timeout: float | None) -> tuple[dict, bytes]:
        """Read one *logical* payload that may span several windows.

        The worker is woken on the first ``RESP_CHUNK`` (the caller has already
        waited for it). For each window: read it, and if ``meta.more`` is set,
        ack (CHUNK_ACK) and wait for the next window; otherwise stop. This is how
        a single response larger than the data SAB is delivered without realloc.

        Returns ``(first_meta, joined_payload)`` - the *first* window's meta (it
        carries ``headers``/``status`` context; continuation windows only carry
        ``{"more": ...}``).
        """
        Atomics = self._js.Atomics
        parts: list[bytes] = []
        first_meta: dict = {}
        first = True
        while True:
            meta, payload = self._read_window()
            if first:
                first_meta = meta
                first = False
            parts.append(payload)
            if not meta.get("more"):
                return first_meta, b"".join(parts)
            # Request the next window of the same logical payload.
            Atomics.store(self._ctrl, _C_STATE, _S_CHUNK_ACK)
            Atomics.notify(self._ctrl, _C_STATE)
            state = self._wait(_S_CHUNK_ACK, timeout)
            if state == _S_RESP_ERROR:
                raise self._error_from_meta()
            if state != _S_RESP_CHUNK:
                raise TransportError(
                    f"unexpected SAB state {state} while reassembling windows"
                )

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
        my_gen = self._gen
        self._write_request(request)
        state = self._wait(_S_REQ_READY, request.get("timeout"))
        try:
            if state == _S_RESP_ERROR:
                raise self._error_from_meta()
            if state not in (_S_RESP_CHUNK, _S_RESP_END):
                raise TransportError(f"unexpected SAB state {state} for unary")
            status = self._js.Atomics.load(self._ctrl, _C_STATUS)
            if state == _S_RESP_CHUNK:
                # First window already present; reassemble across windows.
                meta_first, body = self._reassemble_windows(request.get("timeout"))
                headers = dict(meta_first.get("headers") or {})
            else:
                body, headers = b"", {}
            # HTTP errors (status >= 400) are NOT a transport failure - hand lane
            # 1 a valid HttpResponse with the status + any grpc-status headers so
            # it raises the right SparkConnectGrpcException (the transport contract section 1).
            return HttpResponse(status=status, headers=headers, body=body)
        finally:
            self._reset_idle_if_current(my_gen)

    def server_stream(self, request: dict) -> Iterator[bytes]:
        self._gen += 1
        my_gen = self._gen
        timeout = request.get("timeout")
        self._write_request(request)
        Atomics = self._js.Atomics
        # The worker and main thread ping-pong STATE. After the request the
        # worker is parked on REQ_READY; for every subsequent chunk/window it
        # parks on CHUNK_ACK (the value it itself wrote to request the next one).
        wait_on = _S_REQ_READY
        try:
            while True:
                state = self._wait(wait_on, timeout)
                if state == _S_RESP_END:
                    return
                if state == _S_RESP_ERROR:
                    raise self._error_from_meta()
                if state != _S_RESP_CHUNK:
                    raise TransportError(f"unexpected SAB state {state} in stream")
                # One stream chunk may itself be windowed if it exceeds the data
                # region; reassemble before yielding so the always sees whole
                # off-the-wire chunks (it re-frames them downstream).
                _meta, chunk = self._reassemble_windows(timeout)
                yield chunk
                # The consumer (e.g. PySpark's reattachable iterator on an eager
                # command) may abandon this stream here and start another RPC,
                # which bumps _C_GEN. If so we are stale: do NOT touch shared
                # STATE (an ack/IDLE would stomp the newer RPC mid-flight).
                if Atomics.load(self._ctrl, _C_GEN) != my_gen:
                    return
                # Ack: request the next chunk, then park on CHUNK_ACK.
                Atomics.store(self._ctrl, _C_STATE, _S_CHUNK_ACK)
                Atomics.notify(self._ctrl, _C_STATE)
                wait_on = _S_CHUNK_ACK
                timeout = request.get("timeout")
        finally:
            self._reset_idle_if_current(my_gen)

    def _reset_idle_if_current(self, my_gen: int) -> None:
        """Flip STATE back to IDLE on exit, but ONLY if this RPC is still the
        current one. A stream abandoned by the consumer is finalized (its
        ``finally`` runs) possibly long after a newer RPC has started; writing
        IDLE then would stomp that newer RPC. ``_C_GEN`` (bumped by every
        ``_write_request``) is the guard: skip if a newer generation owns the
        channel."""
        Atomics = self._js.Atomics
        if Atomics.load(self._ctrl, _C_GEN) != my_gen:
            return
        Atomics.store(self._ctrl, _C_STATE, _S_IDLE)
        Atomics.notify(self._ctrl, _C_STATE)

    # -- error mapping ------------------------------------------------------- #
    def _error_from_meta(self) -> TransportError:
        """Build the right exception type from the error meta the main thread
        wrote. ``meta.kind`` distinguishes timeout/abort/generic so PySpark sees
        a meaningful cause."""
        meta = {}
        try:
            meta_len, off = self._get_u32(0)
            raw = bytes(self._data.subarray(off, off + meta_len).to_py())
            meta = self._json.loads(raw.decode("utf-8")) if meta_len else {}
        except Exception:
            return TransportError(
                "transport error (and failed to read error detail from SAB)"
            )
        message = str(meta.get("message", "transport error"))
        kind = meta.get("kind")
        if kind == "timeout":
            return TransportTimeout(message)
        if kind == "abort":
            return TransportAborted(message)
        return TransportError(message)


# Convenience for callers that want a no-arg factory in the worker.
def make_channel(base_url: str, **kw) -> SabSyncChannel:
    """Build a :class:`SabSyncChannel` for ``base_url`` (Pyodide auto-backend)."""
    return SabSyncChannel(base_url, **kw)


__all__ = [
    "SabSyncChannel",
    "SyncBackend",
    "TransportError",
    "TransportTimeout",
    "TransportAborted",
    "is_pyodide",
    "make_channel",
]
