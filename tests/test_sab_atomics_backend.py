# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the Atomics/SAB backend's windowing, realloc, and error paths.

These exercise :class:`pyspark_connect_web.worker.sab_channel._AtomicsBackend`
*without a browser* by injecting a fake ``js`` module that simulates
``SharedArrayBuffer`` + ``Atomics`` and a cooperating "main thread" that plays
exactly the role of ``bridge.js`` - reading the request out of the data SAB and
writing response *windows* back, honouring the same STATE handshake.

This is the closest we can get to the real handshake off-browser, and it covers
the three things the brief calls out: a payload larger than the initial buffer,
multi-window streaming reassembly, and a realloc path - plus error/timeout
mapping across the SAB boundary.

The real ``Atomics.wait``/``notify`` between two OS threads cannot be modelled in
one CPython thread, so the fake collapses it: when the worker calls
``Atomics.wait``, the fake synchronously runs the main thread's next step (which
flips STATE), so ``wait`` then sees STATE moved and returns. This validates the
*protocol* (window framing, ``more`` reassembly, ack sequencing, realloc, error
meta), not the OS-level blocking - which is the documented browser-only item.
"""
from __future__ import annotations

import json
import sys

import pytest

import pyspark_connect_web.worker.sab_channel as sc
from pyspark_connect_web.worker.sab_channel import (
    TransportAborted,
    TransportError,
    TransportTimeout,
    _C_LENGTH,
    _C_STATE,
    _C_STATUS,
    _META_ZONE,
    _S_CHUNK_ACK,
    _S_IDLE,
    _S_REQ_READY,
    _S_RESP_CHUNK,
    _S_RESP_END,
    _S_RESP_ERROR,
)


# --------------------------------------------------------------------------- #
# Minimal fake `js` (SharedArrayBuffer + typed arrays + Atomics) + a scripted
# main thread that mimics bridge.js's window emission.
# --------------------------------------------------------------------------- #
class _Sab:
    def __init__(self, n):
        self.byteLength = n
        self.buf = bytearray(n)


class _U8:
    """Subset of Uint8Array over a _Sab used by the backend."""

    def __init__(self, sab_or_list):
        if isinstance(sab_or_list, _Sab):
            self._buf = sab_or_list.buf
        else:  # list of ints (Uint8Array.new(list(bytes)))
            self._buf = bytearray(sab_or_list)

    def __getitem__(self, i):
        return self._buf[i]

    def __setitem__(self, i, v):
        self._buf[i] = v & 0xFF

    def set(self, src, off):
        data = src._buf if isinstance(src, _U8) else bytearray(src)
        self._buf[off:off + len(data)] = data

    def subarray(self, a, b):
        return _U8(list(self._buf[a:b]))

    def to_py(self):
        return bytes(self._buf)


class _I32:
    """Int32Array over a _Sab (4-byte little-endian slots)."""

    def __init__(self, sab):
        self._sab = sab

    def _load(self, i):
        b = self._sab.buf[i * 4:i * 4 + 4]
        return int.from_bytes(b, "little", signed=True)

    def _store(self, i, v):
        self._sab.buf[i * 4:i * 4 + 4] = int(v).to_bytes(4, "little", signed=True)


class _Atomics:
    """Atomics shim. ``wait`` drives the scripted main thread one step so the
    single-threaded test can make progress, then returns based on STATE."""

    def __init__(self, js):
        self._js = js

    def store(self, arr, i, v):
        arr._store(i, v)

    def load(self, arr, i):
        return arr._load(i)

    def notify(self, arr, i):
        return 0

    def wait(self, arr, i, expected, ms):
        # If a timeout/error is scripted to fire on this wait, honour it.
        if self._js._timeout_on_wait:
            self._js._timeout_on_wait = False
            return "timed-out"
        # Run the main thread's next step (it will flip STATE).
        self._js._main_step()
        return "ok" if arr._load(i) != expected else "not-equal"


class FakeJs:
    """Stand-in for Pyodide's ``js`` module + a scripted bridge.js main thread.

    Construct with the response plan, then build an ``_AtomicsBackend`` with this
    injected (we monkeypatch ``import js`` to return us). The plan is a list of
    "messages" (unary => 1 message; stream => N messages), each a ``bytes``
    payload; an optional terminal ``error``/``timeout`` simulates failures.
    """

    def __init__(self, *, messages=None, status=200, headers=None,
                 error=None, error_kind="error", timeout_on_wait=False,
                 initial_data_bytes=None):
        self.crossOriginIsolated = True
        self.Atomics = _Atomics(self)
        self.SharedArrayBuffer = self._SabFactory()
        self.Int32Array = self._I32Factory()
        self.Uint8Array = self._U8Factory()
        self.posted = []
        self._messages = messages if messages is not None else []
        self._status = status
        self._headers = headers or {}
        self._error = error
        self._error_kind = error_kind
        self._timeout_on_wait = timeout_on_wait
        self._initial_data_bytes = initial_data_bytes
        # Main-thread emission state (mirrors bridge.js handleRpc):
        self._phase = "idle"   # idle -> reading -> emitting
        self._msg_index = 0
        self._sent = 0         # bytes of current message already windowed
        self._ctrl = None      # _I32 over the control SAB (set on first store)
        self._data = None      # _U8 over the data SAB
        # Track the buffers the worker registered so the main thread reads the
        # *current* (possibly realloc'd) data SAB.
        self._control_sab = None
        self._data_sab = None

    # -- typed-array / SAB factories return objects with .new(...) ----------- #
    def _SabFactory(self):
        class _F:
            @staticmethod
            def new(n):
                return _Sab(n)

        return _F

    def _I32Factory(self):
        class _F:
            @staticmethod
            def new(sab):
                return _I32(sab)

        return _F

    def _U8Factory(self):
        class _F:
            @staticmethod
            def new(x):
                return _U8(x)

        return _F

    def postMessage(self, msg):
        # The backend posts the SAB announce + per-RPC nudge here. We snapshot
        # the buffers it announced so the "main thread" reads the right ones.
        self.posted.append(msg)
        env = None
        try:
            env = msg.get("__pcw__")
        except Exception:
            env = None
        if env is not None:
            if env.get("type") == "sab":
                self._control_sab = env["control"]
                self._data_sab = env["data"]

    # -- the scripted "main thread" (bridge.js) ------------------------------ #
    def _cap_payload(self):
        return len(self._data._buf) - _META_ZONE - 4

    def _write_window(self, state, meta, payload):
        b = self._data._buf
        meta_bytes = json.dumps(meta).encode("utf-8")
        off = 0
        b[off:off + 4] = len(meta_bytes).to_bytes(4, "little")
        off += 4
        b[off:off + len(meta_bytes)] = meta_bytes
        off += len(meta_bytes)
        if payload:
            b[off:off + len(payload)] = payload
            off += len(payload)
        self._ctrl._store(_C_STATUS, self._status)
        self._ctrl._store(_C_LENGTH, off)
        self._ctrl._store(_C_STATE, state)

    def _bind_current(self):
        # The worker stores into the control SAB and registers data via post.
        # We read those handles to attach our own views.
        self._ctrl = _I32(self._control_sab)
        self._data = _U8(self._data_sab)

    def _main_step(self):
        """Advance one transition, mirroring bridge.js. Called from Atomics.wait."""
        if self._control_sab is None:
            # No SAB announced yet - nothing the main thread can do.
            return
        self._bind_current()
        state = self._ctrl._load(_C_STATE)

        if self._error is not None and self._phase in ("idle", "reading"):
            # Emit a transport error window before any data.
            self._write_window(
                _S_RESP_ERROR, {"message": self._error, "kind": self._error_kind}, b""
            )
            self._error = None
            return

        if state == _S_REQ_READY and self._phase == "idle":
            self._phase = "emitting"
            self._msg_index = 0
            self._sent = 0
            self._emit_next_window(first_msg=True)
            return

        if state == _S_CHUNK_ACK and self._phase == "emitting":
            # Worker consumed a window / message and asked for the next.
            self._emit_next_window(first_msg=False)
            return

        if state == _S_IDLE:
            self._phase = "idle"
            return

    def _emit_next_window(self, first_msg):
        if self._msg_index >= len(self._messages):
            # No more messages -> end of stream.
            self._ctrl._store(_C_LENGTH, 0)
            self._ctrl._store(_C_STATE, _S_RESP_END)
            self._phase = "done"
            return
        msg = self._messages[self._msg_index]
        cap = self._cap_payload()
        end = min(self._sent + cap, len(msg))
        window = msg[self._sent:end]
        more = end < len(msg)
        is_first_window = self._sent == 0
        meta = {"more": more}
        if is_first_window and self._msg_index == 0:
            meta["headers"] = self._headers
            meta["ok"] = 200 <= self._status < 400
        self._write_window(_S_RESP_CHUNK, meta, window)
        self._sent = end
        if not more:
            # Finished this message; advance for the next ack.
            self._msg_index += 1
            self._sent = 0


# --------------------------------------------------------------------------- #
# Helper to build a backend with a fake js injected.
# --------------------------------------------------------------------------- #
def _make_backend(monkeypatch, fake, *, data_bytes=None):
    # Inject the fake `js` module so `import js` inside the backend returns it.
    monkeypatch.setitem(sys.modules, "js", fake)
    # Force a small data SAB if requested so we can test windowing cheaply.
    if data_bytes is not None:
        monkeypatch.setattr(sc, "_DEFAULT_DATA_BYTES", data_bytes)
    return sc._AtomicsBackend("https://envoy.example:8081", transport="kernel")


def _req(kind="unary", body=b"x", timeout=None):
    return {
        "kind": kind,
        "url": "https://envoy.example:8081/spark.connect.SparkConnectService/X",
        "path": "/spark.connect.SparkConnectService/X",
        "body": body,
        "headers": {"content-type": "application/grpc-web+proto"},
        "timeout": timeout,
    }


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_unary_small_payload_roundtrip(monkeypatch):
    fake = FakeJs(messages=[b"hello-body"], status=200, headers={"grpc-status": "0"})
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    resp = backend.unary(_req())
    assert resp.status == 200
    assert resp.body == b"hello-body"
    assert resp.headers.get("grpc-status") == "0"
    # The kernel transport announces the SAB via a namespaced envelope.
    assert any("__pcw__" in p and p["__pcw__"]["type"] == "sab" for p in fake.posted)


def test_unary_payload_larger_than_buffer_is_windowed(monkeypatch):
    # Data region holds only a few hundred payload bytes; body is many KiB, so
    # it MUST be delivered across multiple windows and reassembled exactly.
    body = bytes((i * 7) & 0xFF for i in range(50_000))
    # tiny buffer: META_ZONE (4096) + 4 + small payload window
    fake = FakeJs(messages=[body], status=200)
    backend = _make_backend(monkeypatch, fake, data_bytes=_META_ZONE + 4 + 512)
    resp = backend.unary(_req())
    assert resp.status == 200
    assert resp.body == body  # exact reassembly across many windows


def test_stream_multi_window_reassembly(monkeypatch):
    # Three stream messages, the middle one larger than the buffer (so it is
    # itself windowed). The worker must yield exactly three whole chunks.
    big = bytes((i * 3) & 0xFF for i in range(20_000))
    msgs = [b"frame-A", big, b"frame-C"]
    fake = FakeJs(messages=msgs, status=200)
    backend = _make_backend(monkeypatch, fake, data_bytes=_META_ZONE + 4 + 1024)
    got = list(backend.server_stream(_req(kind="server_stream")))
    assert got == msgs


def test_stream_empty_ends_cleanly(monkeypatch):
    fake = FakeJs(messages=[], status=200)
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    assert list(backend.server_stream(_req(kind="server_stream"))) == []


def test_realloc_when_request_exceeds_capacity(monkeypatch):
    # A request body bigger than the initial data SAB must trigger _grow_data_sab
    # and still complete. We start very small and send a large request.
    big_req = bytes(range(256)) * 200  # ~51 KiB request body
    fake = FakeJs(messages=[b"ok"], status=200)
    backend = _make_backend(monkeypatch, fake, data_bytes=_META_ZONE + 4 + 256)
    start_cap = backend._capacity
    resp = backend.unary(_req(body=big_req))
    assert resp.status == 200
    assert resp.body == b"ok"
    assert backend._capacity > start_cap  # grew to fit the request
    # A realloc re-announces the SAB to the main thread (>=2 sab envelopes).
    sab_announces = [p for p in fake.posted
                     if "__pcw__" in p and p["__pcw__"]["type"] == "sab"]
    assert len(sab_announces) >= 2


def test_error_maps_to_transport_error(monkeypatch):
    fake = FakeJs(messages=[], error="upstream 502", error_kind="error")
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    with pytest.raises(TransportError, match="upstream 502"):
        backend.unary(_req())


def test_error_kind_timeout_maps_to_transport_timeout(monkeypatch):
    fake = FakeJs(messages=[], error="fetch timed out after 5s", error_kind="timeout")
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    with pytest.raises(TransportTimeout):
        backend.unary(_req())


def test_error_kind_abort_maps_to_transport_aborted(monkeypatch):
    fake = FakeJs(messages=[], error="aborted", error_kind="abort")
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    with pytest.raises(TransportAborted):
        backend.unary(_req())


def test_wait_timeout_raises_transport_timeout(monkeypatch):
    # Simulate Atomics.wait itself timing out (the worker-side deadline).
    fake = FakeJs(messages=[b"never-arrives"], timeout_on_wait=True)
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    with pytest.raises(TransportTimeout):
        backend.unary(_req(timeout=0.001))


def test_crossorigin_isolation_required(monkeypatch):
    fake = FakeJs(messages=[b"x"])
    fake.crossOriginIsolated = False
    monkeypatch.setitem(sys.modules, "js", fake)
    with pytest.raises(TransportError, match="crossOriginIsolated"):
        sc._AtomicsBackend("https://e:1", transport="kernel")


def test_kernel_transport_uses_namespaced_envelope(monkeypatch):
    fake = FakeJs(messages=[b"ok"], status=200)
    backend = _make_backend(monkeypatch, fake, data_bytes=64 * 1024)
    backend.unary(_req())
    # No raw {type:"pcw_rpc"} (that is the standalone shape); all ours are wrapped.
    assert all("__pcw__" in p for p in fake.posted)
    assert any(p["__pcw__"]["type"] == "rpc" for p in fake.posted)
