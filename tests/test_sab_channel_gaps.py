# SPDX-License-Identifier: Apache-2.0
"""Coverage-gap tests for ``pyspark_connect_web.worker.sab_channel`` off-browser.

Complements ``tests/test_sab_channel.py`` without touching it. Covers the
environment-detection branches and the ``make_channel`` helper that the existing
suite leaves uncovered. The Atomics backend itself is browser-only and exercised
by ``tests/test_sab_atomics_backend.py``. No browser, no grpcio, no network.
"""
from __future__ import annotations

import sys

import pytest

from pyspark_connect_web._contract import HttpResponse
from pyspark_connect_web.worker import sab_channel
from pyspark_connect_web.worker.sab_channel import (
    SabSyncChannel,
    TransportError,
    is_pyodide,
    make_channel,
)


class _FakeBackend:
    def unary(self, request):
        return HttpResponse(200)

    def server_stream(self, request):
        yield b""


def test_is_pyodide_true_on_emscripten_platform(monkeypatch):
    monkeypatch.setattr(sab_channel.sys, "platform", "emscripten")
    assert is_pyodide() is True


def test_is_pyodide_true_when_js_module_importable(monkeypatch):
    """Non-emscripten platform but a Pyodide-injected ``js`` module present."""
    monkeypatch.setattr(sab_channel.sys, "platform", "linux")
    import types

    monkeypatch.setitem(sys.modules, "js", types.ModuleType("js"))
    assert is_pyodide() is True


def test_is_pyodide_false_on_cpython(monkeypatch):
    """The normal local/test path: not emscripten and no ``js`` module."""
    monkeypatch.setattr(sab_channel.sys, "platform", "darwin")
    monkeypatch.setitem(sys.modules, "js", None)  # import js -> ImportError
    assert is_pyodide() is False


def test_make_channel_builds_sab_sync_channel_with_injected_backend():
    """``make_channel`` is the worker convenience factory; off-browser it still
    builds a SabSyncChannel when a backend is injected via kwargs."""
    ch = make_channel("https://h:8081", backend=_FakeBackend())
    assert isinstance(ch, SabSyncChannel)
    assert ch.base_url == "https://h:8081"


def test_make_channel_without_backend_raises_off_browser(monkeypatch):
    """Off-browser with no backend, make_channel surfaces the no-backend error."""
    monkeypatch.setattr(sab_channel, "is_pyodide", lambda: False)
    with pytest.raises(TransportError, match="no backend"):
        make_channel("https://h:8081")
