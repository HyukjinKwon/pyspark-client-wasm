# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``pyspark_connect_web._grpc_shim`` (the grpcio stand-in).

The shim only *installs* when real ``grpcio`` is absent (Pyodide). In local dev /
CI-parity runs grpcio is present, so ``install_grpc_shim()`` early-returns and the
interesting code (``_build_module`` / ``_build_grpc_status_modules`` / the install
path) is never exercised by import alone. These tests therefore:

  * exercise the *builders* directly (no grpcio dependency at all), and
  * simulate grpcio absence with a ``sys.meta_path`` blocker + a clean
    ``sys.modules`` so the real ``install_grpc_shim()`` install branch runs and is
    asserted to be idempotent and a no-op when real grpcio is present.

No network, no browser, no grpcio is required by these tests.
"""
from __future__ import annotations

import enum
import importlib
import sys

import pytest

from pyspark_connect_web import _grpc_shim


# --------------------------------------------------------------------------- #
# Helpers: simulate "grpcio is absent" without uninstalling it.
# --------------------------------------------------------------------------- #
class _BlockFinder:
    """A meta_path finder that makes ``find_spec('grpc'|'grpcio'|...)`` return None.

    ``install_grpc_shim`` decides real-grpcio presence via
    ``importlib.util.find_spec('grpc')``. Inserting this finder at the *front* of
    ``sys.meta_path`` shadows the real grpcio spec so the shim's install branch
    runs deterministically even on a machine that has grpcio installed.
    """

    def __init__(self, *names: str) -> None:
        self._blocked = set(names)

    def find_spec(self, name, path, target=None):  # noqa: D401
        if name in self._blocked or name.split(".", 1)[0] in self._blocked:
            # Returning None alone would let the next finder resolve it; raise
            # so find_spec reports "no such module".
            raise ModuleNotFoundError(name)
        return None


@pytest.fixture
def no_grpcio(monkeypatch):
    """Run the body with grpc/grpc_status absent from sys.modules + unfindable."""
    saved = {
        k: sys.modules.get(k)
        for k in ("grpc", "grpc_status", "grpc_status.rpc_status")
    }
    for k in saved:
        sys.modules.pop(k, None)
    finder = _BlockFinder("grpc", "grpc_status", "grpcio")
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        try:
            sys.meta_path.remove(finder)
        except ValueError:
            pass
        # Restore whatever was there (real grpc module, or nothing).
        for k, v in saved.items():
            if v is not None:
                sys.modules[k] = v
            else:
                sys.modules.pop(k, None)


# --------------------------------------------------------------------------- #
# _build_module — the grpc stand-in
# --------------------------------------------------------------------------- #
def test_build_module_exposes_version_and_marker():
    m = _grpc_shim._build_module()
    assert m.__name__ == "grpc"
    assert m.__pcw_shim__ is True
    # PySpark's check_dependencies compares grpc.__version__ against a floor.
    assert isinstance(m.__version__, str)
    assert m.__version__.split(".")[0].isdigit()


def test_build_module_status_code_is_enum_with_canonical_values():
    m = _grpc_shim._build_module()
    assert issubclass(m.StatusCode, enum.Enum)
    # PySpark indexes by name and reads .value == (int_code, "label").
    assert m.StatusCode.OK.value == (0, "ok")
    assert m.StatusCode.UNAVAILABLE.value[0] == 14
    assert m.StatusCode.INTERNAL.value[0] == 13
    # all 17 canonical codes present
    assert len(list(m.StatusCode)) == 17


def test_build_module_rpc_error_is_exception_subclass():
    m = _grpc_shim._build_module()
    assert issubclass(m.RpcError, Exception)


def test_build_module_marker_classes_exist():
    m = _grpc_shim._build_module()
    for name in ("Call", "RpcContext", "Channel", "ServicerContext"):
        assert isinstance(getattr(m, name), type)


def test_build_module_channel_constructors_raise_loudly():
    """The channel/credentials constructors must exist but raise if ever called
    (our patched path never calls them; an unforeseen call should fail loudly)."""
    m = _grpc_shim._build_module()
    for name in ("insecure_channel", "secure_channel", "ssl_channel_credentials"):
        fn = getattr(m, name)
        assert callable(fn)
        with pytest.raises(RuntimeError):
            fn("anything")


def test_build_module_getattr_guard_returns_loud_callable():
    """PEP 562 __getattr__: any unforeseen attribute resolves to the guarded
    callable (so it fails at the call site, not at import)."""
    m = _grpc_shim._build_module()
    unknown = m.__getattr__("some_unforeseen_grpc_symbol")
    assert callable(unknown)
    with pytest.raises(RuntimeError):
        unknown()


def test_build_module_compression_and_connectivity_enums():
    m = _grpc_shim._build_module()
    assert m.Compression.NoCompression == 0
    assert m.Compression.Gzip == 2
    assert m.ChannelConnectivity.READY.value == "ready"


# --------------------------------------------------------------------------- #
# _build_grpc_status_modules — grpc_status / rpc_status stand-in
# --------------------------------------------------------------------------- #
def test_build_grpc_status_modules_from_call_returns_none():
    pkg, rpc_status = _grpc_shim._build_grpc_status_modules()
    assert pkg.__name__ == "grpc_status"
    assert pkg.__pcw_shim__ is True
    assert pkg.rpc_status is rpc_status
    # Enriched error details are unavailable without grpcio: from_call -> None.
    assert rpc_status.from_call(object()) is None


def test_build_grpc_status_to_status_raises():
    _pkg, rpc_status = _grpc_shim._build_grpc_status_modules()
    with pytest.raises(RuntimeError):
        rpc_status.to_status(object())


# --------------------------------------------------------------------------- #
# install_grpc_shim — install / idempotency / no-op semantics
# --------------------------------------------------------------------------- #
def test_install_is_noop_when_real_grpcio_present():
    """grpcio is installed in local dev / CI parity — install must NOT shadow it
    and must return False (real grpcio present, nothing installed)."""
    if importlib.util.find_spec("grpc") is None:
        pytest.skip("grpcio absent in this environment; no-op path not testable")
    # Drop any cached module first so the function takes the find_spec branch.
    saved = sys.modules.pop("grpc", None)
    try:
        installed = _grpc_shim.install_grpc_shim()
        assert installed is False
        # It must not have planted our stub.
        planted = sys.modules.get("grpc")
        if planted is not None:
            assert not getattr(planted, "__pcw_shim__", False)
    finally:
        if saved is not None:
            sys.modules["grpc"] = saved


def test_install_returns_true_marker_if_shim_already_loaded(monkeypatch):
    """If our shim is already in sys.modules, install reports it as installed."""
    monkeypatch.setitem(sys.modules, "grpc", _grpc_shim._build_module())
    assert _grpc_shim.install_grpc_shim() is True


def test_install_returns_false_if_real_grpc_already_loaded(monkeypatch):
    """A non-shim grpc already in sys.modules -> not our shim -> returns False."""
    import types

    fake_real = types.ModuleType("grpc")  # no __pcw_shim__ marker
    monkeypatch.setitem(sys.modules, "grpc", fake_real)
    assert _grpc_shim.install_grpc_shim() is False


def test_install_actually_installs_when_grpcio_absent(no_grpcio):
    """The real install branch: with grpcio unfindable, the shim is planted for
    grpc + grpc_status + grpc_status.rpc_status, and the call is idempotent."""
    assert "grpc" not in sys.modules
    installed = _grpc_shim.install_grpc_shim()
    assert installed is True

    grpc_mod = sys.modules["grpc"]
    assert grpc_mod.__pcw_shim__ is True
    assert sys.modules["grpc_status"].__pcw_shim__ is True
    assert sys.modules["grpc_status.rpc_status"].from_call(None) is None

    # Idempotent: a second call sees our shim already present and returns True
    # without rebuilding/replacing it.
    again = _grpc_shim.install_grpc_shim()
    assert again is True
    assert sys.modules["grpc"] is grpc_mod


def test_install_swallows_find_spec_exception(no_grpcio, monkeypatch):
    """If find_spec itself raises, the shim treats grpcio as absent and installs."""
    def boom(_name):
        raise RuntimeError("find_spec exploded")

    monkeypatch.setattr(_grpc_shim.importlib.util, "find_spec", boom)
    assert _grpc_shim.install_grpc_shim() is True
    assert sys.modules["grpc"].__pcw_shim__ is True


def test_installed_shim_satisfies_import_grpc(no_grpcio):
    """After installing the shim with grpcio absent, ``import grpc`` resolves to
    our stub and the symbols PySpark touches are present."""
    _grpc_shim.install_grpc_shim()
    import grpc  # noqa: PLC0415  (resolves to the shim under no_grpcio)

    assert getattr(grpc, "__pcw_shim__", False) is True
    assert grpc.StatusCode.OK.value == (0, "ok")
    assert issubclass(grpc.RpcError, Exception)
    from grpc_status import rpc_status  # noqa: PLC0415

    assert rpc_status.from_call(None) is None
