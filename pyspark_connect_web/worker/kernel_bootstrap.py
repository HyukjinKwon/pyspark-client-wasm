# SPDX-License-Identifier: Apache-2.0
"""Kernel-side bootstrap for running pyspark-connect-web inside the JupyterLite
pyodide kernel worker.

Background
----------
``worker_bootstrap.js`` is a *standalone* harness that owns its Web Worker. The
JupyterLite ``@jupyterlite/pyodide-kernel`` does **not** let us own the worker -
it spawns its own ES-module worker and runs Pyodide there with its own comms
(``coincident`` when the page is cross-origin isolated, ``comlink`` otherwise).

We therefore integrate non-invasively, in two halves:

* **Page side** - ``jupyterlite/pcw_kernel_bridge.js`` wraps the global
  ``Worker`` constructor before JupyterLite boots, so every kernel worker gets a
  ``Bridge`` (from ``worker/bridge.js``) attached. It performs the real
  cross-origin ``fetch`` and writes response windows back into the SAB. It only
  reacts to our namespaced envelope ``{__pcw__:{...}}`` and ignores everything
  the kernel's own framing uses.

* **Worker side** - *this module*, imported once inside the kernel (e.g. the
  first notebook cell does ``import pyspark_connect_web.worker.kernel_bootstrap``
  or simply ``pyspark_connect_web.install()``). It verifies cross-origin
  isolation and lets :class:`~.sab_channel.SabSyncChannel` allocate the SAB and
  speak the *kernel* transport: it posts ``{__pcw__:{type:"sab",...}}`` once and
  ``{__pcw__:{type:"rpc"}}`` per request, then parks on ``Atomics.wait``.

The selection is automatic: under Pyodide with no ``js.__pcw_register_sab`` hook
(the kernel worker has none), ``SabSyncChannel`` picks ``transport="kernel"``.
So application code does not need to call anything here - ``pcw.install()`` is
enough. This module exists to (a) give an explicit, documented entry point and
(b) fail loudly with actionable guidance when cross-origin isolation is missing
(the #1 footgun on GitHub Pages - use ``coi-serviceworker.js``).
"""
from __future__ import annotations

from .sab_channel import TransportError, is_pyodide


def assert_kernel_ready() -> None:
    """Raise a clear, actionable error unless the kernel worker can host the
    blocking SAB bridge. Safe to call from a notebook cell before ``install()``.
    """
    if not is_pyodide():
        raise TransportError(
            "kernel_bootstrap: not running under Pyodide. This module is meant "
            "to run inside the JupyterLite pyodide kernel worker."
        )
    import js  # noqa: F401  (Pyodide-injected)

    if not getattr(js, "crossOriginIsolated", False):
        raise TransportError(
            "Page is NOT cross-origin isolated, so SharedArrayBuffer/Atomics are "
            "unavailable and the blocking bridge cannot work.\n"
            "Fix by serving the JupyterLite site with:\n"
            "    Cross-Origin-Opener-Policy:   same-origin\n"
            "    Cross-Origin-Embedder-Policy: credentialless\n"
            "On a host that cannot set headers (e.g. GitHub Pages), include "
            "coi-serviceworker.js on the page (it injects these headers via a "
            "service worker and reloads once). See jupyterlite/README.md."
        )
    # Also confirm the page-side Worker wrapper is present. If pcw_kernel_bridge
    # never ran, our envelopes go nowhere and RPCs would hang; better to say so.
    # The wrapper sets this flag on the *worker's* global only if it shares it;
    # we cannot see the page global from here, so this is best-effort and only
    # warns via the returned value of installed().


def installed() -> bool:
    """Best-effort check that we are in a kernel worker with isolation on.

    Returns ``True`` if a :class:`SabSyncChannel` built here would use the kernel
    SAB transport. Does not (and cannot) verify the page-side wrapper from inside
    the worker - that is validated end-to-end in a real browser.
    """
    if not is_pyodide():
        return False
    try:
        import js  # noqa: F401

        return bool(getattr(js, "crossOriginIsolated", False)) and not hasattr(
            js, "__pcw_register_sab"
        )
    except Exception:
        return False


# Importing this module from inside the kernel is enough to assert readiness;
# do it lazily so a plain CPython import (tests) does not blow up.
if is_pyodide():  # pragma: no cover - browser only
    try:
        assert_kernel_ready()
    except TransportError:
        # Defer the error to connect time (install()/getOrCreate) where the user
        # gets a stack they can act on; importing should not hard-crash the cell.
        pass


__all__ = ["assert_kernel_ready", "installed"]
