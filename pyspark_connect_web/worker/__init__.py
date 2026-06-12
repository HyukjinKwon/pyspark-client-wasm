# SPDX-License-Identifier: Apache-2.0
"""Lane 3: Pyodide runtime + the Atomics/SharedArrayBuffer blocking bridge.

This package owns the blocking byte transport that lets PySpark's synchronous
``.collect()`` work from a Web Worker on top of an inherently-async browser
``fetch``. The Python side is :class:`~.sab_channel.SabSyncChannel`, which
implements the ``SyncChannel`` protocol from ``_contract.py``.

Under Pyodide the channel marshals each request into a ``SharedArrayBuffer``,
posts to the main thread (which performs the real ``fetch``), and blocks the
worker with ``Atomics.wait`` until the response is written back. Outside Pyodide
(local unit tests) it falls back to a pluggable synchronous backend so the API
can be exercised without a browser, ``grpcio``, or a network.

The JS glue lives next to this module:
  * ``bridge.js``           - main-thread fetch handler (request -> fetch -> SAB)
  * ``worker_bootstrap.js`` - loads Pyodide, micropip-installs the wheel, wires
                              the SAB + ``Atomics.wait`` protocol.

The exact SAB layout and Atomics handshake are documented in
``team/findings-lane3-bridge.md``.
"""
from __future__ import annotations

from .sab_channel import (
    SabSyncChannel,
    SyncBackend,
    TransportAborted,
    TransportError,
    TransportTimeout,
    is_pyodide,
)

__all__ = [
    "SabSyncChannel",
    "SyncBackend",
    "TransportAborted",
    "TransportError",
    "TransportTimeout",
    "is_pyodide",
]
