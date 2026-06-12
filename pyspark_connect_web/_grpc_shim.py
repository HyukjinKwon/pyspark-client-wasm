# SPDX-License-Identifier: Apache-2.0
"""A minimal stand-in for the ``grpc`` module.

``grpcio`` is a C-extension that does not exist in Pyodide (DECISIONS.md #1), yet
PySpark's ``pyspark.sql.connect`` stack does ``import grpc`` at module load
(client core, error mapping, channel builder). Since we replace the gRPC stub
and channel entirely (see ``patch.py``), real grpc is never *called* on our path
— but the imports must still resolve. This shim satisfies them.

``install_grpc_shim()`` is a no-op when real ``grpcio`` is importable (local dev,
CI parity runs), so we never shadow the genuine library when it is present.
"""
from __future__ import annotations

import enum
import importlib.util
import sys
import types


class _ShimError(RuntimeError):
    """Raised if code reaches a real-grpc call path that should have been patched."""


def _build_module() -> types.ModuleType:
    m = types.ModuleType("grpc")
    m.__doc__ = "Minimal grpc shim installed by pyspark_connect_web (no grpcio in Pyodide)."
    m.__pcw_shim__ = True  # marker so we can detect/uninstall our own shim
    # PySpark's check_dependencies() compares grpc.__version__ against its
    # minimum; report a version comfortably above any pinned floor.
    m.__version__ = "1.76.0"

    class RpcError(Exception):
        """Stand-in for grpc.RpcError; the base PySpark retries on."""

    # grpc.StatusCode is a real enum PySpark indexes by name and reads .value.
    # Each member's value is the canonical (int_code, "label") tuple grpcio uses.
    class StatusCode(enum.Enum):
        OK = (0, "ok")
        CANCELLED = (1, "cancelled")
        UNKNOWN = (2, "unknown")
        INVALID_ARGUMENT = (3, "invalid argument")
        DEADLINE_EXCEEDED = (4, "deadline exceeded")
        NOT_FOUND = (5, "not found")
        ALREADY_EXISTS = (6, "already exists")
        PERMISSION_DENIED = (7, "permission denied")
        RESOURCE_EXHAUSTED = (8, "resource exhausted")
        FAILED_PRECONDITION = (9, "failed precondition")
        ABORTED = (10, "aborted")
        OUT_OF_RANGE = (11, "out of range")
        UNIMPLEMENTED = (12, "unimplemented")
        INTERNAL = (13, "internal")
        UNAVAILABLE = (14, "unavailable")
        DATA_LOSS = (15, "data loss")
        UNAUTHENTICATED = (16, "unauthenticated")

    class Compression(enum.IntEnum):
        NoCompression = 0
        Deflate = 1
        Gzip = 2

    class ChannelConnectivity(enum.Enum):
        IDLE = "idle"
        CONNECTING = "connecting"
        READY = "ready"
        TRANSIENT_FAILURE = "transient_failure"
        SHUTDOWN = "shutdown"

    m.RpcError = RpcError
    m.StatusCode = StatusCode
    m.Compression = Compression
    m.ChannelConnectivity = ChannelConnectivity
    # Empty marker base classes occasionally referenced for isinstance/typing.
    m.Call = type("Call", (), {})
    m.RpcContext = type("RpcContext", (), {})
    m.Channel = type("Channel", (), {})
    m.ServicerContext = type("ServicerContext", (), {})

    def _unsupported(*_args, **_kwargs):
        raise _ShimError(
            "real grpc is unavailable in this environment (grpcio is not in "
            "Pyodide); this call path must be patched out by pyspark_connect_web."
        )

    # Channel/credentials constructors that must exist but are never called on
    # our patched path (patch.py's toChannel returns a WebChannel instead).
    for _name in (
        "insecure_channel",
        "secure_channel",
        "ssl_channel_credentials",
        "local_channel_credentials",
        "metadata_call_credentials",
        "access_token_call_credentials",
        "composite_channel_credentials",
        "composite_call_credentials",
        "channel_ready_future",
        "intercept_channel",
        "compute_engine_channel_credentials",
    ):
        setattr(m, _name, _unsupported)

    # PEP 562: anything else PySpark touches resolves to the guarded callable
    # rather than AttributeError, so an unforeseen reference fails loudly *at the
    # call site* (with our message) instead of breaking import.
    def __getattr__(name: str):  # noqa: N807
        return _unsupported

    m.__getattr__ = __getattr__  # type: ignore[attr-defined]
    return m


def _build_grpc_status_modules():
    """Stub ``grpc_status`` + its ``rpc_status`` submodule.

    grpcio-status reads google.rpc.Status out of a real grpc.Call's trailing
    metadata, which we don't have. ``from_call`` therefore returns None
    (enriched error details simply unavailable); pyspark falls back to the plain
    status message, which our grpc-web trailer already carries.
    """
    pkg = types.ModuleType("grpc_status")
    pkg.__pcw_shim__ = True
    pkg.__version__ = "1.76.0"

    rpc_status = types.ModuleType("grpc_status.rpc_status")
    rpc_status.__pcw_shim__ = True

    def from_call(_call):
        return None

    def to_status(_status):
        raise _ShimError("grpc_status.to_status is unavailable without grpcio")

    rpc_status.from_call = from_call
    rpc_status.to_status = to_status
    pkg.rpc_status = rpc_status
    return pkg, rpc_status


def install_grpc_shim() -> bool:
    """Register stubs for ``grpc`` (+ ``grpc_status``) iff real grpcio is absent.

    Returns True if the shim was installed, False if real grpcio is present
    (or the shim was already installed). Idempotent. Note: ``google.rpc``
    (googleapis-common-protos) is pure Python and is a *real* dependency — it is
    never shimmed.
    """
    existing = sys.modules.get("grpc")
    if existing is not None:
        return bool(getattr(existing, "__pcw_shim__", False))
    # Detect real grpcio WITHOUT a literal `import grpc` (which the CI grpcio
    # guard, and DECISIONS.md #1, forbid in this package). find_spec resolves to
    # None when grpcio is absent (Pyodide); never shadow a real install.
    try:
        real = importlib.util.find_spec("grpc") is not None
    except Exception:
        real = False
    if real:
        return False
    sys.modules["grpc"] = _build_module()
    pkg, rpc_status = _build_grpc_status_modules()
    sys.modules["grpc_status"] = pkg
    sys.modules["grpc_status.rpc_status"] = rpc_status
    return True
