# SPDX-License-Identifier: Apache-2.0
"""Lane 2: the monkey-patch that retargets PySpark Connect at a grpc-web transport.

The whole project hangs off one idea (see ``/API_CONTRACT.md``): PySpark's
Connect client is pure Python above a gRPC *stub*. We replace only that stub
(and teach the connection parser a web scheme); nothing upstream of the stub
-- DataFrame, Column, functions, plan building, the reattachable iterator --
is touched. We **patch, we do not fork** (``DECISIONS.md`` #2).

What ``install()`` does, in order:

1. **Version-guard** the running ``pyspark`` to the range in ``DECISIONS.md`` #3
   (``>=4.0,<4.2``) and raise a clear error otherwise. The seam we patch is
   private (``DefaultChannelBuilder.toChannel``,
   ``base_pb2_grpc.SparkConnectServiceStub`` construction); the guard is what
   lets us depend on those internals safely.

2. **Teach the connection parser the web scheme.** We wrap
   ``DefaultChannelBuilder.__init__`` so it accepts
   ``sc://host:port/;transport=grpcweb`` (and an ``https://host`` shorthand)
   in addition to the stock ``sc://`` URLs. The result is a normal
   ``SparkSession`` -- ``SparkSession.builder.remote(...)`` is unchanged.

3. **Swap the channel + stub.** We wrap ``DefaultChannelBuilder.toChannel`` so
   that, for a web endpoint, it returns a lightweight :class:`WebChannel`
   marker (carrying host/port/secure + a lane-3 ``SyncChannel``) instead of
   calling ``grpc.*`` (which would import ``grpcio`` -- forbidden, ``DECISIONS``
   #1). We then replace ``base_pb2_grpc.SparkConnectServiceStub`` with a
   factory that, given a ``WebChannel``, returns lane 1's ``GrpcWebStub`` and,
   given anything else, falls back to the original stub. Both
   ``SparkConnectClient.__init__`` and ``ArtifactManager.__init__`` construct
   the stub via ``grpc_lib.SparkConnectServiceStub(channel)`` where
   ``grpc_lib`` is the *module* ``base_pb2_grpc`` -- so patching that one
   attribute covers both call sites.

The lane-1 stub factory and the lane-3 channel factory are **pluggable hooks**
(:func:`set_stub_factory`, :func:`set_channel_factory`) with lazy-importing
defaults, so this module has no hard import cycle with lanes 1/3 and stays
importable even before they land.
"""
from __future__ import annotations

import re
import sys
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from ._contract import SyncChannel

# ---------------------------------------------------------------------------
# Supported pyspark range (mirrors DECISIONS.md #3). Inclusive lower, exclusive
# upper, by (major, minor).
# ---------------------------------------------------------------------------
SUPPORTED_PYSPARK_MIN: Tuple[int, int] = (4, 0)
SUPPORTED_PYSPARK_MAX_EXCLUSIVE: Tuple[int, int] = (4, 2)
SUPPORTED_PYSPARK_RANGE = ">=4.0,<4.2"

#: The grpc-web transport marker used in the connection string params.
WEB_TRANSPORT = "grpcweb"
#: Connection-string param key that selects the web transport.
PARAM_TRANSPORT = "transport"


class UnsupportedPySparkError(RuntimeError):
    """Raised by :func:`install` when the running pyspark is out of range."""


# ---------------------------------------------------------------------------
# Version guard
# ---------------------------------------------------------------------------
def _parse_major_minor(version: str) -> Tuple[int, int]:
    """Parse the leading ``major.minor`` out of a pyspark version string.

    Tolerates suffixes like ``4.0.0``, ``4.1.0.dev0``, ``4.0.0+abc``.
    """
    m = re.match(r"^\s*(\d+)\.(\d+)", version)
    if not m:
        raise UnsupportedPySparkError(
            f"could not parse pyspark version {version!r}; "
            f"pyspark-connect-web supports pyspark {SUPPORTED_PYSPARK_RANGE}."
        )
    return int(m.group(1)), int(m.group(2))


def check_pyspark_version(version: Optional[str] = None) -> Tuple[int, int]:
    """Verify the running pyspark is in the supported range, else raise.

    Returns the parsed ``(major, minor)`` on success.
    """
    if version is None:
        try:
            import pyspark

            version = pyspark.__version__
        except Exception as e:  # pragma: no cover - pyspark missing entirely
            raise UnsupportedPySparkError(
                "pyspark is not importable; pyspark-connect-web requires "
                f"pyspark {SUPPORTED_PYSPARK_RANGE}."
            ) from e

    mm = _parse_major_minor(version)
    if not (SUPPORTED_PYSPARK_MIN <= mm < SUPPORTED_PYSPARK_MAX_EXCLUSIVE):
        raise UnsupportedPySparkError(
            f"pyspark {version} is not supported by pyspark-connect-web "
            f"(requires {SUPPORTED_PYSPARK_RANGE}). The patch depends on private "
            f"internals of SparkConnectClient/DefaultChannelBuilder that are only "
            f"pinned for that range."
        )
    return mm


# ---------------------------------------------------------------------------
# The web "channel" marker. This is what our patched toChannel() returns in
# place of a real grpc.Channel. It is duck-typed: PySpark only ever passes it
# back to the (patched) stub factory and calls .close() on it.
# ---------------------------------------------------------------------------
@dataclass
class WebChannel:
    """Lightweight stand-in for ``grpc.Channel`` for the grpc-web transport.

    Carries everything the stub factory needs to reach the server. ``channel``
    is lane 3's blocking :class:`SyncChannel`; ``metadata`` is the list of
    ``(key, value)`` header pairs PySpark's ``ChannelBuilder.metadata()`` would
    inject (the stub forwards per-call metadata, but the channel-level pairs
    such as auth headers live here too).
    """

    host: str
    port: int
    secure: bool
    channel: SyncChannel
    params: Dict[str, str] = field(default_factory=dict)
    _closed: bool = False

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def base_url(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}"

    def close(self) -> None:
        # PySpark's SparkConnectClient.close() calls self._channel.close().
        self._closed = True
        close = getattr(self.channel, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Pluggable hooks (lane 3 channel + lane 1 stub). Defaults lazily import the
# sibling lanes so this module has no import cycle and stays importable before
# lanes 1/3 land.
# ---------------------------------------------------------------------------
# A channel factory turns a parsed endpoint into a lane-3 SyncChannel.
ChannelFactory = Callable[["WebEndpoint"], SyncChannel]
# A stub factory turns a WebChannel into a duck-typed SparkConnectServiceStub.
StubFactory = Callable[[WebChannel], Any]

_channel_factory: Optional[ChannelFactory] = None
_stub_factory: Optional[StubFactory] = None


@dataclass
class WebEndpoint:
    """Parsed web endpoint handed to the channel factory."""

    host: str
    port: int
    secure: bool
    params: Dict[str, str] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}"


def set_channel_factory(factory: Optional[ChannelFactory]) -> None:
    """Override the lane-3 ``SyncChannel`` factory (e.g. inject a fake in tests).

    Pass ``None`` to restore the lazy default.
    """
    global _channel_factory
    _channel_factory = factory


def set_stub_factory(factory: Optional[StubFactory]) -> None:
    """Override the lane-1 stub factory (e.g. inject a fake in tests).

    Pass ``None`` to restore the lazy default.
    """
    global _stub_factory
    _stub_factory = factory


def _default_channel_factory(endpoint: WebEndpoint) -> SyncChannel:
    """Lazily build lane 3's blocking ``SyncChannel`` for ``endpoint``.

    Imported lazily to avoid a hard cycle with lane 3 and to keep ``install()``
    importable before lane 3 lands. Raises a clear, deferred error if lane 3's
    channel is not available at *connect* time (not at install time).
    """
    try:
        from .worker import SabSyncChannel
    except Exception as e:  # lane 3 not present yet
        raise RuntimeError(
            "pyspark-connect-web: no SyncChannel available. The default "
            "transport is lane 3's worker.SabSyncChannel, which is not "
            "importable here. Install lane 3, run in Pyodide, or inject one via "
            "pyspark_connect_web.patch.set_channel_factory(...)."
        ) from e
    # Lane 3 owns the exact constructor; we pass the base URL it needs to fetch.
    return SabSyncChannel(base_url=endpoint.base_url)  # type: ignore[call-arg]


def _default_stub_factory(channel: WebChannel) -> Any:
    """Lazily build lane 1's ``GrpcWebStub`` over the channel's ``SyncChannel``.

    The exact call shape -- ``GrpcWebStub(sync_channel, metadata=...)`` -- is
    documented in ``team/findings-lane2-patch.md`` so lane 1 conforms.
    """
    try:
        from .transport import GrpcWebStub
    except Exception as e:  # lane 1 not present yet
        raise RuntimeError(
            "pyspark-connect-web: lane 1's transport.GrpcWebStub is not "
            "importable. Install lane 1 or inject a stub via "
            "pyspark_connect_web.patch.set_stub_factory(...)."
        ) from e
    return GrpcWebStub(channel.channel, metadata=list(channel.params.items()))  # type: ignore[call-arg]


def _get_channel_factory() -> ChannelFactory:
    return _channel_factory or _default_channel_factory


def _get_stub_factory() -> StubFactory:
    return _stub_factory or _default_stub_factory


# ---------------------------------------------------------------------------
# Connection-string parsing for the web scheme.
# ---------------------------------------------------------------------------
def _is_web_url(url: str) -> bool:
    """True if ``url`` selects the grpc-web transport.

    Recognised forms:
      * ``https://host[:port]``                       (shorthand, secure)
      * ``http://host[:port]``                        (shorthand, insecure)
      * ``sc://host[:port]/;transport=grpcweb[;...]``  (canonical)
    """
    if url.startswith("https://") or url.startswith("http://"):
        return True
    if url.startswith("sc://"):
        # cheap scan for transport=grpcweb in the params section
        return bool(re.search(rf";{PARAM_TRANSPORT}=\s*{WEB_TRANSPORT}\b", url))
    return False


def _normalize_web_url(url: str) -> str:
    """Rewrite an ``http(s)://`` shorthand into a canonical ``sc://`` URL.

    ``https://host`` -> ``sc://host:443/;transport=grpcweb;use_ssl=true``
    ``http://host``  -> ``sc://host:80/;transport=grpcweb``
    A canonical ``sc://...;transport=grpcweb`` URL is returned unchanged.
    """
    if url.startswith("sc://"):
        return url

    secure = url.startswith("https://")
    rest = url.split("://", 1)[1].rstrip("/")
    # split host[:port]
    if ":" in rest:
        host, port_s = rest.rsplit(":", 1)
        port = port_s
    else:
        host = rest
        port = "443" if secure else "80"
    params = [f"{PARAM_TRANSPORT}={WEB_TRANSPORT}"]
    if secure:
        params.append("use_ssl=true")
    return f"sc://{host}:{port}/;" + ";".join(params)


# ---------------------------------------------------------------------------
# The actual monkey-patch. We keep originals for idempotency + uninstall.
# ---------------------------------------------------------------------------
_LOCK = threading.RLock()
_INSTALLED = False
_ORIG: Dict[str, Any] = {}


def is_installed() -> bool:
    return _INSTALLED


class _SyncExecutor:
    """A ThreadPoolExecutor stand-in that runs submitted callables inline.

    Pyodide cannot start OS threads; pyspark's reattach release pool would raise
    "can't start new thread". ReleaseExecute is best-effort cleanup, so running
    it synchronously is correct (the SAB bridge serializes RPCs regardless)."""

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        # NO-OP. pyspark submits ReleaseExecute here mid-ExecutePlan-stream.
        # Running it inline (Pyodide has no threads) would re-enter the single
        # SAB channel while the stream's Atomics state machine is active and
        # deadlock. ReleaseExecute is best-effort server cleanup: the result is
        # already received, and the server frees buffers on session/operation
        # end, so skipping it is safe and keeps .collect() unblocked.
        from concurrent.futures import Future

        fut: "Future[Any]" = Future()
        fut.set_result(None)
        return fut

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass

    # Some pyspark versions poke ThreadPoolExecutor internals (e.g.
    # reattach.py reads ``_shutdown``); expose a benign attribute.
    _shutdown = False


def install() -> None:
    """Monkey-patch pyspark.sql.connect to use the grpc-web transport.

    Idempotent: calling it more than once is a no-op. Raises
    :class:`UnsupportedPySparkError` if the running pyspark is out of the
    supported range.
    """
    global _INSTALLED
    with _LOCK:
        if _INSTALLED:
            return

        # (a) version guard -- before importing the internals we patch.
        check_pyspark_version()

        import pyspark.sql.connect.client.core as core
        import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib

        DefaultChannelBuilder = core.DefaultChannelBuilder

        # Save originals.
        _ORIG["DCB_init"] = DefaultChannelBuilder.__init__
        _ORIG["DCB_toChannel"] = DefaultChannelBuilder.toChannel
        _ORIG["stub_cls"] = grpc_lib.SparkConnectServiceStub
        _ORIG["grpc_lib"] = grpc_lib
        _ORIG["DefaultChannelBuilder"] = DefaultChannelBuilder

        orig_init = _ORIG["DCB_init"]
        orig_to_channel = _ORIG["DCB_toChannel"]
        orig_stub_cls = _ORIG["stub_cls"]

        # (b) teach the parser the web scheme.
        def patched_init(
            self: Any,
            url: str,
            channelOptions: Optional[List[Tuple[str, Any]]] = None,
        ) -> None:
            if isinstance(url, str) and _is_web_url(url):
                normalized = _normalize_web_url(url)
                orig_init(self, normalized, channelOptions)
                # Mark this builder as web so toChannel() routes to grpc-web.
                self._pcw_web = True  # type: ignore[attr-defined]
            else:
                orig_init(self, url, channelOptions)
                self._pcw_web = False  # type: ignore[attr-defined]

        # (c) route toChannel() for web builders to a WebChannel marker.
        def patched_to_channel(self: Any) -> Any:
            if not getattr(self, "_pcw_web", False):
                return orig_to_channel(self)
            endpoint = WebEndpoint(
                host=self.host,
                port=self._port,
                secure=self.secure,
                params=dict(self._params),
            )
            sync_channel = _get_channel_factory()(endpoint)
            # Channel-level metadata = what ChannelBuilder.metadata() would inject
            # (everything except the channel-private params).
            meta = dict(self.metadata())
            return WebChannel(
                host=endpoint.host,
                port=endpoint.port,
                secure=endpoint.secure,
                channel=sync_channel,
                params=meta,
            )

        # (d) stub factory: WebChannel -> lane 1 stub; anything else -> original.
        def patched_stub_factory(channel: Any, *args: Any, **kwargs: Any) -> Any:
            if isinstance(channel, WebChannel):
                return _get_stub_factory()(channel)
            return orig_stub_cls(channel, *args, **kwargs)

        DefaultChannelBuilder.__init__ = patched_init  # type: ignore[assignment]
        DefaultChannelBuilder.toChannel = patched_to_channel  # type: ignore[assignment]
        grpc_lib.SparkConnectServiceStub = patched_stub_factory  # type: ignore[assignment]

        # (e) Pyodide is single-threaded: pyspark's reattachable iterator sends
        # ReleaseExecute via a ThreadPoolExecutor -> "can't start new thread".
        # Swap it for a no-op synchronous executor. ONLY under Pyodide
        # (sys.platform == "emscripten"): on real CPython (local dev, the CI
        # integration job) threads work, so we must leave the real pool in place
        # - replacing it breaks pyspark versions that poke its internals.
        if sys.platform == "emscripten":
            _patch_reattach_pool()

        _INSTALLED = True


def _patch_reattach_pool() -> None:
    """Swap pyspark's reattach ReleaseExecute pool for a no-op (Pyodide only)."""
    try:
        import pyspark.sql.connect.client.reattach as reattach

        it_cls = reattach.ExecutePlanResponseReattachableIterator
        _ORIG["reattach_cls"] = it_cls
        _ORIG["reattach_pool_attr"] = it_cls.__dict__.get(
            "_get_or_create_release_thread_pool"
        )

        def _sync_pool(cls: Any) -> Any:
            if cls._release_thread_pool_instance is None:
                cls._release_thread_pool_instance = _SyncExecutor()
            return cls._release_thread_pool_instance

        it_cls._get_or_create_release_thread_pool = classmethod(  # type: ignore[assignment]
            _sync_pool
        )
        it_cls._release_thread_pool_instance = None
    except Exception:  # pragma: no cover - reattach should always import
        pass


def uninstall() -> None:
    """Restore pyspark to its un-patched state. Mainly for tests. Idempotent."""
    global _INSTALLED
    with _LOCK:
        if not _INSTALLED:
            return
        DefaultChannelBuilder = _ORIG["DefaultChannelBuilder"]
        DefaultChannelBuilder.__init__ = _ORIG["DCB_init"]
        DefaultChannelBuilder.toChannel = _ORIG["DCB_toChannel"]
        _ORIG["grpc_lib"].SparkConnectServiceStub = _ORIG["stub_cls"]
        # Restore the reattach release-pool factory (if we patched it).
        reattach_cls = _ORIG.get("reattach_cls")
        if reattach_cls is not None:
            orig_attr = _ORIG.get("reattach_pool_attr")
            if orig_attr is not None:
                reattach_cls._get_or_create_release_thread_pool = orig_attr
            reattach_cls._release_thread_pool_instance = None
        _ORIG.clear()
        _INSTALLED = False
