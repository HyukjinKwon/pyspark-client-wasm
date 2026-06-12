# SPDX-License-Identifier: Apache-2.0
"""Lane 2 guard tests for the monkey-patch + integration layer.

These tests stub the stub and the channel with fakes -- they do NOT
require a real Spark Connect server, ``grpcio``, or a browser. They assert:

* ``install()`` is idempotent.
* ``install()`` raises on an unsupported pyspark version (version monkeypatched).
* after ``install()`` the connection parser accepts the web scheme
  (``sc://...;transport=grpcweb`` and the ``https://`` shorthand), and
  ``SparkSession.builder.remote(...)`` builds a session whose stub is our fake.
* ``base_pb2_grpc.SparkConnectServiceStub`` still builds the original stub for a
  non-web (real grpc) channel -- i.e. we don't break the stock path.
"""
from __future__ import annotations

import pytest

import pyspark_connect_web as pcw
from pyspark_connect_web import patch as pcw_patch
from pyspark_connect_web._contract import HttpResponse


# ---------------------------------------------------------------------------
# Fakes for the components and 3 (no grpcio, no browser, no server).
# ---------------------------------------------------------------------------
class FakeSyncChannel:
    """Stand-in for the SabSyncChannel -- implements the SyncChannel proto."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.closed = False

    def unary(self, path, body, headers, timeout):
        return HttpResponse(status=200, headers={}, body=b"")

    def server_stream(self, path, body, headers, timeout):
        return iter(())

    def close(self):
        self.closed = True


class FakeStub:
    """Stand-in for the GrpcWebStub -- just records what it was built with."""

    def __init__(self, channel, metadata=None):
        self.channel = channel
        self.metadata = metadata


@pytest.fixture
def fakes():
    """Install fake the stub + the channel factories, restore afterwards."""
    pcw_patch.set_channel_factory(lambda ep: FakeSyncChannel(ep.base_url))
    pcw_patch.set_stub_factory(lambda ch: FakeStub(ch.channel, list(ch.params.items())))
    try:
        yield
    finally:
        pcw_patch.set_channel_factory(None)
        pcw_patch.set_stub_factory(None)


@pytest.fixture(autouse=True)
def clean_patch_state():
    """Ensure each test starts and ends un-patched."""
    if pcw.is_installed():
        pcw.uninstall()
    yield
    if pcw.is_installed():
        pcw.uninstall()


# ---------------------------------------------------------------------------
# Version guard
# ---------------------------------------------------------------------------
def test_version_parsing():
    assert pcw_patch._parse_major_minor("4.0.0") == (4, 0)
    assert pcw_patch._parse_major_minor("4.1.0.dev0") == (4, 1)
    assert pcw_patch._parse_major_minor("4.0.0+abc") == (4, 0)


def test_check_version_accepts_in_range():
    assert pcw_patch.check_pyspark_version("4.0.0") == (4, 0)
    assert pcw_patch.check_pyspark_version("4.1.5") == (4, 1)


@pytest.mark.parametrize("bad", ["3.5.1", "4.2.0", "5.0.0", "4.2.0.dev0"])
def test_check_version_rejects_out_of_range(bad):
    with pytest.raises(pcw_patch.UnsupportedPySparkError):
        pcw_patch.check_pyspark_version(bad)


def test_check_version_rejects_garbage():
    with pytest.raises(pcw_patch.UnsupportedPySparkError):
        pcw_patch.check_pyspark_version("not-a-version")


def test_install_raises_on_unsupported_version(monkeypatch):
    """Monkeypatch the reported pyspark version out of range; install() must raise."""
    import pyspark

    monkeypatch.setattr(pyspark, "__version__", "3.5.1", raising=False)
    assert not pcw.is_installed()
    with pytest.raises(pcw.UnsupportedPySparkError):
        pcw.install()
    # A failed guard must not flip the installed flag.
    assert not pcw.is_installed()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------
def test_install_idempotent(fakes):
    import pyspark.sql.connect.client.core as core

    assert not pcw.is_installed()
    pcw.install()
    assert pcw.is_installed()
    patched_init = core.DefaultChannelBuilder.__init__
    patched_to_channel = core.DefaultChannelBuilder.toChannel

    # Second install() is a no-op: same patched functions, no double-wrapping.
    pcw.install()
    assert pcw.is_installed()
    assert core.DefaultChannelBuilder.__init__ is patched_init
    assert core.DefaultChannelBuilder.toChannel is patched_to_channel


def test_uninstall_restores_originals(fakes):
    import pyspark.sql.connect.client.core as core
    import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib

    orig_init = core.DefaultChannelBuilder.__init__
    orig_to_channel = core.DefaultChannelBuilder.toChannel
    orig_stub = grpc_lib.SparkConnectServiceStub

    pcw.install()
    assert core.DefaultChannelBuilder.__init__ is not orig_init

    pcw.uninstall()
    assert not pcw.is_installed()
    assert core.DefaultChannelBuilder.__init__ is orig_init
    assert core.DefaultChannelBuilder.toChannel is orig_to_channel
    assert grpc_lib.SparkConnectServiceStub is orig_stub


# ---------------------------------------------------------------------------
# Web-scheme connection parsing
# ---------------------------------------------------------------------------
def test_is_web_url():
    assert pcw_patch._is_web_url("sc://h:8081/;transport=grpcweb")
    assert pcw_patch._is_web_url("https://h")
    assert pcw_patch._is_web_url("http://h:8080")
    # plain sc:// without the transport param is NOT a web url
    assert not pcw_patch._is_web_url("sc://localhost:15002")
    assert not pcw_patch._is_web_url("sc://localhost/;use_ssl=true")


def test_normalize_https_shorthand():
    assert (
        pcw_patch._normalize_web_url("https://h")
        == "sc://h:443/;transport=grpcweb;use_ssl=true"
    )
    assert (
        pcw_patch._normalize_web_url("http://h:8080")
        == "sc://h:8080/;transport=grpcweb"
    )
    # canonical url passes through unchanged
    canonical = "sc://h:8081/;transport=grpcweb"
    assert pcw_patch._normalize_web_url(canonical) == canonical


def test_parser_accepts_web_scheme_canonical(fakes):
    import pyspark.sql.connect.client.core as core

    pcw.install()
    builder = core.DefaultChannelBuilder("sc://localhost:8081/;transport=grpcweb")
    assert builder.host == "localhost"
    assert builder._port == 8081
    assert getattr(builder, "_pcw_web") is True

    channel = builder.toChannel()
    assert isinstance(channel, pcw_patch.WebChannel)
    assert channel.endpoint == "localhost:8081"
    assert isinstance(channel.channel, FakeSyncChannel)
    assert channel.base_url == "http://localhost:8081"


def test_parser_accepts_https_shorthand(fakes):
    import pyspark.sql.connect.client.core as core

    pcw.install()
    builder = core.DefaultChannelBuilder("https://example.com")
    assert builder.host == "example.com"
    assert builder._port == 443
    assert builder.secure is True
    channel = builder.toChannel()
    assert isinstance(channel, pcw_patch.WebChannel)
    assert channel.secure is True
    assert channel.base_url == "https://example.com:443"


def test_stub_factory_routes_webchannel_to_fake(fakes):
    import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib

    pcw.install()
    sync = FakeSyncChannel("http://h:1")
    web = pcw_patch.WebChannel(host="h", port=1, secure=False, channel=sync)
    stub = grpc_lib.SparkConnectServiceStub(web)
    assert isinstance(stub, FakeStub)
    assert stub.channel is sync


def test_stub_factory_falls_back_for_non_web_channel(fakes):
    """A non-WebChannel must still build the original (stock) stub."""
    import pyspark.sql.connect.proto.base_pb2_grpc as grpc_lib

    orig_stub_cls = grpc_lib.SparkConnectServiceStub
    pcw.install()

    class FakeGrpcChannel:
        # what grpc.*_channel would return; the stock stub just stores callables
        def unary_unary(self, *a, **k):
            return lambda *a, **k: None

        def unary_stream(self, *a, **k):
            return lambda *a, **k: None

        def stream_unary(self, *a, **k):
            return lambda *a, **k: None

        def stream_stream(self, *a, **k):
            return lambda *a, **k: None

    stub = grpc_lib.SparkConnectServiceStub(FakeGrpcChannel())
    # original stub class is the *un-patched* one captured before install
    assert isinstance(stub, orig_stub_cls)
    assert not isinstance(stub, FakeStub)


def test_full_session_builder_remote_uses_web_transport(fakes):
    """End-to-end through the public API: builder.remote(...) -> our fake stub.

    Exercises the real SparkConnectClient.__init__ path: it parses the URL via
    DefaultChannelBuilder, calls toChannel() (-> WebChannel) and builds the stub
    via the patched factory (-> FakeStub). No server, no grpcio, no browser.
    """
    pcw.install()
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

    session = (
        ConnectSparkSession.builder.remote(
            "sc://localhost:8081/;transport=grpcweb"
        ).create()
    )
    client = session.client
    assert isinstance(client._channel, pcw_patch.WebChannel)
    assert isinstance(client._stub, FakeStub)
    # ArtifactManager shares the same patched factory + channel.
    assert isinstance(client._artifact_manager._stub, FakeStub)
