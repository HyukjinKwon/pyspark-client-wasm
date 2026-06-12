<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings-lane2-patch - the monkey-patch seam

Lane 2 (monkey-patch + integration). Owns `pyspark_connect_web/__init__.py`,
`pyspark_connect_web/patch.py`. We **patch, do not fork** (DECISIONS #2); **no
`grpcio`** (DECISIONS #1).

## pyspark inspected
- **VERIFIED** against `pyspark 4.0.0`, installed at
  `/opt/miniconda3/envs/python3.11/lib/python3.11/site-packages/pyspark`
  (Python 3.11). Inside the pinned range `>=4.0,<4.2` (DECISIONS #3).
- The `4.1.x` line was **not** inspected on disk; symbols below are expected to
  hold across the range and are guarded by the version check. Marked
  **UNVERIFIED-4.1** where relevant. Re-verify when a 4.1 wheel is available.

## Exact private symbols we patch (pin these). All under `pyspark.sql.connect`.

| # | Symbol (4.0.0) | What we do | Why it's the seam |
|---|---|---|---|
| 1 | `proto.base_pb2_grpc.SparkConnectServiceStub` | **Replace** attribute with factory `f(channel, *a, **k)`: a `WebChannel` -> lane-1 stub; else delegate to original class. | Both stub call sites do `import ...base_pb2_grpc as grpc_lib` then `grpc_lib.SparkConnectServiceStub(channel)` at **call time** - patching this one module attribute covers both. |
| 2 | `client.core.DefaultChannelBuilder.__init__(self, url, channelOptions=None)` | **Wrap**: detect web URLs, normalize `http(s)://` -> canonical `sc://...;transport=grpcweb`, call original, set `self._pcw_web`. | Stock `__init__` hard-rejects any non-`sc://` URL (`INVALID_CONNECT_URL`). Must intercept before that for the `https://` shorthand. `;transport=grpcweb` survives the stock param parser (`_extract_attributes` stores unknown k=v in `self._params`). |
| 3 | `client.core.DefaultChannelBuilder.toChannel(self) -> grpc.Channel` | **Wrap**: for `self._pcw_web`, build a `WebChannel` marker via the lane-3 channel factory instead of calling `grpc.*`. Non-web delegates to original. | Stock `toChannel` calls `grpc.insecure_channel`/`grpc.secure_channel` -> imports `grpcio`. Forbidden. This is where we cut over to grpc-web. |

### Stub construction call sites (both covered by symbol #1)
- `client/core.py:673` - `self._internal_stub = grpc_lib.SparkConnectServiceStub(self._channel)` in `SparkConnectClient.__init__`.
- `client/artifact.py:180` - `self._stub = grpc_lib.SparkConnectServiceStub(channel)` in `ArtifactManager.__init__`.
- Both get the channel from `self._builder.toChannel()` (a `WebChannel` for web endpoints), so both get a lane-1 stub. **Verified** by `test_full_session_builder_remote_uses_web_transport`.

### Other relevant internals (read, not patched)
- `SparkConnectClient.__init__` (4.0.0:602): `self._builder = ... DefaultChannelBuilder(connection, channel_options)`, then `self._channel = self._builder.toChannel()`. We do **not** patch `__init__` - patching the builder + stub factory is enough and less brittle.
- `SparkConnectClient._stub` is a property (4.0.0:692) returning `self._internal_stub`, with a test-only setter. Untouched.
- `SparkConnectClient.close()` (4.0.0:1206) calls `self._channel.close()` -> `WebChannel.close()` delegates to the SyncChannel's `close()` if present.
- `ChannelBuilder.metadata()` (4.0.0:227) returns channel-level header pairs (excludes token/use_ssl/user_id/user_agent/session_id). We snapshot this into `WebChannel.params` for the stub factory.
- `session.py:57` imports `from pyspark.sql.connect.client import SparkConnectClient, DefaultChannelBuilder`; `Builder.remote()/create()` -> `SparkSession(connection=url)` -> `SparkConnectClient(connection=url)`. Patching `DefaultChannelBuilder` (the class `session.py` instantiates) is sufficient for `builder.remote(...)`. **UNVERIFIED-4.1** that import path is unchanged.

## Connection scheme (matches API_CONTRACT section 2)
- Canonical: `sc://<host>:<port>/;transport=grpcweb` (extra `;k=v` allowed, e.g. `use_ssl=true`).
- `https://<host>[:port]` -> `sc://<host>:443/;transport=grpcweb;use_ssl=true`.
- `http://<host>[:port]`  -> `sc://<host>:80/;transport=grpcweb`.
- Plain `sc://host:port` **without** `transport=grpcweb` -> stock (grpc) path, untouched.

## Contract I depend on from lanes 1 and 3 (both overridable via set_*_factory)
- **Lane 3 (`worker.SabSyncChannel`)** - default channel factory calls:
  `SabSyncChannel(base_url="http(s)://host:port")`. Must implement the
  `SyncChannel` Protocol from `_contract.py` (+ optional `close()`). If it needs
  more than `base_url`, read from env or have lane 3 call
  `set_channel_factory(...)` in its bootstrap. **OPEN with lane 3.**
- **Lane 1 (`transport.GrpcWebStub`)** - default stub factory calls:
  `GrpcWebStub(sync_channel, metadata=[(k, v), ...])`. `sync_channel` is the
  lane-3 `SyncChannel`; `metadata` is the channel-level header pairs from
  `ChannelBuilder.metadata()`. Stub must expose the 10 callables in API_CONTRACT
  section 1 with `fn(request, *, metadata=None, timeout=None)`. **OPEN with lane 1.**
- If lane 1's constructor differs, change `_default_stub_factory` in `patch.py`
  (one line) - do not contort lane 1 to match us.

## What works (local, no server / grpcio / browser)
- `import pyspark_connect_web; pcw.install()` - idempotent; `uninstall()` restores originals.
- Version guard raises `UnsupportedPySparkError` outside `>=4.0,<4.2`.
- Web-scheme parsing (canonical + `https://`/`http://` shorthand); `toChannel()` -> `WebChannel`.
- `SparkSession.builder.remote("sc://...;transport=grpcweb").create()` builds a real `SparkConnectClient` whose `_stub` and `_artifact_manager._stub` are the (fake) lane-1 stub - verified e2e with fakes.
- 17 guard tests in `tests/test_install.py`, all green. No `grpcio`/`grpc` import in `pyspark_connect_web/`.

## What's blocked / pending other lanes
- Real e2e needs lane 1's `GrpcWebStub` and lane 3's `SabSyncChannel` to land with the shapes above. Until then the default factories raise a clear deferred error **at connect time**, not at `install()` time.
- `.collect()`/reattach parity (DECISIONS #5/#6/#7) is a cross-lane e2e concern (lane 5).

## Contract changes needed
- None to API_CONTRACT.md section 2. **Note for lanes 1/5**: `transport=grpcweb` is
  currently forwarded by `ChannelBuilder.metadata()` as a request header. If
  undesirable on the wire, strip it in lane 1's stub or have the proxy ignore
  it. Flagging rather than changing the contract.
