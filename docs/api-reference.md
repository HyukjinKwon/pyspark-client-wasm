<!-- SPDX-License-Identifier: Apache-2.0 -->

# API reference

The public surface of `pyspark_connect_web` is intentionally tiny: you
`install()` the patch once, then use ordinary PySpark. Everything else
(DataFrame, Column, functions, `SparkSession`) is upstream PySpark, documented by
the [PySpark project](https://spark.apache.org/docs/latest/api/python/).

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

All names below are importable both from the top-level package
(`pyspark_connect_web`) and from `pyspark_connect_web.patch`.

## Functions

### `install() -> None`

Monkey-patch `pyspark.sql.connect` so the Connect client speaks grpc-web over the
blocking browser bridge instead of grpcio. **Idempotent** — calling it more than
once is a no-op. Raises [`UnsupportedPySparkError`](#unsupportedpysparkerror) if
the installed PySpark is outside [`SUPPORTED_PYSPARK_RANGE`](#supported_pyspark_range).

### `uninstall() -> None`

Restore the original (unpatched) `pyspark.sql.connect` symbols. Idempotent.

### `is_installed() -> bool`

Return `True` if the patch is currently installed.

### `set_stub_factory(factory: Optional[StubFactory]) -> None`

Override the factory that builds the gRPC-service stub the patched client uses.
Pass `None` to restore the default (the lane-1 grpc-web stub). Primarily a
pluggable hook for wiring an alternate transport or injecting a fake in tests.

### `set_channel_factory(factory: Optional[ChannelFactory]) -> None`

Override the factory that builds the [`SyncChannel`](#protocols-transport-seam)
backing the stub. Pass `None` to restore the default (the lane-3 SAB bridge
channel). Used to inject a fake/loopback channel in tests and integration harnesses.

### `check_pyspark_version(version: Optional[str] = None) -> tuple[int, int]`

Validate the PySpark version (defaults to the installed one) against
[`SUPPORTED_PYSPARK_RANGE`](#supported_pyspark_range), returning `(major, minor)`.
Raises [`UnsupportedPySparkError`](#unsupportedpysparkerror) if out of range.

## Constants

### `SUPPORTED_PYSPARK_RANGE`

`str` — the supported PySpark version specifier, currently `">=4.0,<4.2"`.

### `__version__`

`str` — the installed `pyspark-connect-web` version.

## Exceptions

### `UnsupportedPySparkError`

Subclass of `RuntimeError`, raised by `install()` / `check_pyspark_version()`
when the running PySpark is outside the supported range.

## Protocols (transport seam)

These describe the seam the factories above plug into; see
[Architecture](architecture.md) and `API_CONTRACT.md` for the full contract.

- **`SyncChannel`** — a blocking byte transport with `unary(...)` and
  `server_stream(...)` methods (implemented by the lane-3 SAB bridge in the
  browser, or by a loopback in tests).
- **`StubFactory` / `ChannelFactory`** — callables that build the service stub
  and the `SyncChannel`, respectively.
