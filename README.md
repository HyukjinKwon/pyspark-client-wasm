<!-- SPDX-License-Identifier: Apache-2.0 -->

# pyspark-connect-web - PySpark in JupyterLite

[![CI](https://github.com/HyukjinKwon/pyspark-client-wasm/actions/workflows/ci.yml/badge.svg)](https://github.com/HyukjinKwon/pyspark-client-wasm/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pyspark-connect-web.svg)](https://pypi.org/project/pyspark-connect-web/)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://hyukjinkwon.github.io/pyspark-client-wasm/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

Run the **real** PySpark Connect Python client inside a browser
(JupyterLite/Pyodide), talking to an Apache Spark Connect server through a
grpc-web transport. Your existing PySpark code runs unchanged - no
reimplementation, no local JVM, no Python backend server.

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()   # runs in your browser tab
```

**This is a thin client, not local compute.** You still need a running Spark
Connect server (Spark 4.x) behind an Envoy grpc-web proxy. The win is: no Python
*backend*, the real PySpark API, anywhere a browser runs.

## How it works

PySpark's Connect client is pure Python above a single gRPC stub: it builds
protobuf plans and ships them to the server. We **monkey-patch only that stub**
with a grpc-web/`fetch` transport, and make calls blocking via a Web Worker +
`Atomics`/`SharedArrayBuffer` bridge so `.collect()` returns data synchronously.
Everything above the stub - DataFrame, Column, functions - is untouched. We
patch; we do not fork PySpark. See [`docs/architecture.md`](docs/architecture.md).

## Requirements

* A browser (for the client) **or** Python 3.11+ (for local dev/tests).
* In the browser: Pyodide >= 0.28 (Python 3.13), which already ships
  `pyarrow`, `pandas`, `protobuf`, and `numpy`. **`grpcio` is not available in
  Pyodide and is never imported** - all transport is grpc-web over `fetch`.
* `pyspark>=4.0,<4.2` (pinned by `install()`; provided by Pyodide in the browser).
* A running Spark Connect server (Spark 4.x) behind an Envoy grpc-web proxy -
  the [`deploy/`](deploy/) stack brings this up for you.
* The JupyterLite page must be **cross-origin isolated** (`COOP: same-origin` +
  `COEP: require-corp`), which the deploy stack serves for you. Without it,
  `SharedArrayBuffer` - the backbone of the blocking bridge - is unavailable.

## Installation

Use a **conda** environment:

```bash
conda create -n pcw python=3.11
conda activate pcw
pip install pyspark-connect-web
```

In the browser (JupyterLite/Pyodide), install with `micropip` inside the kernel:

```python
import micropip
await micropip.install("pyspark-connect-web")
```

> The **import / package name** is `pyspark_connect_web` (distribution name
> `pyspark-connect-web`); the **repository** is `pyspark-client-wasm`.

## Running a local Spark Connect server

The [`deploy/`](deploy/) stack brings up a Spark 4.0.0 Connect server, an Envoy
`grpc_web` proxy, and a static host for the JupyterLite site with the mandatory
cross-origin-isolation headers:

```bash
docker compose -f deploy/compose.yaml up
# wait for the "spark-connect" container to report healthy (~60s cold start)
```

This exposes:

| URL | What |
|-----|------|
| `sc://localhost:8081/;transport=grpcweb` | grpc-web endpoint the client connects to |
| <http://localhost:8000/> | JupyterLite site, served with `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp` (required for `SharedArrayBuffer`) |
| `:15002` | Spark Connect raw gRPC (native clients / reference generator) |

See [`deploy/README.md`](deploy/README.md) for ports, version pins, and
CORS/header `curl` checks, and [`docs/running-locally.md`](docs/running-locally.md)
for the full walkthrough.

## Connecting

Point the client at the grpc-web proxy after `install()`:

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

The connection string is the standard Spark Connect `sc://` URI with a
`transport=grpcweb` parameter. A plain `https://`/`http://` shorthand is also
accepted. For anything past localhost, terminate **TLS** at the proxy and use a
secure context - a browser needs HTTPS for `crossOriginIsolated` off localhost.

For **TLS + auth** (the hardened prod overlay), the proxy is the enforcement
point: it gates on `Authorization: Bearer <token>` and forwards the header
upstream. Bring it up with:

```bash
# provide a TLS cert (deploy/certs/), set your origins, then:
docker compose -f deploy/compose.yaml -f deploy/compose.prod.yaml up -d
# or: make up-prod
```

See [`docs/connection-patterns.md`](docs/connection-patterns.md) and
[`deploy/README.md`](deploy/README.md) (TLS, CORS allowlist, bearer-token gate ->
`jwt_authn`/`ext_authz`).

## A quick tour

Runnable scripts live in [`examples/`](examples/). Run any of them against the
deploy stack (see [`examples/README.md`](examples/README.md)):

```bash
conda activate pcw
python examples/quickstart.py
```

| Example | Shows |
|---------|-------|
| [`quickstart.py`](examples/quickstart.py) | `install()`, connect, `range`/`filter`, `toPandas` |
| [`transformations.py`](examples/transformations.py) | `select`, `withColumn`, `filter`, `orderBy`, `functions` |
| [`aggregations.py`](examples/aggregations.py) | `groupBy`/`agg`, `count`, `avg`, `sum` |
| [`joins.py`](examples/joins.py) | inner/left/semi joins across DataFrames |
| [`window.py`](examples/window.py) | `Window` partition/order, `row_number`, `rank`, running totals |
| [`sql.py`](examples/sql.py) | temp views, `spark.sql(...)`, parameterized SQL |
| [`io.py`](examples/io.py) | `createDataFrame`, `printSchema`, read/write Parquet/JSON |

## Documentation

Full docs: <https://hyukjinkwon.github.io/pyspark-client-wasm/>

* [Architecture](docs/architecture.md) - the stub seam, the sync bridge, the wire framing.
* [Quickstart](docs/quickstart.md) and [Running locally](docs/running-locally.md).
* [Connection patterns](docs/connection-patterns.md) - `sc://` URIs, TLS, auth.
* [Installation](docs/installation.md) and [JupyterLite hosting](docs/jupyterlite-hosting.md).
* [Packaging & release](docs/packaging-release.md).
* [Security](docs/security.md) - threat model (cross-origin isolation, CORS, auth, untrusted server, notebook XSS).

## Compatibility

| Component | Supported |
|-----------|-----------|
| PySpark | `>=4.0,<4.2` (Connect default; reattachable execute present). `install()` raises outside the range. |
| Spark Connect server | Spark 4.x (`apache/spark:4.0.0` in the deploy stack) |
| Pyodide | >= 0.28 (Python 3.13) in the browser; Python 3.11+ for local dev |
| Proxy | Envoy with `envoy.filters.http.grpc_web` (`v1.31-latest`) |

The v0 target is full read-path parity - `range/select/filter/groupBy/agg`,
`toPandas`, `createDataFrame`, and `spark.sql(...)` - returning results
byte/row-exact versus a native Spark Connect run. See [`DECISIONS.md`](DECISIONS.md).

## Development

```bash
conda create -n pcw python=3.11 && conda activate pcw
pip install -e ".[dev]"
pytest -q
```

Unit tests stub the transport: they **never import `grpcio`** and never touch a
browser. `grpcio` is not available in Pyodide, so the package registers a
lightweight gRPC shim (`pyspark_connect_web/_grpc_shim.py`) before PySpark is
imported; CI fails if `grpcio` is imported anywhere under
`pyspark_connect_web/` (DECISIONS.md #1).

Build the JupyterLite site (produces `_output/` served on `:8000`):

```bash
make site          # or: scripts/build_site.sh
```

Browser end-to-end tests run under Playwright against the deploy stack; see
[`docs/running-locally.md`](docs/running-locally.md). Contribution workflow and
the lane/coordination model: [`CONTRIBUTING.md`](CONTRIBUTING.md) and
[`COORDINATION.md`](COORDINATION.md).
