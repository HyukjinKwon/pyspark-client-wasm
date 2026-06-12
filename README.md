<!-- SPDX-License-Identifier: Apache-2.0 -->

# pyspark-connect-web — PySpark in JupyterLite

Run the **real** PySpark Connect Python client inside a browser
(JupyterLite/Pyodide), talking to a Spark Connect server through a grpc-web
transport. Your existing PySpark code runs unchanged — no reimplementation, no
local JVM, no Python backend server.

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()   # runs in your browser tab
```

## How it works
PySpark's Connect client is pure Python above a single gRPC stub: it builds
protobuf plans and ships them to the server. We **monkey-patch only that stub**
with a grpc-web/`fetch` transport, and make calls blocking via a Web Worker +
`Atomics`/`SharedArrayBuffer` bridge so `.collect()` returns data synchronously.
Everything above the stub — DataFrame, Column, functions — is untouched.

**This is a thin client, not local compute.** You still need a running Spark
Connect server (Spark 4.x) behind an Envoy grpc-web proxy. The win is: no Python
*backend*, real PySpark API, anywhere a browser runs.

## Quickstart

### 1. Bring up the server side (Spark Connect + Envoy grpc-web proxy)

```bash
docker compose -f deploy/compose.yaml up
```

This starts a Spark 4.0.0 Connect server and an Envoy proxy that exposes:

| URL | What |
|-----|------|
| `sc://localhost:8081/;transport=grpcweb` | grpc-web endpoint the client connects to |
| <http://localhost:8000/> | JupyterLite site, served with the mandatory `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp` headers (required for `SharedArrayBuffer`) |

See [`deploy/README.md`](deploy/README.md) for ports, version pins, and CORS/header checks.

### 2. Use the client

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()
```

In JupyterLite, open <http://localhost:8000/> and run the demo notebook. Verify
isolation first — `crossOriginIsolated === true` in the console — or the
blocking bridge cannot work.

Full walkthrough (reference generation, e2e, troubleshooting):
[`docs/running-locally.md`](docs/running-locally.md). Architecture:
[`docs/architecture.md`](docs/architecture.md).

## Status
Early development. The server side (`deploy/`) and the e2e scaffold
(`tests/e2e/`) are in place; the browser client (lanes 1–4) and the JupyterLite
build (lane 3) are in progress. See `COORDINATION.md` for the build plan and
`DECISIONS.md` for invariants. Built by a team of cooperating agents — lane map
in `COORDINATION.md`.
