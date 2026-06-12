<!-- SPDX-License-Identifier: Apache-2.0 -->

# Quickstart

Get from nothing to a query result in a browser tab. This walks the happy path;
for the full local setup (reference generation, e2e, troubleshooting) see
[Running locally](running-locally.md).

## 1. Set up a local Python environment (conda)

```bash
conda create -n pcw python=3.11
conda activate pcw
pip install pyspark-connect-web
```

See [Installation](installation.md) for the browser-side `micropip` install.

## 2. Bring up the server side (Spark Connect + Envoy grpc-web proxy)

```bash
docker compose -f deploy/compose.yaml up
```

This starts a Spark 4.1.2 Connect server and an Envoy proxy that exposes:

| URL | What |
|-----|------|
| `sc://localhost:8081/;transport=grpcweb` | grpc-web endpoint the client connects to |
| <http://localhost:8000/> | JupyterLite site, served with the mandatory `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp` headers (required for `SharedArrayBuffer`) |

See [`deploy/README.md`](https://github.com/HyukjinKwon/pyspark-client-wasm/blob/main/deploy/README.md)
for ports, version pins, and CORS/header checks.

## 3. Use the client

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()
```

In JupyterLite, open <http://localhost:8000/> and run the demo notebook. **Verify
isolation first** - `crossOriginIsolated === true` in the browser console - or the
blocking bridge cannot work:

```js
crossOriginIsolated === true   // must be true; else SharedArrayBuffer is unavailable
```

## What just happened

* `pcw.install()` monkey-patched PySpark's Connect stub to use a grpc-web/`fetch`
  transport. Nothing above the stub changed.
* `SparkSession.builder.remote("sc://...;transport=grpcweb")` parsed the web scheme
  and returned an ordinary `SparkSession`.
* `.toPandas()` built a protobuf plan, shipped it through Envoy to the Spark
  Connect server, and decoded the Arrow IPC result back into a pandas DataFrame -
  all synchronously, via the `Atomics`/`SharedArrayBuffer` bridge.

## Next steps

* [Connection patterns](connection-patterns.md) - `sc://` scheme, TLS, and auth.
* [Running locally](running-locally.md) - reference generation and the e2e harness.
* [JupyterLite hosting](jupyterlite-hosting.md) - host the site on GitHub Pages and friends.
* [Security](security.md) - what to harden before going past localhost.
