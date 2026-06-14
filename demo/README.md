<!-- SPDX-License-Identifier: Apache-2.0 -->

# Embedded BI query cell - demo

A small, product-style BI page that runs the **real PySpark Connect client in the
browser** (Pyodide) and queries a remote Spark Connect server over grpc-web - no
JVM, no `pip install pyspark`, no client setup. It demonstrates the
"embed a live query cell in a web app" use case for **pyspark-connect-web**.

![PySpark BI demo: pick a table, write SQL, see results, all in the browser](../docs/demo.gif)

> The GIF above is this demo recorded in CI (`tests/e2e/demo.spec.ts`) against a
> real Spark Connect server.

What it does:

- **Picks a table** - lists the remote Spark catalog (`SHOW TABLES`) in a sidebar.
- **Shows schema** - `DESCRIBE TABLE` on click.
- **Runs your SQL** - `spark.sql(...).toPandas()` over the blocking SAB/Atomics
  bridge, rendered as a result grid (first 1000 rows).
- **Ships sample data** - a synthetic retail dataset (`customers`, `products`,
  `orders`) defined as deterministic CTEs (seeded `rand`/`pmod`) that are injected
  into every query, so the "tables" need no DDL: every statement sent to Spark is
  a data-returning `SELECT`. Same dataset for every visitor, no warehouse writes,
  works on any Spark Connect server. Plus four example analytics queries (top
  products, revenue by country, monthly revenue, top customers).

  > Design note. The dataset is injected as CTEs so every statement the demo
  > sends is a data-returning `SELECT` (no `CREATE`/temp-view DDL). Table schemas
  > are returned without an RPC and result columns come from the pandas frame, so
  > the demo touches Spark only through `toPandas`. These exact queries are
  > regression-tested against a real Spark Connect server in
  > `tests/integration/test_demo_queries.py` (the `ci.yml` integration job, no
  > browser). A secondary finding while building this: a server-side query error
  > (for example invalid SQL) currently surfaces poorly in the browser bridge (it
  > can loop on the error response rather than report it); valid queries work.

It is the same boot path as [`jupyterlite/harness.html`](../pyspark_connect_web/jupyterlite/harness.html)
(a module Web Worker running `worker_bootstrap.js` + the `bridge.js` blocking
transport), with a BI UI layered on top of `window.__pcwRunPython`.

## Run it

You need Docker (for the Spark Connect server + Envoy proxy) and the build
toolchain `scripts/build_site.sh` uses (Python with `jupyterlite-core`,
`jupyterlite-pyodide-kernel`, `build`; first build also needs network to fetch
Pyodide and the PyPI wheels).

```bash
# 1. Build the site and stage the demo page into _output/demo/.
scripts/build_demo_site.sh

# 2. Bring up Spark Connect (4.x) + Envoy grpc-web + the static host.
docker compose -f deploy/compose.yaml up
#    wait for "pcw-spark-connect" to become healthy (~60s JVM cold start)

# 3. Open the cross-origin-isolated page.
open http://localhost:8000/demo/
```

First load spends ~15-30s booting Pyodide and installing the PySpark wheels in
the browser (shown on the boot overlay); after that, queries are interactive.

## How it connects

```
browser tab (this page, Pyodide + PySpark Connect client)
   |  sc://localhost:8081/;transport=grpcweb     (grpc-web over fetch)
   v
Envoy :8081  --grpc_web filter-->  Spark Connect :15002 (gRPC/HTTP2)
Envoy :8000  -- static host (COOP/COEP) -->  _output/ (this page + assets)
```

The endpoint is overridable with `?remote=` - 
e.g. `http://localhost:8000/demo/?remote=sc://myhost:8081/;transport=grpcweb`.

## Why it must be served by Envoy (`:8000`), not opened from disk

The blocking bridge uses `SharedArrayBuffer` + `Atomics.wait`, which require a
**cross-origin-isolated** page (`Cross-Origin-Opener-Policy: same-origin` +
`Cross-Origin-Embedder-Policy: credentialless`). The deploy Envoy sets those
headers; opening `index.html` from the filesystem will fail the
`crossOriginIsolated` check (the page shows a clear error if so). The page also
loads `/worker/...`, `/pyodide/...` and the `*.whl` files **same-origin**, which is
exactly why it must live under `_output/` next to those assets.
