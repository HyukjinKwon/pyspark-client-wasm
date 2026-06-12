<!-- SPDX-License-Identifier: Apache-2.0 -->

# deploy/ — Spark Connect + Envoy grpc-web proxy

This directory brings up the server side of **pyspark-connect-web**: a Spark 4.x
Connect server fronted by an Envoy `grpc_web` proxy, plus a static host for the
JupyterLite site that serves the mandatory cross-origin-isolation headers.

```
browser (JupyterLite/Pyodide, lane 3)
   │  sc://localhost:8081/;transport=grpcweb   (grpc-web over fetch)
   ▼
Envoy :8081  ──grpc_web filter──▶  Spark Connect :15002 (gRPC/HTTP2)
Envoy :8000  ── static site (COOP/COEP) ──▶  JupyterLite (./_output)
```

## Ports

| Port | Who | What |
|------|-----|------|
| 8081 | browser client | grpc-web endpoint. Client URL: `sc://localhost:8081/;transport=grpcweb` |
| 8000 | browser | JupyterLite site, served with `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp` |
| 15002 | native clients / Envoy upstream | Spark Connect raw gRPC. Also exposed on the host for `tests/e2e/reference.py` |
| 9901 | ops | Envoy admin |

## Bring it up

```bash
docker compose -f deploy/compose.yaml up
# wait for "spark-connect" to become healthy (gRPC port open; ~60s cold start)
```

Then point the web client at:

```python
import pyspark_connect_web as pcw
pcw.install()
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

Open the JupyterLite page at <http://localhost:8000/>. Confirm isolation in the
browser console:

```js
crossOriginIsolated === true   // must be true, or SharedArrayBuffer is unavailable
```

The JupyterLite site is produced by lane 3 (`jupyterlite build` into `../_output`).
Until that exists, the `:8000` host comes up but serves 404s — the grpc-web proxy
and Spark Connect server still work and can be exercised by `tests/e2e/reference.py`.

## Image / version pins

| Component | Pin | Notes |
|-----------|-----|-------|
| Spark Connect server | `apache/spark:4.0.0` | Bundles Connect; matches `pyspark>=4.0,<4.2` (DECISIONS.md #3) |
| Spark Connect package | `org.apache.spark:spark-connect_2.13:4.0.0` | Must match Spark + Scala (2.13) version exactly |
| Envoy | `envoyproxy/envoy:v1.31-latest` | Has `envoy.filters.http.grpc_web` |
| Static host | `halverneus/static-file-server:v1.8.10` | Serves `../_output` on :80 |

See `team/findings-lane5-deploy.md` for bring-up gotchas (stream timeouts, the
`--packages` first-run download, COEP and CDN wheels, the gRPC vs grpc-web split).

## Verifying CORS / grpc-web without a browser

```bash
# Envoy should answer the CORS preflight on the grpc-web port:
curl -i -X OPTIONS http://localhost:8081/spark.connect.SparkConnectService/ExecutePlan \
  -H "Origin: http://localhost:8000" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: x-grpc-web,content-type"
# expect: access-control-allow-origin and access-control-allow-headers in the response

# Static host should carry the isolation headers:
curl -sI http://localhost:8000/ | grep -i 'cross-origin'
# expect: Cross-Origin-Opener-Policy: same-origin
#         Cross-Origin-Embedder-Policy: require-corp
```
