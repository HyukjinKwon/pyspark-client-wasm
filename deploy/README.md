<!-- SPDX-License-Identifier: Apache-2.0 -->

# deploy/ - Spark Connect + Envoy grpc-web proxy

This directory brings up the server side of **pyspark-connect-web**: a Spark 4.x
Connect server fronted by an Envoy `grpc_web` proxy, plus a static host for the
JupyterLite site that serves the mandatory cross-origin-isolation headers.

```
browser (JupyterLite/Pyodide, the components)
   |  sc://localhost:8081/;transport=grpcweb   (grpc-web over fetch)
   v
Envoy :8081  --grpc_web filter-->  Spark Connect :15002 (gRPC/HTTP2)
Envoy :8000  -- static site (COOP/COEP) -->  JupyterLite (./_output)
```

## Ports

| Port | Who | What |
|------|-----|------|
| 8081 | browser client | grpc-web endpoint. Client URL: `sc://localhost:8081/;transport=grpcweb` |
| 8000 | browser | JupyterLite site, served with `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: credentialless` |
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

The JupyterLite site is produced by the (`jupyterlite build` into `../_output`).
Until that exists, the `:8000` host comes up but serves 404s - the grpc-web proxy
and Spark Connect server still work and can be exercised by `tests/e2e/reference.py`.

## Image / version pins

| Component | Pin | Notes |
|-----------|-----|-------|
| Spark Connect server | `apache/spark:4.0.0` | Bundles Connect; matches `pyspark>=4.0,<4.2` |
| Spark Connect package | `org.apache.spark:spark-connect_2.13:4.0.0` | Must match Spark + Scala (2.13) version exactly |
| Envoy | `envoyproxy/envoy:v1.31-latest` | Has `envoy.filters.http.grpc_web` |
| Static host | `halverneus/static-file-server:v1.8.10` | Serves `../_output` on :80 |

See `the project notes` for bring-up gotchas (stream timeouts, the
`--packages` first-run download, COEP and CDN wheels, the gRPC vs grpc-web split).

## Dev vs prod

`deploy/envoy.yaml` + `deploy/compose.yaml` are the **dev** stack: plaintext,
wildcard CORS, no auth, ports 8081/8000, admin on :9901, Spark's gRPC port
published to the host for `tests/e2e/reference.py`. Convenient for a laptop;
**not safe past localhost.**

`deploy/envoy.prod.yaml` + `deploy/compose.prod.yaml` are the **hardened prod
overlay.** Bring it up as a compose override:

```bash
docker compose -f deploy/compose.yaml -f deploy/compose.prod.yaml up -d
# or: make up-prod
```

| Concern | Dev | Prod |
|---------|-----|------|
| Transport | plaintext HTTP | TLS (HTTPS/wss), `TLSv1_2`+ |
| Ports | 8081 grpc-web, 8000 static, 9901 admin | 8443 grpc-web, 8444 static, 8089 probe; admin loopback-only |
| CORS | wildcard `.*` | explicit origin allowlist (`exact:`), no wildcard |
| Auth | none | bearer-token gate (Lua) -> replace with jwt_authn/ext_authz |
| Request size | unbounded | 32 MiB per-connection buffer + bounded headers |
| Spark gRPC | published `15002:15002` | **not** published (private) |
| Health/ready | (Envoy admin) | dedicated `:8089` `/healthz` + `/ready` |

Both keep the `grpc_web` filter, the long-stream timeouts, the h2c upstream, and
the mandatory COOP/COEP on the static host. `scripts/validate_deploy.py`
(`make validate-deploy`, also a CI gate) parses both configs and fails if
COOP/COEP go missing or the prod CORS regresses to a wildcard.

## TLS / cert mounting (prod)

`envoy.prod.yaml` terminates TLS using a cert at `/etc/envoy/certs/tls.{crt,key}`.
`compose.prod.yaml` bind-mounts `deploy/certs/` there read-only. Provide a cert:

```bash
# Production: use a real cert (Let's Encrypt / your CA). Place the chain + key:
#   deploy/certs/tls.crt   (full chain)
#   deploy/certs/tls.key   (private key, chmod 600)

# Staging / local TLS testing only - self-signed (browsers will warn):
PCW_PUBLIC_HOST=localhost scripts/gen_dev_cert.sh
```

For Kubernetes, mount a TLS `Secret` at the same path instead of the bind mount.
A browser needs a **secure context** for `crossOriginIsolated` off localhost, so
prod must be HTTPS - a self-signed cert that the browser rejects will also break
cross-origin isolation.

## Setting your origins (prod)

`envoy.prod.yaml` has placeholders `YOUR-PUBLIC-HOST.example.com` and
`https://YOUR-LITE-ORIGIN.example.com`. Either edit them in place, or render a
deploy-ready copy from env without touching the template:

```bash
PCW_PUBLIC_HOST=spark.example.com \
PCW_LITE_ORIGIN=https://lite.example.com \
scripts/render_envoy_prod.sh > deploy/envoy.prod.rendered.yaml
```

## Auth (prod) - Spark Connect has no built-in auth

The proxy is the enforcement point. The shipped Lua filter rejects any request
without `Authorization: Bearer <token>` (401), letting CORS preflights through.
**It does not validate the token.** For production, replace it with
`envoy.filters.http.jwt_authn` (validate a JWT against your IdP's JWKS) or
`envoy.filters.http.ext_authz` (delegate to an authz service) - both are sketched
at the bottom of `envoy.prod.yaml`. The `authorization` header is forwarded
upstream so a Spark-side interceptor can re-check. See `docs/security.md` section 3.

## Health / readiness (prod)

The prod config adds a dedicated probe listener on `:8089` that does **not** hit
Spark, so probes stay cheap:

```bash
curl -fsS http://<host>:8089/healthz   # -> "live"  (Envoy process up)
curl -fsS http://<host>:8089/ready     # -> 200 normally, 503 while draining
```

Point your load balancer / k8s `livenessProbe` at `/healthz` and `readinessProbe`
at `/ready`. The `spark-connect` cluster also has an active TCP health check, so
`/ready` and routing reflect real upstream health.

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
#         Cross-Origin-Embedder-Policy: credentialless
```
