<!-- SPDX-License-Identifier: Apache-2.0 -->

# Connection patterns

After `pcw.install()`, you connect with the ordinary
`SparkSession.builder.remote(...)` API. The patch teaches the connection parser a
**grpc-web scheme**; everything else about building a session is stock PySpark.

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

## The `sc://` scheme

The canonical form selects the grpc-web transport with a `transport=grpcweb`
parameter:

```
sc://<envoy-host>:<port>/;transport=grpcweb[;<more params>]
```

* `<envoy-host>:<port>` is the **Envoy grpc-web endpoint** (default `:8081`),
  **not** the Spark Connect gRPC port (`:15002`). The browser only ever talks to
  Envoy.
* Without `;transport=grpcweb`, a stock `sc://` URL is left to PySpark's own
  (native gRPC) handling — which cannot work in the browser, since `grpcio` is
  absent. The web transport is only engaged when the parameter is present.

### `http://` / `https://` shorthand

You can also pass a plain URL; it is normalized to the canonical `sc://` form:

| You pass | Normalized to |
|---|---|
| `https://host` | `sc://host:443/;transport=grpcweb;use_ssl=true` |
| `http://host` | `sc://host:80/;transport=grpcweb` |
| `https://host:8443` | `sc://host:8443/;transport=grpcweb;use_ssl=true` |

```python
spark = SparkSession.builder.remote("https://spark.example.com").getOrCreate()
```

## TLS

* **Localhost / dev** runs plaintext (`http://`, `sc://…` without `use_ssl`). The
  dev Envoy config terminates no TLS — fine for a laptop, **never** past
  localhost.
* **Anything public must use TLS** (`https://` shorthand, or
  `;use_ssl=true`). Two reasons:
    1. Browsers only grant `crossOriginIsolated` (and therefore
       `SharedArrayBuffer`) on a **secure context** off `localhost`. Without HTTPS
       the blocking bridge is dead.
    2. The bearer token (below) rides the connection — it must never travel in
       plaintext.

The prod Envoy listeners terminate TLS (`TLSv1_2` minimum); cert mounting is
documented in [`deploy/README.md`](https://github.com/HyukjinKwon/pyspark-client-wasm/blob/main/deploy/README.md).
See [Security §3–4](security.md) for the full rationale.

## Authentication

**Spark Connect has no built-in authentication.** Anyone who can reach the gRPC
port can run arbitrary Spark plans. In this topology the browser reaches Spark
*through Envoy*, so **Envoy is the only enforcement point** — if Envoy is open,
Spark is open.

Auth is carried as channel-level metadata that the stub forwards as grpc-web
request headers — typically an `Authorization: Bearer <token>` header. PySpark's
`ChannelBuilder.metadata()` is the supported way to inject channel-level headers,
and the patch forwards exactly those pairs (see the `WebChannel.params` plumbing
in `pyspark_connect_web/patch.py`).

On the proxy side:

* The prod Envoy config ships a minimal Lua gate that **rejects any request
  lacking** `Authorization: Bearer <token>` with `401` (CORS preflights pass
  through). This stops anonymous access but **does not validate the token**.
* For production, replace the presence-only gate with `jwt_authn` (validate a JWT
  against your IdP's JWKS) or `ext_authz` (delegate to an authz service). Both are
  sketched at the bottom of `deploy/envoy.prod.yaml`.

!!! warning "Token handling in the browser"
    The token lives in the page's JS context and is sent as a grpc-web header.
    Treat it as a bearer credential: short-lived, scoped, refreshable. Do not log
    it, and do not put it in URLs (it would land in proxy/access logs and the
    `Referer` header).

## Endpoint cheat-sheet

| Endpoint | Port | Who talks to it |
|---|---|---|
| Envoy grpc-web | `:8081` | the browser client (`sc://…;transport=grpcweb`) |
| Envoy static host | `:8000` | the browser loading the JupyterLite site (COOP/COEP) |
| Spark Connect (gRPC) | `:15002` | the native reference generator only; **never** the browser, and not exposed in prod |
