<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings — lane 5 (Envoy proxy + deploy + e2e + docs)

## Version pins (exact)

| Component | Pin | Why |
|-----------|-----|-----|
| Spark Connect server | `apache/spark:4.0.0` | Bundles Spark Connect; matches `pyspark>=4.0,<4.2` (DECISIONS.md #3). Reattachable execute present. |
| Spark Connect package | `org.apache.spark:spark-connect_2.13:4.0.0` | Passed to `start-connect-server.sh --packages`. MUST match Spark version AND Scala build (apache/spark:4.0.0 is Scala 2.13). Mismatch fails at startup. |
| Envoy | `envoyproxy/envoy:v1.31-latest` | Ships `envoy.filters.http.grpc_web` and the v3 HttpProtocolOptions used for the h2c upstream. |
| Static host | `halverneus/static-file-server:v1.8.10` | Serves the built JupyterLite site on :80 (configurable FOLDER/PORT). |
| Playwright | `@playwright/test ^1.49.0` | Chromium honours server-sent COOP/COEP, so `crossOriginIsolated` works without launch flags. |
| Pyodide (browser) | `>= 0.28` / Python 3.13 | Per COORDINATION.md; provided by lane 3, not pinned here. |

## Port layout (decided)

* `8081` — grpc-web endpoint. Client URL `sc://localhost:8081/;transport=grpcweb` (matches API_CONTRACT.md §2 + README).
* `8000` — JupyterLite static host with COOP/COEP.
* `15002` — Spark Connect raw gRPC. Envoy upstream AND host-exposed so `tests/e2e/reference.py` (native client) can reach it.
* `9901` — Envoy admin.

## Bring-up gotchas

1. **Two transports, do not confuse them.** Browser speaks grpc-web to Envoy :8081. The native reference generator speaks plain gRPC to :15002. Envoy translates grpc-web -> gRPC for the browser path only.
2. **Streaming timeouts.** ExecutePlan/ReattachExecute are long-lived server streams. Default Envoy idle/route timeouts kill them and look like spurious disconnects. envoy.yaml sets `stream_idle_timeout: 0s` on the HCM and `timeout: 0s` + `grpc_timeout_header_max: 0s` on the grpc-web route.
3. **Upstream must be HTTP/2 (h2c).** Spark Connect is gRPC over HTTP/2 without TLS inside the compose network. The spark-connect cluster uses HttpProtocolOptions.explicit_http_config.http2_protocol_options to force h2c; omitting it makes Envoy speak HTTP/1.1 upstream and gRPC fails.
4. **First-run --packages download.** start-connect-server.sh resolves the connect jar via Ivy on first boot (~1 min, needs network). Healthcheck has start_period: 60s + 12 retries. Air-gapped: pre-bake the jar.
5. **COEP + CDN wheels.** require-corp means any cross-origin resource the page loads (Pyodide/pyarrow wheels from a CDN) must send Cross-Origin-Resource-Policy or be CORS+crossorigin. Static host adds CORP: cross-origin to its own responses; if lane 3 pulls wheels from a CDN that does NOT set CORP, isolation breaks the import. Safest: vendor wheels behind the same isolated origin.
6. **start-connect-server.sh foregrounding.** Script daemonizes by default. Compose command runs it then tails the log so the container stays up and logs stream to docker logs.
7. **CORS is permissive by design.** grpc-web vhost allows origin .* so a JupyterLite page served anywhere can call :8081. Tighten before any non-local deployment.

## Contract issues / observations

* **Port discrepancy, brief vs frozen contract.** My brief mentioned :15002 as the gRPC default; API_CONTRACT.md §2 + README use sc://localhost:8081. I used 8081 for grpc-web (contract-facing) and 15002 for the upstream gRPC (Spark default). No contract change needed — both honored.
* **grpcio scoping.** DECISIONS.md #1 forbids grpcio inside pyspark_connect_web/. tests/e2e/reference.py uses PySpark's normal Connect client (pulls grpcio) and lives under tests/, so it is compliant. CI grpcio-guard greps pyspark_connect_web/ ONLY — verified it does not flag tests/. Do not widen the guard.
* **No false-positive on grpcweb.** Guard regex uses word boundaries so `import grpcweb`, `grpc_path`, `from mygrpc import x` are NOT flagged; verified.

## e2e: live vs TODO-pending other lanes

* **Live today:** crossOriginIsolated === true check (pure server-header assertion), stack-up probe + graceful skip, the reference generator, and the CI grpcio/headers guards.
* **Scaffolded (test.fixme + TODO hooks), pending lanes 1-4 + lane 3 JupyterLite build:** range/collect, filter/groupBy/agg parity, createDataFrame round-trip, spark.sql, and mid-stream ReattachExecute recovery. Each has the exact in-kernel snippet and the JS bridge contract (window.__pcwRunPython) it needs lane 3 to expose, plus a documented disconnect-injection strategy (page.route() abort or a bridge test hook).

## Open dependency on lane 3

The e2e harness needs lane 3 to: (a) build the JupyterLite site into ./_output (served by the static container), and (b) expose a window.__pcwRunPython(src) bridge that runs Python in the kernel and returns JSON. Helper stubs in tests/e2e/helpers.ts document this contract.
