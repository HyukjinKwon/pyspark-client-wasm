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

---

# Production-hardening pass (2026-06-12)

## Prod vs dev deploy story

* **Dev** = `deploy/envoy.yaml` + `deploy/compose.yaml` (unchanged, source of truth for the grpc_web/COOP/COEP/h2c plumbing): plaintext, wildcard CORS, no auth, ports 8081/8000/9901, Spark :15002 published for `reference.py`.
* **Prod** = `deploy/envoy.prod.yaml` + `deploy/compose.prod.yaml` (a compose override; `docker compose -f compose.yaml -f compose.prod.yaml up`). Envoy has no native overlay-merge, so envoy.prod.yaml is a **full** bootstrap — diff it against envoy.yaml for the delta. Hardening: TLS termination on both listeners (8443 grpc-web / 8444 static, TLSv1_2+), **tight CORS** (exact-origin allowlist, no `.*`; allow_credentials needs a concrete origin), a **bearer-token Lua gate** (401 if no `Authorization: Bearer`, OPTIONS passes; jwt_authn/ext_authz sketched for real validation), 32 MiB per-connection buffer + bounded headers, a dedicated **health/ready** listener on :8089 (`/healthz` direct 200, `/ready` via health_check filter — never touches Spark) + active TCP health check on the spark-connect cluster, admin bound to **loopback** (127.0.0.1:9901, not published), HSTS + nosniff on the static host. Spark's gRPC port is NOT published in prod (private; proxy is the only public surface).

## Build/release runbook

* `scripts/build_site.sh` (also `make site`): `python -m build --wheel` then `jupyter lite build` into `_output`, copies `_headers` (COOP/COEP) + the wheel into the output, and sanity-checks both are present. Pinned tools: `build==1.2.2`, `jupyterlite-core==0.6.4`, `jupyterlite-pyodide-kernel==0.6.1` (must stay compatible with Pyodide >=0.28/Py3.13). Offline once deps cached; fails loud otherwise — could not RUN here (no net/docker), validated by static checks (bash -n, yaml/json parse, ruff).
* `Makefile`: install/test/lint/fmt/wheel/sdist/dist/site/up/up-prod/down/reference/e2e/validate-deploy. `make help` lists them.
* `scripts/render_envoy_prod.sh` templates YOUR-PUBLIC-HOST / YOUR-LITE-ORIGIN from env. `scripts/gen_dev_cert.sh` makes a self-signed cert for staging only.
* `scripts/validate_deploy.py` (`make validate-deploy` + CI headers-guard): parses all 4 deploy YAMLs, asserts COOP/COEP in dev AND prod, asserts prod has no wildcard CORS. Replaced the old grep-based headers-guard.
* Full runbook + release checklist + micropip-in-Pyodide install + py.typed note: `docs/packaging-release.md`.

## What the e2e now covers (was scaffold, now WIRED + gated)

* helpers.ts now drives lane 3's `window.__pcwRunPython(src)`: `runPython` (JSON-parses captured stdout), `waitForKernel` (waits for the bridge global + smoke-runs a trivial snippet), `bridgeAvailable` (probe), and `injectMidStreamDisconnect` (one-shot Playwright `page.route()` that aborts the FIRST ExecutePlan POST after it starts, so PySpark's reattachable iterator must recover via ReattachExecute — a path we do NOT intercept — DECISIONS.md #6).
* v0-checklist.spec.ts: removed all `test.fixme`. Each item asserts the DECISIONS.md v0 matrix. Query #3 is byte-for-byte aligned with reference.py (added the missing `select((F.col("id")%10)...).groupBy.agg.orderBy`). Two-axis graceful skip: stack-down skips all; bridge-not-wired skips the bridge tests (both become hard failures only under E2E_REQUIRE_STACK=1, the CI gate). The `crossOriginIsolated` test still runs whenever the page is up (no bridge).
* Validated by `node --experimental-strip-types --check` (no npm/net here).

## CI (.github/workflows/ci.yml)

* `unit`: pytest on a py matrix (3.11, 3.12).
* `lint`: ruff check + format --check, SCOPED to `scripts/ tests/e2e/reference.py` (whole-repo lint disabled — pyspark_connect_web/ has 2 pre-existing ruff findings lane 5 does not own / must not edit: F401 TRAILER_FLAG in transport/grpcweb.py, F841 `length` in worker/sab_channel.py).
* `grpcio-guard`: unchanged, package-scoped (DECISIONS.md #1).
* `headers-guard`: now runs `scripts/validate_deploy.py` instead of grep.
* `build-wheel`: builds the wheel, installs+imports it, asserts no `grpcio` in its declared requirements, uploads it as an artifact.
* e2e job: still a commented sketch (no browser stack in CI); updated to call `scripts/build_site.sh` + E2E_REQUIRE_STACK=1 once the stack lands.

## Heads-up for other lanes

* **py.typed (lanes 1–4 / integrator):** package is fully type-hinted but ships no PEP 561 `py.typed`, so downstream type-checkers treat it as untyped. Lane 5 did NOT add it (package source not ours). To ship types: add empty `pyspark_connect_web/py.typed` + `[tool.setuptools.package-data]` in pyproject incl. `py.typed`, `worker/*.js`, `jupyterlite/*`. Details in docs/packaging-release.md.
* **2 ruff findings in pyspark_connect_web/** (above) block enabling whole-repo lint; cheap fixes for owners — then flip lint paths to `.` + land `[tool.ruff]` in pyproject.
* **Bridge open item (lane 3):** e2e bridge tests skip until `window.__pcwRunPython` is wired into the JupyterLite kernel (findings-lane3 open item #1).

## Security review (docs/security.md) — findings summary

Threat-modeled 5 areas (threat→impact→mitigation):
1. **SharedArrayBuffer / cross-origin isolation:** Spectre-class side channels are why COOP/COEP exist; silent isolation loss (CDN strips headers) breaks the bridge; COEP blocks non-CORP cross-origin wheels. Mitigations: COOP/COEP in both Envoy configs + CI guard + e2e first-gate assert; worker fails closed if not isolated; vendor wheels behind the isolated origin.
2. **CORS:** wildcard = any site can drive the user's browser to a no-auth Spark server (confused deputy). Mitigation: prod exact-origin allowlist, POST only, short max_age, CI fails on wildcard; CORS is not authz.
3. **Auth to Spark Connect:** Connect has NO built-in auth — the proxy is the only gate. Mitigations: keep Spark private (no host port in prod), proxy bearer-token gate → jwt_authn/ext_authz for real validation, TLS, admin on loopback, segmentation, treat browser token as short-lived bearer (no logs/URLs).
4. **Untrusted/malicious server:** crafted Arrow bytes (decoder DoS), reattach loops, unbounded streams, active content in values/errors. Mitigations: TLS authenticates the server, pyarrow decode in WASM sandbox (crash contained), lane-4 chunk-integrity checks, 32 MiB upload bound, document .collect() OOM (use limit/pagination).
5. **Notebook-output XSS:** server strings rendered as HTML in the token-bearing notebook origin = XSS. Mitigations: text/plain by default, keep Jupyter DOMPurify, never display(HTML(raw)), escape in to_html, CSP/nosniff defense-in-depth.

Residual/out-of-scope: Lua gate is presence-only (real token validation = deploy responsibility), no rate limiting, single-trust-domain Spark, no SRI/hash-pinning (versions pinned, hashes not). Public-deploy checklist at the bottom of docs/security.md.
