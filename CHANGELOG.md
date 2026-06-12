<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to **pyspark-connect-web** ("PySpark in JupyterLite") are
documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Unofficial personal project. Not affiliated with, sponsored by, or endorsed by
> the Apache Software Foundation. "Apache Spark", "Spark", and "PySpark" are
> trademarks of the ASF, used here only to describe interoperability. See the
> README disclaimer.

Distribution name on PyPI: `pyspark-connect-web`. Import/package name:
`pyspark_connect_web`. Both are pure-Python, `py3-none-any`, and (by invariant)
carry **no** `grpcio` dependency - see DECISIONS.md #1.

## [Unreleased]

### Added
- _Nothing yet._

<!--
Release runbook (kept here so it travels with the changelog; docs/ is owned by
the DOCS agent - see docs/packaging-release.md for the longer-form checklist):

  1. Pre-flight, green on `main`:
       make test            # unit; transport stubbed, no browser, no grpcio
       make lint            # ruff check + format --check
       make validate-deploy # COOP/COEP present, no wildcard prod CORS
     and the e2e gate (.github/workflows/e2e.yml) green on the commit, plus the
     `integration` CI job (real Spark Connect round-trip) green.
  2. Move the "Unreleased" entries below into a new "## [X.Y.Z] - YYYY-MM-DD"
     section. Keep the heading shape EXACT - release.yml's awk extractor matches
     `^## \[X.Y.Z\]` and copies until the next `## [`.
  3. Bump `version` in pyproject.toml to X.Y.Z (drop any `.devN`/`rcN` suffix for
     a final release) AND `appVersion` in jupyterlite/jupyter-lite.json +
     `PCW_WHEEL_URL` default in worker/worker_bootstrap.js to the same string.
     release.yml's `build` job HARD-FAILS if the `vX.Y.Z` tag != package version.
  4. Optional rehearsal (no real PyPI, no Release):
       Actions -> release -> "Run workflow" -> dry_run = testpypi
     uploads to TestPyPI via OIDC so you can `pip install -i .../simple ...`.
  5. Cut it:
       git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z
     The tag push runs release.yml: build sdist+wheel (assert no grpcio) ->
     publish to PyPI via OIDC trusted publishing (environment `pypi`) -> build
     the JupyterLite site -> create the GitHub Release with these notes +
     dist/* + the site tarball attached.
  6. Post-release: smoke-test the published site (crossOriginIsolated === true,
     demo notebook end-to-end against a reachable Connect server); update the
     README status; file a team/findings-* note for anything surprising.

One-time setup (maintainer): register the repo as a trusted publisher on PyPI
(and TestPyPI) for the `pypi`/`testpypi` GitHub Environments - no API token is
ever stored in repo secrets.
-->

## [0.1.0] - 2026-06-12

First tagged release of the **PySpark in JupyterLite** client: run the *real*
PySpark Connect Python client inside a browser (JupyterLite/Pyodide), talking to
a Spark Connect server over a grpc-web transport. Existing PySpark code runs
unchanged - no reimplementation, no local JVM, no Python backend.

### Added
- **grpc-web transport** - a `fetch`-based grpc-web stub (length-prefixed
  framing, `0x80` trailer frame) that replaces *only* PySpark's Connect service
  stub. We patch, we do not fork (DECISIONS.md #2). Implements
  ExecutePlan / ReattachExecute / ReleaseExecute, so mid-stream disconnects
  recover via reattach (DECISIONS.md #6).
- **JupyterLite/Pyodide bridge** - a Web Worker + `Atomics`/`SharedArrayBuffer`
  channel that makes Connect calls *blocking*, so `.collect()` / `.toPandas()`
  return synchronously and the public PySpark API stays unchanged
  (DECISIONS.md #5). Page-side `Worker` wrapping wires the bridge into the
  JupyterLite Pyodide kernel without forking it; a COI service worker covers
  header-less hosts. Cross-origin isolation (COOP/COEP) is mandatory and
  asserted (DECISIONS.md #4).
- **Arrow result decoding** - IPC reassembly to pandas, including SPARK-53525
  multi-chunk split-batch handling, byte/row-exact against a native Connect
  reference (DECISIONS.md #7).
- **`pcw.install()`** - idempotent, version-guarded to `pyspark>=4.0,<4.2`
  (DECISIONS.md #3); raises a clear error outside the range. Accepts
  `sc://host:port/;transport=grpcweb` plus `http(s)://` shorthand.
- **Real Spark-Connect-verified Python vertical** - the full v0 read-path matrix
  (`range`, `select`/`filter`/`groupBy`/`agg`, `toPandas`, `createDataFrame`,
  `spark.sql(...)`, 200k-row multi-response stream, mid-stream reattach) proven
  against a real in-process Spark Connect server in `tests/integration/` with a
  pure-Python grpc-web<->gRPC bridge standing in for Envoy.
- **Deploy stack** - dev `docker compose` (Spark 4.0.0 Connect + Envoy grpc-web
  proxy + COOP/COEP static host) and a hardened prod overlay (TLS, exact-origin
  CORS, bearer-token gate, size limits, health/readiness).
- **Pure-Python wheel** - `py3-none-any`, `dependencies = []`, **no `grpcio`**
  (DECISIONS.md #1); CI guards the no-grpcio invariant at source, wheel-metadata,
  and import time.
- **Packaging & release automation** - `python -m build` sdist + wheel, PyPI
  publish via OIDC trusted publishing on a `vX.Y.Z` tag, JupyterLite site built
  as a release asset, GitHub Release notes sourced from this changelog
  (`.github/workflows/release.yml`).

### Known limitations
- Session-timezone localization and struct-handling-mode are not applied on the
  result-decode side (no client config on a bare response iterable); the only
  known gap to byte-exact parity for timestamp/struct columns.
- The package does not yet ship a PEP 561 `py.typed` marker (owned by the
  package source lanes; tracked in COORDINATION.md / docs/packaging-release.md).
- `AddArtifacts` is lowered to a single grpc-web `unary()` POST (grpc-web has no
  true client streaming); not exercised end-to-end yet.

[Unreleased]: https://github.com/HyukjinKwon/pyspark-connect-web/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HyukjinKwon/pyspark-connect-web/releases/tag/v0.1.0
