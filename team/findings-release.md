<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings — release automation (RELEASE agent)

Owns: `.github/workflows/release.yml`, `CHANGELOG.md`. Does NOT edit `ci.yml`,
`pyproject.toml`, `docs/`, or any package source — snippets for the integrator
are below.

## What landed

- **`.github/workflows/release.yml`** — tag-driven publish, modelled on the
  maintainer's sibling release flows (semver tag -> build language package ->
  publish to the language registry -> GitHub Release from CHANGELOG). For Python
  the registry is PyPI via **OIDC trusted publishing** (no API token in secrets).
- **`CHANGELOG.md`** — Keep a Changelog format, `[Unreleased]` + a `[0.1.0]`
  first entry. The release runbook lives in an HTML comment inside it (docs/ is
  the DOCS agent's; this keeps the runbook traveling with the changelog).

## Release flow, stage by stage

Trigger: push of a tag matching `v*` (e.g. `v0.1.0`).

1. **`build`** (ubuntu, perms: default read)
   - `pip install build==1.2.2` (pin matches `scripts/build_site.sh` + ci.yml).
   - `python -m build --sdist --wheel --outdir dist`.
   - Installs the wheel WITHOUT grpcio and imports it -> proves Pyodide-constraint
     import (DECISIONS.md #1).
   - Re-runs ci.yml's exact assertion: `importlib.metadata.requires(...)` has no
     `grpcio` entry.
   - `twine check dist/*` (metadata + long-description render).
   - Resolves `version` from the wheel filename and, on a tag push, HARD-FAILS if
     `${tag#v} != version` — tag and `pyproject.toml` version must agree.
   - Uploads `dist/*` as an artifact; exposes `version` as a job output.

2. **`site`** (parallel to build)
   - Installs pinned `build`/`jupyterlite-core==0.6.4`/
     `jupyterlite-pyodide-kernel==0.6.1`, runs `scripts/build_site.sh` -> `_output`.
   - Tars `_output` -> `pyspark-connect-web-site-<version>.tgz` artifact (Release asset).

3a. **`publish-pypi`** (needs build+site; if tag push only)
   - `environment: pypi`, `permissions: id-token: write`.
   - `pypa/gh-action-pypi-publish@release/v1` with **no password** -> OIDC
     trusted publishing to real PyPI; `environment.url` deep-links the version page.

3b. **`publish-testpypi`** (workflow_dispatch + `dry_run == 'testpypi'` only)
   - `environment: testpypi`, `id-token: write`, same action with
     `repository-url: https://test.pypi.org/legacy/`. Never touches real PyPI.

4. **`github-release`** (needs build+publish-pypi; if tag push only)
   - `permissions: contents: write`.
   - `awk` extracts the `^## \[X.Y.Z\]` section from `CHANGELOG.md` until the next
     `## [` (validated locally on `[0.1.0]` — 48 lines, stops cleanly).
   - `softprops/action-gh-release@v2`: Release `vX.Y.Z`, body = those notes,
     assets = `dist/*` + the site `.tgz`. `prerelease` auto-true for dev/rc/a/b.

**`workflow_dispatch` input `dry_run`:** `build-only` (default; build + all
assertions, no upload/Release) or `testpypi` (also OIDC-upload to TestPyPI).
Neither ever publishes to real PyPI or creates a Release.

Pinned actions: `actions/checkout@v4`, `actions/setup-python@v5`,
`actions/upload-artifact@v4`, `actions/download-artifact@v4`,
`pypa/gh-action-pypi-publish@release/v1`, `softprops/action-gh-release@v2`.

## Versioning / tag convention

- **Semantic versioning**, single source of truth = `version` in `pyproject.toml`.
  `0.1.0` is the first release (repo currently at `0.0.1.dev0`).
- Release = annotated tag `vX.Y.Z` (`v` prefix matches the sibling Ruby/Scala
  projects). `build` enforces tag-vs-version agreement.
- Pre-releases `vX.Y.ZrcN`/`…devN` flow through unchanged and are auto-marked
  `prerelease` on the GitHub Release.

## Validated here vs. only on a real tag push

Validated locally (no network/docker here):
- `release.yml` parses as YAML (5 jobs; correct on:/permissions:/environment:).
- Every inline `run:` script is `bash -n`-clean.
- The `awk` notes-extractor yields the right non-empty `0.1.0` section and stops
  before the next heading.

Only on a real tag push / dispatch (network + GitHub identity):
- The **OIDC handshake** with PyPI/TestPyPI + actual upload (needs the trusted-
  publisher registration for the `pypi`/`testpypi` Environments).
- `pypa/gh-action-pypi-publish` / `softprops/action-gh-release` runtime behaviour.
- `scripts/build_site.sh` actually building `_output` (needs pinned jupyterlite +
  Pyodide assets = network).
- GitHub Release creation + asset upload.
- One-time maintainer setup: register the repo as a **trusted publisher** on PyPI
  and TestPyPI bound to the `pypi`/`testpypi` Environments. Until that exists,
  `publish-pypi` fails the OIDC mint (expected first-run gate, not a bug).

## ===== SNIPPETS FOR THE INTEGRATOR (RELEASE must not edit ci.yml/pyproject) =====

### 1. New `integration` job for `.github/workflows/ci.yml`

Append under `jobs:`. Makes the real Spark-Connect round-trip a continuous gate.
`grpcio`/`grpcio-status` are installed ONLY for the test bridge (under `tests/`,
outside the package — DECISIONS.md #1 allows it).

```yaml
  integration:
    name: real Spark Connect round-trip (tests/integration)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Java 17 (Temurin)
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: install (dev + test-only grpcio for the Envoy stand-in bridge)
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"
          # grpcio/grpcio-status are the downstream gRPC client used by the
          # pure-Python grpc-web<->gRPC bridge in tests/integration/bridge.py.
          # Test-only; NOT a package dependency (DECISIONS.md #1 is package-scoped).
          pip install grpcio grpcio-status
      - name: pytest tests/integration (real in-process Spark Connect server)
        run: pytest -q tests/integration
```

### 2. `pyproject.toml` additions

`[project.urls]` (README leaves the repo name undecided; these use the sibling-
project org placeholder — confirm/adjust the repo slug, and keep CHANGELOG.md's
compare/tag links in sync):

```toml
[project.urls]
Homepage = "https://github.com/HyukjinKwon/pyspark-connect-web"
Repository = "https://github.com/HyukjinKwon/pyspark-connect-web"
Changelog = "https://github.com/HyukjinKwon/pyspark-connect-web/blob/main/CHANGELOG.md"
```

Version source: the current static `version = "..."` in `[project]` is the single
source of truth and works as-is (the tag-vs-version guard enforces agreement).
**No change required.** If tag-derived versioning is wanted later, switch to a
dynamic backend (e.g. `setuptools-scm` + `dynamic = ["version"]`), but that
changes the wheel-name->version derivation `release.yml` relies on — coordinate
first. Static-version path recommended for now.

### 3. Optional, related

- No `LICENSE` file exists yet (only SPDX headers + `license = { text =
  "Apache-2.0" }`). A top-level Apache-2.0 `LICENSE` would let `twine check`/PyPI
  surface the license and matches the sibling projects. Not RELEASE-owned;
  flagging for the integrator.
