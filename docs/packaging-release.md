<!-- SPDX-License-Identifier: Apache-2.0 -->

# Packaging & release

How the `pyspark_connect_web` wheel is built, how it is installed in the browser
via `micropip`, and the release checklist.

> Disclaimer: this is an **unofficial personal project**, not affiliated with or
> endorsed by the Apache Software Foundation. "Apache Spark" and "PySpark" are
> trademarks of the ASF. See the README disclaimer.

## What ships

The distributable is a pure-Python wheel: `pyspark_connect_web-<version>-py3-none-any.whl`.

* **No compiled extensions** — it must import under Pyodide/WASM, so it is
  `py3-none-any` and depends on nothing native. In particular it does **not**
  depend on `grpcio` (DECISIONS.md #1) — `dependencies = []` in `pyproject.toml`,
  and `pyspark`/`pyarrow`/`pandas`/`protobuf` come from the Pyodide environment.
* The JS glue (`worker/*.js`, `jupyterlite/*`) ships inside the wheel as package
  data so the JupyterLite build can reference it.

### `py.typed` (PEP 561)

The package is fully type-hinted (typed `Protocol`s in `_contract.py`, etc.) but
does **not yet ship a `py.typed` marker**, so downstream type-checkers treat it
as untyped. To publish the type information:

1. Add an empty `pyspark_connect_web/py.typed` (PEP 561 marker).
2. Ensure it is packaged. With setuptools + `[tool.setuptools.packages.find]`
   this needs package-data inclusion, e.g. in `pyproject.toml`:

   ```toml
   [tool.setuptools.package-data]
   pyspark_connect_web = ["py.typed", "worker/*.js", "jupyterlite/*"]
   ```

`pyspark_connect_web/` is owned by lanes 1–4 / the integrator; lane 5 does not
add the marker unilaterally. This is flagged in `COORDINATION.md` for the owner
to land. (Without it, the JS/notebook package data above should still be
declared so the wheel is complete — confirm with the integrator.)

## Build the wheel

```bash
# in a dev venv (the browser does NOT use this venv)
python -m pip install build==1.2.2
python -m build --wheel --outdir dist     # -> dist/pyspark_connect_web-*.whl
# or:  make wheel
```

Validate the wheel is importable + grpcio-free *before* publishing:

```bash
python -m pip install dist/pyspark_connect_web-*.whl
python -c "import pyspark_connect_web; print(pyspark_connect_web.__name__)"
# grpcio must NOT be a transitive dependency:
python - <<'PY'
import importlib.metadata as m
reqs = m.requires("pyspark-connect-web") or []
assert not any("grpcio" in r for r in reqs), reqs
print("OK: no grpcio in wheel requirements")
PY
```

## Install in the browser (Pyodide / JupyterLite)

In the Pyodide worker (see `worker/worker_bootstrap.js`), `micropip` installs the
wheel by URL alongside the pinned runtime deps:

```python
import micropip
await micropip.install("protobuf>=7")
await micropip.install("pyspark>=4.0,<4.2")
await micropip.install("https://<your-lite-origin>/pyspark_connect_web-<version>-py3-none-any.whl")
```

`scripts/build_site.sh` copies the freshly built wheel into the JupyterLite
output root so it is served from the same (cross-origin-isolated) origin as the
page — important under COEP `require-corp` (a cross-origin CDN wheel must send
`Cross-Origin-Resource-Policy` or the import is blocked; see
`team/findings-lane5-deploy.md` gotcha #5). `worker_bootstrap.js` reads the wheel
URL from `self.PCW_WHEEL_URL` (default: the wheel served at the site root).

Then, in a notebook cell:

```python
import pyspark_connect_web as pcw
pcw.install()          # idempotent; monkey-patches the Connect stub
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://<host>:8081/;transport=grpcweb").getOrCreate()
```

## Version pins (the load-bearing ones)

| Thing | Pin | Why |
|-------|-----|-----|
| `pyspark` (browser + dev) | `>=4.0,<4.2` | DECISIONS.md #3 — reattachable execute present; `install()` raises outside the range |
| Pyodide | `>=0.28` / Python 3.13 | COORDINATION.md; provides `pyarrow>=22`, `pandas`, `numpy`, `protobuf>=7` |
| `build` | `==1.2.2` | wheel build (`scripts/build_site.sh`, Makefile) |
| `jupyterlite-core` | `==0.6.4` | `jupyter lite` CLI (`scripts/build_site.sh`) |
| `jupyterlite-pyodide-kernel` | `==0.6.1` | Pyodide kernel for the lite site |
| Envoy | `envoyproxy/envoy:v1.31-latest` | grpc_web filter + v3 HttpProtocolOptions |
| Spark Connect server | `apache/spark:4.0.0` | matches the `pyspark` range |

## Release checklist

Pre-release:

- [ ] `make test` green (unit; no browser, no grpcio).
- [ ] `make validate-deploy` green (YAML parse + COOP/COEP + no wildcard prod CORS).
- [ ] `make lint` green (ruff check + format).
- [ ] Bump `version` in `pyproject.toml` (drop the `.devN` suffix for a real release).
- [ ] `appVersion` in `jupyterlite/jupyter-lite.json` and `PCW_WHEEL_URL`
      default in `worker/worker_bootstrap.js` reference the same version.
- [ ] `make wheel` + the wheel-import / no-grpcio check above pass.
- [ ] `make site` builds `_output` with the wheel + `_headers` present.
- [ ] e2e against a live stack: `E2E_REQUIRE_STACK=1 make e2e` green (the full
      DECISIONS.md v0 matrix). Skip-only runs do **not** count as a release gate.
- [ ] Trademark/identity disclaimer present in `README.md` and docs (it is).

Publish:

- [ ] Tag the release (`vX.Y.Z`); CI builds the wheel on the tag.
- [ ] (If publishing to an index) `twine check dist/*` then upload. The package
      name on the index is `pyspark-connect-web`; the import name is
      `pyspark_connect_web`. The maintainer is still deciding the **repo** name
      vs the **package** name — see the README note on the split.
- [ ] Publish the built `_output` site to the cross-origin-isolated host
      (Envoy/static, or a Pages host that honours `_headers`).
- [ ] Smoke-test the published site: open it, confirm `crossOriginIsolated === true`,
      run the demo notebook end to end against a reachable Connect server.

Post-release:

- [ ] Update `README.md` status if the milestone changed.
- [ ] File a `team/findings-*` note for anything that surprised you.
