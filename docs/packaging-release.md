<!-- SPDX-License-Identifier: Apache-2.0 -->

# Packaging & release

How the `pyspark_connect_web` wheel is built, how it is installed in the browser
via `micropip`, and the release checklist.

## What ships

The distributable is a pure-Python wheel: `pyspark_connect_web-<version>-py3-none-any.whl`.

* **No compiled extensions** - it must import under Pyodide/WASM, so it is
  `py3-none-any` and depends on nothing native. In particular it does **not**
  depend on `grpcio` - `dependencies = []` in `pyproject.toml`,
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

`pyspark_connect_web/` is owned by the components / the integrator; the does not
add the marker unilaterally. This is flagged in `CONTRIBUTING.md` for the owner
to land. (Without it, the JS/notebook package data above should still be
declared so the wheel is complete - confirm with the integrator.)

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
await micropip.install("googleapis-common-protos>=1.56.4")
# Slim Spark Connect client; deps=False (grpcio/grpcio-status are shimmed).
await micropip.install("https://<your-lite-origin>/pyspark_client-4.1.2-py3-none-any.whl", deps=False)
await micropip.install("https://<your-lite-origin>/pyspark_connect_web-<version>-py3-none-any.whl")
```

`scripts/build_site.sh` copies the freshly built wheel into the JupyterLite
output root so it is served from the same (cross-origin-isolated) origin as the
page - important under COEP `require-corp` (a cross-origin CDN wheel must send
`Cross-Origin-Resource-Policy` or the import is blocked; see
`the project notes` gotcha #5). `worker_bootstrap.js` reads the wheel
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
| `pyspark` (browser + dev) | `>=4.0` |  - reattachable execute present; `install()` raises outside the range |
| Pyodide | `>=0.28` / Python 3.13 | CONTRIBUTING.md; provides `pyarrow>=22`, `pandas`, `numpy`, `protobuf>=7` |
| `build` | `==1.2.2` | wheel build (`scripts/build_site.sh`, Makefile) |
| `jupyterlite-core` | `==0.6.4` | `jupyter lite` CLI (`scripts/build_site.sh`) |
| `jupyterlite-pyodide-kernel` | `==0.6.1` | Pyodide kernel for the lite site |
| Envoy | `envoyproxy/envoy:v1.31-latest` | grpc_web filter + v3 HttpProtocolOptions |
| Spark Connect server | `apache/spark:4.1.2` | matches the `pyspark` range |

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
      the design notes v0 matrix). Skip-only runs do **not** count as a release gate.

Publish:

- [ ] Tag the release (`vX.Y.Z`); CI builds the wheel on the tag.
- [ ] (If publishing to an index) `twine check dist/*` then upload. The package
      name on the index is `pyspark-connect-web`; the import name is
      `pyspark_connect_web`.
- [ ] Publish the built `_output` site to the cross-origin-isolated host
      (Envoy/static, or a Pages host that honours `_headers`).
- [ ] Smoke-test the published site: open it, confirm `crossOriginIsolated === true`,
      run the demo notebook end to end against a reachable Connect server.

Post-release:

- [ ] Update `README.md` status if the milestone changed.
- [ ] File a `the project notes note for anything that surprised you.
