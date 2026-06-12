<!-- SPDX-License-Identifier: Apache-2.0 -->

# Installation

There are two distinct environments to keep straight:

1. **A local Python environment** - for development, building the wheel, running
   the unit tests, and generating e2e reference results. We recommend **conda**
   for this.
2. **The browser (Pyodide/JupyterLite) environment** - where the package
   actually runs. Here the package is installed with `micropip`, and `pyspark` /
   `pyarrow` / `pandas` / `protobuf` come from Pyodide, not from your machine.

`pyspark-connect-web` is a pure-Python wheel (`py3-none-any`, no compiled
extensions, no `grpcio`) so it imports cleanly under Pyodide/WASM.

## Local environment with conda

Create an isolated conda environment and install the package with `pip` inside
it. Conda manages the environment; `pip` installs the package itself.

```bash
conda create -n pcw python=3.11
conda activate pcw
pip install pyspark-connect-web
```

To verify the install:

```bash
python -c "import pyspark_connect_web as pcw; print(pcw.__version__)"
```

For development work (unit tests, the reference generator, linting), install the
dev extras into the same conda env:

```bash
conda create -n pcw python=3.11
conda activate pcw
pip install "pyspark-connect-web[dev]"
```

The `dev` extras pull in `pyspark>=4.0`, `pyarrow>=22`, `pandas`,
`protobuf>=7`, `googleapis-common-protos`, and `pytest`. Note that `grpcio` is
intentionally **not** a dependency - the package never imports it, mirroring the
Pyodide environment (see [Architecture](architecture.md) and `the design notes` #1).

!!! note "Why conda for the env but pip for the package?"
    The package is published to PyPI as a wheel, so `pip install` is the right
    way to install it. Conda is used only to give you a clean, reproducible
    Python interpreter and environment to install it *into*. If you prefer
    `python -m venv`, that works too - only the env-management tool differs.

## Supported PySpark version

`install()` is version-guarded to **`pyspark>=4.0`** (`the design notes` #3). The
patch depends on private internals of `SparkConnectClient` /
`DefaultChannelBuilder` that are only pinned for that range; calling `install()`
on an unsupported `pyspark` raises `UnsupportedPySparkError`.

## Browser / JupyterLite install

In a Pyodide worker (for example the JupyterLite kernel), the wheel is installed
by URL alongside the pinned runtime deps via `micropip`:

```python
import micropip
await micropip.install("protobuf>=7")
await micropip.install("googleapis-common-protos>=1.56.4")
# The slim Spark Connect client (`pyspark-client`: pure-Python, no JVM/py4j).
# deps=False - its grpcio/grpcio-status base deps have no Pyodide wheel and are
# stubbed by pyspark-connect-web's _grpc_shim. pyarrow/pandas/numpy/zstandard come
# from Pyodide (loadPackage). Host the wheel same-origin (built in CI).
await micropip.install("https://<your-lite-origin>/pyspark_client-4.1.2-py3-none-any.whl", deps=False)
await micropip.install("https://<your-lite-origin>/pyspark_connect_web-<version>-py3-none-any.whl")
```

Then, in a notebook cell:

```python
import pyspark_connect_web as pcw
pcw.install()          # idempotent; monkey-patches the Connect stub
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://<host>:8081/;transport=grpcweb").getOrCreate()
```

Under COEP `require-corp`, a cross-origin CDN wheel must send
`Cross-Origin-Resource-Policy` or the import is blocked - so the build copies the
wheel into the JupyterLite site root and serves it same-origin. See
[JupyterLite hosting](jupyterlite-hosting.md) and
[Packaging & release](packaging-release.md) for the full build flow.

## Distribution vs import name

* **Distribution / PyPI name:** `pyspark-connect-web` (what you `pip install`).
* **Import / package name:** `pyspark_connect_web` (what you `import`).
