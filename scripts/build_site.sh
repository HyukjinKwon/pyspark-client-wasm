#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# build_site.sh - build the pyspark-connect-web wheel and the JupyterLite site
# into ./_output, ready to be served by deploy/ (Envoy static host with
# COOP/COEP). Offline-capable once the pinned build deps + Pyodide assets are
# cached; there is NO network in CI/this environment, so this script is written
# to be correct + runnable where the deps exist, and fails loudly otherwise.
#
# Usage:
#   scripts/build_site.sh                 # build wheel + lite site into _output
#   PCW_OUTPUT_DIR=dist_site scripts/build_site.sh
#
# Pinned tool versions:
#   jupyterlite-core           ==0.6.4    (jupyter lite CLI)
#   jupyterlite-pyodide-kernel ==0.6.1    (Pyodide kernel; Pyodide >=0.28/Py3.13)
#   build                      ==1.2.2    (PEP 517 wheel build)
# These pins must stay compatible with Pyodide >=0.28 / Python 3.13 in the
# browser (CONTRIBUTING.md). Bump deliberately, together.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUTPUT_DIR="${PCW_OUTPUT_DIR:-_output}"
# Absolute, so `jupyter lite build --output-dir` is unambiguous when we point
# --lite-dir at a temp dir (a relative _output would otherwise land under it).
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
LITE_DIR="pyspark_connect_web/jupyterlite"

# --- pinned build tooling --------------------------------------------------
# jupyterlite 0.7.x: the kernel uses MODULE workers. (0.6.1 spawned a CLASSIC
# worker, which Pyodide rejects with "Classic web workers are not supported", so
# the lite kernel never booted.) We vendor the EXACT Pyodide this kernel expects
# (derived below; 0.7.2 -> Pyodide 0.29.3), so the kernel and the standalone
# harness load the same same-origin, module-capable build. Bump deliberately,
# together with the e2e.yml / release.yml pins.
JUPYTERLITE_CORE_PIN="jupyterlite-core==0.7.6"
JUPYTERLITE_PYODIDE_PIN="jupyterlite-pyodide-kernel==0.7.2"
BUILD_PIN="build==1.2.2"

log() { printf '[build_site] %s\n' "$*"; }
die() { printf '[build_site] ERROR: %s\n' "$*" >&2; exit 1; }

command -v python3 >/dev/null 2>&1 || die "python3 not found"

# --- 0. ensure build tooling (offline if already installed) ----------------
if ! python3 -c "import build" >/dev/null 2>&1; then
  log "installing build tooling ($BUILD_PIN $JUPYTERLITE_CORE_PIN $JUPYTERLITE_PYODIDE_PIN)"
  # jupyter-server is required by jupyterlite's `contents` addon (`--contents`).
  python3 -m pip install "$BUILD_PIN" "$JUPYTERLITE_CORE_PIN" "$JUPYTERLITE_PYODIDE_PIN" "jupyter-server>=2,<3"
fi
command -v jupyter >/dev/null 2>&1 || die "jupyter CLI not found (pip install $JUPYTERLITE_CORE_PIN)"

# --- 1. build the wheel micropip installs in the browser -------------------
log "building wheel (python -m build --wheel)"
python3 -m build --wheel --outdir dist
WHEEL="$(ls -t dist/pyspark_connect_web-*.whl 2>/dev/null | head -1)" \
  || die "no wheel produced in dist/"
[ -n "$WHEEL" ] || die "no wheel produced in dist/"
log "wheel: $WHEEL"

# --- 2. build the JupyterLite site -----------------------------------------
log "jupyter lite build -> $OUTPUT_DIR"
# Build from an ISOLATED lite-dir containing only jupyter-lite.json. Passing the
# config by its deep in-package path makes jupyterlite's `lite` addon mirror that
# path under _output (e.g. _output/pyspark_connect_web/jupyterlite/...) and fail
# with FileNotFoundError. A clean lite-dir keeps the config at the root so it is
# patched to _output/jupyter-lite.json, and avoids scanning stray package files.
LITE_BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$LITE_BUILD_DIR"' EXIT
cp "$LITE_DIR/jupyter-lite.json" "$LITE_BUILD_DIR/jupyter-lite.json"
jupyter lite build \
  --lite-dir "$LITE_BUILD_DIR" \
  --contents "$LITE_DIR/demo.ipynb" \
  --output-dir "$OUTPUT_DIR"

# --- 2b. vendor Pyodide SAME-ORIGIN -----------------------------------------
# Under COOP/COEP the worker cannot import Pyodide from a cross-origin CDN
# (jsdelivr sends no CORP; credentialless still blocks the worker script load),
# so we host the full Pyodide dist same-origin. Both consumers load it from
# /pyodide/: the JupyterLite kernel (jupyter-lite.json -> pyodideUrl
# /pyodide/pyodide.js) and the standalone harness (worker_bootstrap.js -> the
# /pyodide/ index). The full dist also carries pyarrow/pandas/numpy/zstandard.
# Skipped if already vendored (offline-friendly); needs network on first build.
# Use the EXACT Pyodide the installed jupyterlite-pyodide-kernel expects, so the
# kernel finds its packages and the harness loads the same module-capable build.
PYODIDE_VER="${PCW_PYODIDE_VERSION:-$(python3 - <<'PY' 2>/dev/null || true
try:
    from jupyterlite_pyodide_kernel.constants import PYODIDE_VERSION as v
    print(v)
except Exception:
    pass
PY
)}"
PYODIDE_VER="${PYODIDE_VER:-314.0.0}"
log "kernel expects Pyodide ${PYODIDE_VER}"
if [ -f "$OUTPUT_DIR/pyodide/pyodide.js" ]; then
  log "Pyodide already vendored at $OUTPUT_DIR/pyodide (skipping download)"
else
  log "vendoring Pyodide ${PYODIDE_VER} into $OUTPUT_DIR/pyodide"
  command -v curl >/dev/null 2>&1 \
    || die "curl needed to download Pyodide (or pre-place the dist in $OUTPUT_DIR/pyodide/)"
  pyo_url="https://github.com/pyodide/pyodide/releases/download/${PYODIDE_VER}/pyodide-${PYODIDE_VER}.tar.bz2"
  curl -fsSL --retry 3 "$pyo_url" -o "$LITE_BUILD_DIR/pyodide.tar.bz2" \
    || die "failed to download Pyodide ${PYODIDE_VER} from $pyo_url"
  tar xjf "$LITE_BUILD_DIR/pyodide.tar.bz2" -C "$OUTPUT_DIR"   # -> $OUTPUT_DIR/pyodide/
  [ -f "$OUTPUT_DIR/pyodide/pyodide.js" ] || die "Pyodide not vendored (no pyodide.js)"
fi

# --- 3. drop in COOP/COEP _headers + the wheel so micropip can fetch it -----
log "copying _headers + wheel into $OUTPUT_DIR"
cp "$LITE_DIR/_headers" "$OUTPUT_DIR/_headers"
cp "$WHEEL" "$OUTPUT_DIR/"

# --- 3b. wire the bridge JS into the site -----------------------------
# Two of the three scripts SELF-INSTALL on load and must run BEFORE the
# JupyterLite app reads `Worker` off the global scope:
#   - coi-serviceworker.js  : registers a SW that injects COOP/COEP (header-less
#                             hosts like GitHub Pages); harmless when Envoy/_headers
#                             already set them. Served at site root for SW scope.
#   - pcw_kernel_bridge.js  : wraps globalThis.Worker so every kernel worker gets
#                             the SAB Bridge. It `import`s "../worker/bridge.js",
#                             so the jupyterlite/ <-> worker/ relative layout MUST
#                             be preserved in the output (hence the two dirs below
#                             + the /jupyterlite/... site-root-absolute src).
# run_python_bridge.js (Shape B) IS auto-wired inside JupyterLite by
# pcw_runpython_bootstrap.js (injected below): it starts a kernel and binds
# window.__pcwRunPython to it. Covered by tests/e2e/kernel.spec.ts.
log "copying bridge JS assets into $OUTPUT_DIR (preserving module layout)"
mkdir -p "$OUTPUT_DIR/jupyterlite" "$OUTPUT_DIR/worker"
cp "$LITE_DIR"/pcw_kernel_bridge.js "$LITE_DIR"/run_python_bridge.js \
   "$LITE_DIR"/pcw_runpython_bootstrap.js "$OUTPUT_DIR/jupyterlite/"
cp "$LITE_DIR"/coi-serviceworker.js "$OUTPUT_DIR/coi-serviceworker.js"
cp pyspark_connect_web/worker/*.js "$OUTPUT_DIR/worker/"
# Standalone e2e harness page (boots the worker + binds `spark` + exposes
# window.__pcwRunPython without JupyterLite); served at the site root.
cp "$LITE_DIR"/harness.html "$OUTPUT_DIR/harness.html"

log "injecting self-installing bridge <script> tags into emitted HTML"
PCW_OUTPUT_DIR="$OUTPUT_DIR" python3 - <<'PY'
import os, pathlib
out = pathlib.Path(os.environ["PCW_OUTPUT_DIR"])
MARK = "pcw bridge (injected by build_site.sh)"
TAGS = (
    f"\n<!-- {MARK} -->"
    '\n<script src="/coi-serviceworker.js"></script>'
    '\n<script type="module" src="/jupyterlite/pcw_kernel_bridge.js"></script>'
    '\n<script type="module" src="/jupyterlite/pcw_runpython_bootstrap.js"></script>\n'
)
n = 0
for html in out.rglob("*.html"):
    text = html.read_text(encoding="utf-8")
    if MARK in text or "</head>" not in text:
        continue
    html.write_text(text.replace("</head>", TAGS + "</head>", 1), encoding="utf-8")
    n += 1
print(f"[build_site] injected bridge scripts into {n} html file(s)")
if n == 0:
    raise SystemExit("[build_site] ERROR: no HTML files got the bridge injection")
PY

# --- 4. sanity checks (these must hold for the bridge to work) -------------
grep -q 'Cross-Origin-Opener-Policy: same-origin' "$OUTPUT_DIR/_headers" \
  || die "COOP missing from $OUTPUT_DIR/_headers"
grep -q 'Cross-Origin-Embedder-Policy: credentialless' "$OUTPUT_DIR/_headers" \
  || die "COEP missing from $OUTPUT_DIR/_headers"
ls "$OUTPUT_DIR"/pyspark_connect_web-*.whl >/dev/null 2>&1 \
  || die "wheel not copied into $OUTPUT_DIR"
[ -f "$OUTPUT_DIR/pyodide/pyodide.js" ] \
  || die "Pyodide not vendored same-origin at $OUTPUT_DIR/pyodide/ (CDN is blocked under COEP)"
[ -f "$OUTPUT_DIR/coi-serviceworker.js" ] && [ -f "$OUTPUT_DIR/jupyterlite/pcw_kernel_bridge.js" ] \
  && [ -f "$OUTPUT_DIR/jupyterlite/pcw_runpython_bootstrap.js" ] \
  && [ -f "$OUTPUT_DIR/jupyterlite/run_python_bridge.js" ] \
  && [ -f "$OUTPUT_DIR/worker/bridge.js" ] \
  && [ -f "$OUTPUT_DIR/worker/worker_bootstrap.js" ] \
  && [ -f "$OUTPUT_DIR/harness.html" ] \
  || die "bridge JS assets / harness.html missing from $OUTPUT_DIR"

log "done. Serve $OUTPUT_DIR with a host that emits COOP/COEP (deploy/ Envoy does)."
log "  dev:  docker compose -f deploy/compose.yaml up   # serves _output on :8000"
