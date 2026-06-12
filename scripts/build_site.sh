#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# build_site.sh — build the pyspark-connect-web wheel and the JupyterLite site
# into ./_output, ready to be served by deploy/ (Envoy static host with
# COOP/COEP). Offline-capable once the pinned build deps + Pyodide assets are
# cached; there is NO network in CI/this environment, so this script is written
# to be correct + runnable where the deps exist, and fails loudly otherwise.
#
# Usage:
#   scripts/build_site.sh                 # build wheel + lite site into _output
#   PCW_OUTPUT_DIR=dist_site scripts/build_site.sh
#
# Pinned tool versions (see also team/findings-lane5-deploy.md):
#   jupyterlite-core           ==0.6.4    (jupyter lite CLI)
#   jupyterlite-pyodide-kernel ==0.6.1    (Pyodide kernel; Pyodide >=0.28/Py3.13)
#   build                      ==1.2.2    (PEP 517 wheel build)
# These pins must stay compatible with Pyodide >=0.28 / Python 3.13 in the
# browser (COORDINATION.md). Bump deliberately, together.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUTPUT_DIR="${PCW_OUTPUT_DIR:-_output}"
LITE_DIR="pyspark_connect_web/jupyterlite"

# --- pinned build tooling --------------------------------------------------
JUPYTERLITE_CORE_PIN="jupyterlite-core==0.6.4"
JUPYTERLITE_PYODIDE_PIN="jupyterlite-pyodide-kernel==0.6.1"
BUILD_PIN="build==1.2.2"

log() { printf '[build_site] %s\n' "$*"; }
die() { printf '[build_site] ERROR: %s\n' "$*" >&2; exit 1; }

command -v python3 >/dev/null 2>&1 || die "python3 not found"

# --- 0. ensure build tooling (offline if already installed) ----------------
if ! python3 -c "import build" >/dev/null 2>&1; then
  log "installing build tooling ($BUILD_PIN $JUPYTERLITE_CORE_PIN $JUPYTERLITE_PYODIDE_PIN)"
  python3 -m pip install "$BUILD_PIN" "$JUPYTERLITE_CORE_PIN" "$JUPYTERLITE_PYODIDE_PIN"
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
jupyter lite build \
  --config "$LITE_DIR/jupyter-lite.json" \
  --contents "$LITE_DIR/demo.ipynb" \
  --output-dir "$OUTPUT_DIR"

# --- 3. drop in COOP/COEP _headers + the wheel so micropip can fetch it -----
log "copying _headers + wheel into $OUTPUT_DIR"
cp "$LITE_DIR/_headers" "$OUTPUT_DIR/_headers"
cp "$WHEEL" "$OUTPUT_DIR/"

# --- 3b. wire lane 3's bridge JS into the site -----------------------------
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
# run_python_bridge.js is copied for the e2e hook but is NOT auto-wired inside
# JupyterLite (Shape B needs a live kernel connection — the remaining browser
# integration item; see jupyterlite/README.md + team/findings-lane3-bridge.md).
log "copying bridge JS assets into $OUTPUT_DIR (preserving module layout)"
mkdir -p "$OUTPUT_DIR/jupyterlite" "$OUTPUT_DIR/worker"
cp "$LITE_DIR"/pcw_kernel_bridge.js "$LITE_DIR"/run_python_bridge.js \
   "$LITE_DIR"/pcw_runpython_bootstrap.js "$OUTPUT_DIR/jupyterlite/"
cp "$LITE_DIR"/coi-serviceworker.js "$OUTPUT_DIR/coi-serviceworker.js"
cp pyspark_connect_web/worker/*.js "$OUTPUT_DIR/worker/"

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
  || die "COOP missing from $OUTPUT_DIR/_headers (DECISIONS.md #4)"
grep -q 'Cross-Origin-Embedder-Policy: require-corp' "$OUTPUT_DIR/_headers" \
  || die "COEP missing from $OUTPUT_DIR/_headers (DECISIONS.md #4)"
ls "$OUTPUT_DIR"/pyspark_connect_web-*.whl >/dev/null 2>&1 \
  || die "wheel not copied into $OUTPUT_DIR"
[ -f "$OUTPUT_DIR/coi-serviceworker.js" ] && [ -f "$OUTPUT_DIR/jupyterlite/pcw_kernel_bridge.js" ] \
  && [ -f "$OUTPUT_DIR/jupyterlite/pcw_runpython_bootstrap.js" ] \
  && [ -f "$OUTPUT_DIR/jupyterlite/run_python_bridge.js" ] \
  && [ -f "$OUTPUT_DIR/worker/bridge.js" ] \
  || die "bridge JS assets missing from $OUTPUT_DIR (pcw_kernel_bridge import would 404)"

log "done. Serve $OUTPUT_DIR with a host that emits COOP/COEP (deploy/ Envoy does)."
log "  dev:  docker compose -f deploy/compose.yaml up   # serves _output on :8000"
