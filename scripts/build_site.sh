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

# --- 4. sanity checks (these must hold for the bridge to work) -------------
grep -q 'Cross-Origin-Opener-Policy: same-origin' "$OUTPUT_DIR/_headers" \
  || die "COOP missing from $OUTPUT_DIR/_headers (DECISIONS.md #4)"
grep -q 'Cross-Origin-Embedder-Policy: require-corp' "$OUTPUT_DIR/_headers" \
  || die "COEP missing from $OUTPUT_DIR/_headers (DECISIONS.md #4)"
ls "$OUTPUT_DIR"/pyspark_connect_web-*.whl >/dev/null 2>&1 \
  || die "wheel not copied into $OUTPUT_DIR"

log "done. Serve $OUTPUT_DIR with a host that emits COOP/COEP (deploy/ Envoy does)."
log "  dev:  docker compose -f deploy/compose.yaml up   # serves _output on :8000"
