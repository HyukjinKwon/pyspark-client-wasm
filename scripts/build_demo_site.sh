#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# build_demo_site.sh - build the JupyterLite site (scripts/build_site.sh) and
# drop the "Embedded BI query cell" demo page (demo/index.html) into it, so the
# deploy/ Envoy static host serves it cross-origin isolated alongside the
# same-origin /worker/, /pyodide/ and *.whl assets the browser client needs.
#
#   scripts/build_demo_site.sh
#   docker compose -f deploy/compose.yaml up
#   open http://localhost:8000/demo/
#
# PCW_OUTPUT_DIR overrides the output dir (default _output), same as build_site.sh.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUTPUT_DIR="${PCW_OUTPUT_DIR:-_output}"

# 1. Build the wheel + JupyterLite site + vendored Pyodide + COOP/COEP _headers.
scripts/build_site.sh

# 2. Lay the BI demo page into the served root at /demo/. It references the
#    site-root-absolute /worker/, /pyodide/ and *.whl assets that build_site.sh
#    just produced, so it only needs to live somewhere under the same origin.
mkdir -p "$OUTPUT_DIR/demo"
cp demo/index.html "$OUTPUT_DIR/demo/index.html"

echo
echo "[build_demo_site] BI demo staged at $OUTPUT_DIR/demo/index.html"
echo "[build_demo_site] next:"
echo "    docker compose -f deploy/compose.yaml up        # Spark Connect + Envoy + static"
echo "    open http://localhost:8000/demo/                # the embedded BI query cell"
