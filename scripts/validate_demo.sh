#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# validate_demo.sh - static checks for the embedded BI demo (demo/index.html +
# scripts/build_demo_site.sh). No docker, no network, no browser - this is the
# always-on gate the CI `demo` job runs, and it is runnable in the maintainer's
# offline sandbox exactly as CI runs it.
#
# It asserts:
#   1. the page's module <script> is valid JS (`node --check`)
#   2. the embedded PY_BOOTSTRAP (a JS string array) reconstructs to valid Python
#      (`python -m py_compile`) - so a typo in the in-browser snippet fails CI,
#      not a user's first boot
#   3. the page references the same-origin worker assets the SAB bridge needs
#   4. scripts/build_demo_site.sh is valid bash and stages the page into _output/

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DEMO="demo/index.html"
log() { printf '[validate_demo] %s\n' "$*"; }
die() { printf '[validate_demo] ERROR: %s\n' "$*" >&2; exit 1; }

command -v node >/dev/null 2>&1 || die "node not found"
command -v python3 >/dev/null 2>&1 || die "python3 not found"
[ -f "$DEMO" ] || die "$DEMO missing"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# 1. The page's module <script> must be valid JS. Extract it, stub the
#    /worker/bridge.js import (a browser path, not resolvable here), `node --check`.
node -e '
const fs = require("fs");
const html = fs.readFileSync("demo/index.html", "utf8");
const m = html.match(/<script type="module">([\s\S]*?)<\/script>/);
if (!m) { console.error("no module <script> in demo/index.html"); process.exit(1); }
const js = m[1].replace(
  /^\s*import \{ installBridge \} from "\/worker\/bridge\.js";/m,
  "const installBridge = () => {};");
fs.writeFileSync(process.argv[1], js);
' "$tmp/demo.mjs"
node --check "$tmp/demo.mjs"
log "demo page JS: syntax OK"

# 2. The embedded PY_BOOTSTRAP (built as a JS string array, with SPARK_REMOTE /
#    RESULT_MARK interpolated) must be valid Python. Reconstruct it exactly as the
#    page does at runtime and py_compile it.
node -e '
const fs = require("fs");
const html = fs.readFileSync("demo/index.html", "utf8");
const m = html.match(/const PY_BOOTSTRAP = \[([\s\S]*?)\]\.join\("\\n"\);/);
if (!m) { console.error("PY_BOOTSTRAP array not found in demo/index.html"); process.exit(1); }
const SPARK_REMOTE = "sc://localhost:8081/;transport=grpcweb"; // values the page substitutes
const RESULT_MARK = "@@PCW@@";
const arr = eval("[" + m[1] + "]"); // elements include `JSON.stringify(SPARK_REMOTE)` etc.
fs.writeFileSync(process.argv[1], arr.join("\n"));
' "$tmp/bootstrap.py"
python3 -c "import py_compile, sys; py_compile.compile(sys.argv[1], doraise=True)" "$tmp/bootstrap.py"
log "embedded Python bootstrap: compiles OK"

# 3. The page must load the same-origin worker assets the blocking bridge needs.
for needle in '/worker/bridge.js' '/worker/worker_bootstrap.js'; do
  grep -q "$needle" "$DEMO" || die "demo page does not reference $needle"
done
log "demo page references the worker bridge assets"

# 4. The build helper must be valid bash and stage the page into _output/demo.
bash -n scripts/build_demo_site.sh
grep -q 'OUTPUT_DIR/demo' scripts/build_demo_site.sh \
  || die "build_demo_site.sh does not stage demo/ into the output dir"
log "build_demo_site.sh: syntax OK + stages the demo page"

log "all demo static checks passed"
