#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# render_envoy_prod.sh — produce a deploy-ready envoy.prod.yaml with your real
# public host + allowed CORS origin substituted in, instead of editing the
# checked-in template by hand.
#
# Usage:
#   PCW_PUBLIC_HOST=spark.example.com \
#   PCW_LITE_ORIGIN=https://lite.example.com \
#   scripts/render_envoy_prod.sh > deploy/envoy.prod.rendered.yaml
#
# Then point compose at the rendered file (or bind-mount it as envoy.prod.yaml).
# The rendered file is gitignored output — do not commit secrets/origins.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$REPO_ROOT/deploy/envoy.prod.yaml"

: "${PCW_PUBLIC_HOST:?set PCW_PUBLIC_HOST (e.g. spark.example.com)}"
: "${PCW_LITE_ORIGIN:?set PCW_LITE_ORIGIN (e.g. https://lite.example.com)}"

# Strip any trailing slash from the origin (CORS origins must have none).
PCW_LITE_ORIGIN="${PCW_LITE_ORIGIN%/}"

sed \
  -e "s|YOUR-PUBLIC-HOST.example.com|${PCW_PUBLIC_HOST}|g" \
  -e "s|https://YOUR-LITE-ORIGIN.example.com|${PCW_LITE_ORIGIN}|g" \
  "$SRC"
