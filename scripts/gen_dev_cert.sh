#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# gen_dev_cert.sh — generate a SELF-SIGNED TLS cert for STAGING/local-TLS testing
# of the prod Envoy overlay. NOT for production: browsers will warn, and a
# self-signed cert is not a secure context users should trust. Use a real cert
# (Let's Encrypt / your CA) in production.
#
# Writes deploy/certs/tls.crt + deploy/certs/tls.key, which compose.prod.yaml
# bind-mounts into Envoy at /etc/envoy/certs.
#
# Usage:
#   PCW_PUBLIC_HOST=localhost scripts/gen_dev_cert.sh

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$REPO_ROOT/deploy/certs"
HOST="${PCW_PUBLIC_HOST:-localhost}"

command -v openssl >/dev/null 2>&1 || { echo "openssl not found" >&2; exit 1; }
mkdir -p "$CERT_DIR"

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$CERT_DIR/tls.key" \
  -out "$CERT_DIR/tls.crt" \
  -subj "/CN=${HOST}" \
  -addext "subjectAltName=DNS:${HOST},DNS:localhost,IP:127.0.0.1"

chmod 600 "$CERT_DIR/tls.key"
echo "[gen_dev_cert] wrote $CERT_DIR/tls.crt and tls.key (CN=${HOST}, self-signed)"
echo "[gen_dev_cert] WARNING: self-signed — staging/testing only, never production."
