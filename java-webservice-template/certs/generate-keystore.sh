#!/usr/bin/env bash
# Generate a self-signed PKCS12 keystore for local/dev HTTPS. SAN includes
# localhost + 'app' so in-network containers (Prometheus, k6) validate the host.
# Mirrors the Python template's certs/generate_self_signed_cert.py.
set -euo pipefail
OUT="${1:-certs/keystore.p12}"
PASS="${KEYSTORE_PASSWORD:-changeit}"
keytool -genkeypair -alias app -keyalg RSA -keysize 2048 -validity 3650 \
  -storetype PKCS12 -keystore "$OUT" -storepass "$PASS" \
  -dname "CN=localhost, OU=template, O=example, L=local, ST=local, C=US" \
  -ext "SAN=dns:localhost,dns:app,ip:127.0.0.1"
echo "Wrote $OUT"
