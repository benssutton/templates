#!/usr/bin/env bash
# Mint a self-signed DEV certificate for HTTPS (key.pem + cert.pem).
# Compose runs it in the `dev-certs` one-shot service.
# CN=localhost; SANs localhost, app (the compose service k6 targets) and
# 127.0.0.1; RSA 4096; 365 days. Never use in production.
# Usage: certs/generate_self_signed_cert.sh [output-dir]   (default ./certs)
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash: keep "/CN=localhost" from becoming a Windows path
out="${1:-./certs}"
mkdir -p "$out"
openssl req -x509 -newkey rsa:4096 -nodes -sha256 -days 365 \
  -keyout "$out/key.pem" -out "$out/cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:app,IP:127.0.0.1"
echo "Done"
