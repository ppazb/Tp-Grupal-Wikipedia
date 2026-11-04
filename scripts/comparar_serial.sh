#!/usr/bin/env bash
# Ejecuta la referencia sin instalar sus dependencias en el host.
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -f scripts/serial/Dockerfile -t wikipedia-comparacion-serial .
mkdir -p registros/comparacion-serial
docker run --rm --user "$(id -u):$(id -g)" \
    -e IMAGE_USER_AGENT \
    -e IMAGE_INTERVAL_SECONDS -e IMAGE_RETRY_SECONDS \
    -e IMAGE_MAX_BACKOFF_SECONDS -e IMAGE_BYTES_PER_SECOND \
    -v "$PWD:/proyecto:ro" \
    -v "$PWD/registros:/proyecto/registros" \
    wikipedia-comparacion-serial "$@"
