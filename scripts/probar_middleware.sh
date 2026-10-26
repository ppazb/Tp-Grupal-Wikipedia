#!/usr/bin/env bash
# Conserva el comando específico del paquete de middleware.
set -euo pipefail
exec bash "$(dirname "$0")/probar_escenarios.sh" middleware
