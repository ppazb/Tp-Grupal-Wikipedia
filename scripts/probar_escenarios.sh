#!/usr/bin/env bash
# Ejecuta los escenarios en un despliegue exclusivo y elimina sus recursos.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-todos}" in
    todos) export DIRECTORIO_PRUEBAS=tests ;;
    middleware|eof|q4) export DIRECTORIO_PRUEBAS="tests/$1" ;;
    *) echo "Uso: $0 [todos|middleware|eof|q4]" >&2; exit 2 ;;
esac
proyecto="wiki-pruebas-$$"
compose=(docker compose -p "$proyecto" -f tests/compose.yml)

limpiar() {
    resultado=$?
    trap - EXIT
    if ! "${compose[@]}" down --volumes --remove-orphans; then
        if [ "$resultado" -eq 0 ]; then
                echo "Las pruebas pasaron exitosamente, pero no se pudo limpiar el entorno: $proyecto" >&2
        fi
        echo "No se pudo limpiar el despliegue de pruebas: $proyecto" >&2
        resultado=1
    fi
    exit "$resultado"
}
trap limpiar EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up --build --attach escenarios --abort-on-container-exit --exit-code-from escenarios
