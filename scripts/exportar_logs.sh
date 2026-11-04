#!/usr/bin/env bash
# Exporta la salida que Docker conserva, sin detener ni modificar los servicios.
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.yaml --profile clientes)
servicios=$("${compose[@]}" ps --all --services)
if [[ -z "$servicios" ]]; then
    echo 'No hay contenedores para exportar. Ejecutar antes de make down.' >&2
    exit 1
fi
destino=registros/logs
mkdir -p "$destino"
resultado=0
while IFS= read -r servicio; do
    # Cada servicio generado representa una instancia; se conserva el prefijo de Docker.
    archivo="$destino/$servicio.log"
    if "${compose[@]}" logs --no-color --timestamps "$servicio" > "$archivo.incompleto" 2>&1; then
        mv "$archivo.incompleto" "$archivo"
    else
        echo "No se pudieron exportar los logs de $servicio. Ver $archivo.incompleto" >&2
        resultado=1
    fi
done <<< "$servicios"
if ((resultado == 0)); then
    echo "Logs exportados: $destino"
else
    echo "Exportación incompleta: $destino" >&2
fi
exit "$resultado"
