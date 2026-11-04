#!/usr/bin/env bash
# Espera la terminación de cada cliente sin deducirla de sus logs.
set -euo pipefail
cd "$(dirname "$0")/.."
clientes="${1:?Indicar la cantidad de clientes}"
if [[ ! "$clientes" =~ ^[1-9][0-9]*$ ]]; then
    echo 'La cantidad de clientes debe ser un entero positivo.' >&2
    exit 2
fi
compose=(docker compose -f docker-compose.yaml --profile clientes)
trap 'echo "Espera interrumpida. El despliegue sigue levantado; usar make down para detenerlo." >&2; exit 130' INT
trap 'echo "Espera interrumpida. El despliegue sigue levantado; usar make down para detenerlo." >&2; exit 143' TERM

"${compose[@]}" up -d --build
contenedores=()
for ((indice=0; indice<clientes; indice++)); do
    servicio=client
    if ((indice > 0)); then servicio="client-$indice"; fi
    contenedor=$("${compose[@]}" ps --all --quiet "$servicio")
    if [[ -z "$contenedor" || "$contenedor" == *$'\n'* ]]; then
        echo "Se esperaba exactamente un contenedor para $servicio." >&2
        exit 1
    fi
    contenedores+=("$contenedor")
done

echo "Esperando que terminen los $clientes clientes. No es necesario interrumpir este comando."
echo 'Para seguir los logs desde otra terminal: docker compose -f docker-compose.yaml --profile clientes logs -f'
codigos=()
for contenedor in "${contenedores[@]}"; do
    # Docker bloquea hasta la salida del contenedor; también informa si ya terminó.
    codigos+=("$(docker wait "$contenedor")")
done

resultado=0
echo 'Resumen final de clientes:'
for ((indice=0; indice<clientes; indice++)); do
    codigo="${codigos[$indice]}"
    if [[ "$codigo" == 0 ]]; then
        echo "  client_$indice: resultados completos y archivos cerrados."
    else
        echo "  client_$indice: ERROR (código de salida: $codigo)." >&2
        resultado=1
    fi
done
if ((resultado == 0)); then
    echo 'Todos los clientes terminaron correctamente. Ya se puede ejecutar make down y make comparar-serial.'
else
    echo 'La ejecución no terminó correctamente. Revisar los logs antes de comparar resultados.' >&2
fi
exit "$resultado"
