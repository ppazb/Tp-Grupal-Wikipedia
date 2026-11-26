#!/usr/bin/env bash
# Levanta el despliegue, ejecuta Chaos Monkey en paralelo y valida la finalización de los clientes.
set -euo pipefail
cd "$(dirname "$0")/.."

clientes="${1:?Indicar la cantidad de clientes}"
intervalo="${2:-12}"
cantidad="${3:-1}"

if [[ ! "$clientes" =~ ^[1-9][0-9]*$ ]]; then
    echo 'La cantidad de clientes debe ser un entero positivo.' >&2
    exit 2
fi

compose=(docker compose -f docker-compose.yaml --profile clientes)
registro_victimas="$(mktemp -t chaos-victimas.XXXXXX)"
pid_chaos=""

detener_chaos() {
    if [[ -n "$pid_chaos" ]] && kill -0 "$pid_chaos" 2>/dev/null; then
        kill -TERM "$pid_chaos" 2>/dev/null || true
        wait "$pid_chaos" 2>/dev/null || true
    fi
    pid_chaos=""
}

limpiar() {
    detener_chaos
    rm -f "$registro_victimas"
}

interrumpir() {
    codigo="$1"
    detener_chaos
    rm -f "$registro_victimas"
    echo "Espera interrumpida. El despliegue sigue levantado; usar make down para detenerlo." >&2
    exit "$codigo"
}

trap limpiar EXIT
trap 'interrumpir 130' INT
trap 'interrumpir 143' TERM

# Evita dejar directorios parciales de ejecuciones previas en output/.
rm -rf output/client_*

inicio=$SECONDS
"${compose[@]}" up -d --build

nombres_clientes=()
contenedores=()
for ((indice=0; indice<clientes; indice++)); do
    servicio=client
    if ((indice > 0)); then servicio="client-$indice"; fi
    contenedor=$("${compose[@]}" ps --all --quiet "$servicio")
    if [[ -z "$contenedor" || "$contenedor" == *$'\n'* ]]; then
        echo "Se esperaba exactamente un contenedor para $servicio." >&2
        exit 1
    fi
    nombres_clientes+=("$servicio")
    contenedores+=("$contenedor")
done

extra_args=()
if [[ -n "${CHAOS_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    extra_args=(${CHAOS_ARGS})
fi

echo "Iniciando Chaos Monkey en segundo plano (intervalo=${intervalo}s, cantidad=${cantidad})..."
python3 scripts/chaos_monkey.py \
    --compose docker-compose.yaml \
    --espera-inicial 5 \
    --intervalo "$intervalo" \
    --cantidad "$cantidad" \
    --registro "$registro_victimas" \
    "${extra_args[@]}" &
pid_chaos=$!

echo "Esperando la terminación de los $clientes clientes bajo fallas periódicas..."
echo 'Para seguir los logs desde otra terminal: docker compose -f docker-compose.yaml --profile clientes logs -f'

codigos=()
for contenedor in "${contenedores[@]}"; do
    codigos+=("$(docker wait "$contenedor")")
done

# Detenemos Chaos Monkey en cuanto terminan los clientes para congelar el estado final.
detener_chaos

total=$((SECONDS - inicio))
resultado=0
completados=0

echo 'Resumen final de clientes (escenario Chaos Monkey):'
for ((indice=0; indice<clientes; indice++)); do
    codigo="${codigos[$indice]}"
    servicio="${nombres_clientes[$indice]}"
    if [[ "$codigo" == 0 ]]; then
        echo "  client_$indice ($servicio): resultados completos y archivos cerrados."
        completados=$((completados + 1))
    elif grep -qxF "$servicio" "$registro_victimas" && [[ "$codigo" == 137 || "$codigo" == 143 ]]; then
        echo "  client_$indice ($servicio): derribado por Chaos Monkey (salida $codigo); consulta abandonada y salida parcial limpiada."
        rm -rf "output/client_$indice"
    else
        echo "  client_$indice ($servicio): ERROR inesperado (código de salida: $codigo)." >&2
        resultado=1
    fi
done

if ((completados == 0)); then
    echo 'Ningún cliente completó la ejecución (todos fueron interrumpidos o fallaron).' >&2
    resultado=1
fi

total_caidas=$(wc -l < "$registro_victimas" | tr -d ' ')
echo "Resumen de Chaos Monkey: $total_caidas caída(s) abrupta(s) inyectada(s)."
if ((total_caidas > 0)); then
    sort "$registro_victimas" | uniq -c | sed 's/^/  /'
fi

printf 'Tiempo total de punta a punta con fallas: %d min %d s (%d s)\n' $((total / 60)) $((total % 60)) "$total"
if ((resultado == 0)); then
    echo 'Escenario Chaos Monkey superado. Para comparar los clientes sobrevivientes contra serial: bash scripts/comparar_serial.sh --entrada input'
else
    echo 'El escenario Chaos Monkey detectó errores. Revisar los logs del despliegue.' >&2
fi
exit "$resultado"
