#!/usr/bin/env python3
"""Chaos Monkey para inyectar caídas abruptas (SIGKILL) en nodos del sistema."""

import argparse
from dataclasses import dataclass
from fnmatch import fnmatchcase
import json
import logging
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

LOG = logging.getLogger("chaos_monkey")
RAIZ = Path(__file__).resolve().parents[1]
COMPOSE_DEFECTO = RAIZ / "docker-compose.yaml"

ALIAS_CATEGORIAS = {
    "@workers": "worker",
    "@gateway": "gateway",
    "@health_checkers": "health_checker",
    "@clientes": "cliente",
}


@dataclass(frozen=True)
class TopologiaChaos:
    """Clasificación de contenedores candidatos a fallas (excluye broker e inicializador)."""

    workers: Tuple[str, ...]
    gateways: Tuple[str, ...]
    health_checkers: Tuple[str, ...]
    clientes: Tuple[str, ...]

    @property
    def todos(self) -> Tuple[str, ...]:
        return tuple(sorted(
            set(self.workers) | set(self.gateways) | set(self.health_checkers) | set(self.clientes)
        ))

    def categoria(self, nodo: str) -> str:
        if nodo in self.health_checkers:
            return "health_checker"
        if nodo in self.gateways:
            return "gateway"
        if nodo in self.clientes:
            return "cliente"
        if nodo in self.workers:
            return "worker"
        raise KeyError(f"Nodo desconocido en la topología: {nodo}")

    def por_categoria(self, nombre_categoria: str) -> Tuple[str, ...]:
        mapa = {
            "worker": self.workers,
            "gateway": self.gateways,
            "health_checker": self.health_checkers,
            "cliente": self.clientes,
        }
        return mapa[nombre_categoria]


def _normalizar_entorno(entorno_crudo) -> Dict[str, str]:
    if isinstance(entorno_crudo, dict):
        return {str(k): str(v) for k, v in entorno_crudo.items() if v is not None}
    if isinstance(entorno_crudo, list):
        resultado = {}
        for item in entorno_crudo:
            texto = str(item)
            clave, sep, valor = texto.partition("=")
            resultado[clave] = valor if sep else ""
        return resultado
    return {}


def extraer_topologia(documento_compose: dict) -> TopologiaChaos:
    """Extrae workers, gateway, health_checkers y clientes; ignora rabbitmq e inicializador."""
    servicios = documento_compose.get("services")
    if not isinstance(servicios, dict):
        raise ValueError("El archivo Compose no contiene una sección 'services' válida")

    workers, gateways, health_checkers, clientes = [], [], [], []
    for nombre, servicio in servicios.items():
        if not isinstance(servicio, dict):
            continue
        contenedor = str(servicio.get("container_name") or nombre).strip()
        entorno = _normalizar_entorno(servicio.get("environment"))
        if "HEALTH_CHECKER_ID" in entorno:
            health_checkers.append(contenedor)
        elif "GATEWAY_GROUP" in entorno:
            gateways.append(contenedor)
        elif "WIKI_MW_PROCESO_GRUPO" in entorno:
            workers.append(contenedor)
        elif "CLIENT_ID" in entorno:
            clientes.append(contenedor)

    return TopologiaChaos(
        workers=tuple(sorted(set(workers))),
        gateways=tuple(sorted(set(gateways))),
        health_checkers=tuple(sorted(set(health_checkers))),
        clientes=tuple(sorted(set(clientes))),
    )


def cargar_topologia_compose(ruta_compose: Path) -> TopologiaChaos:
    """Resuelve el modelo efectivo de Docker Compose sin depender de bibliotecas externas."""
    resultado = subprocess.run(
        [
            "docker", "compose", "-f", str(ruta_compose),
            "--profile", "clientes", "config", "--format", "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return extraer_topologia(json.loads(resultado.stdout))


def resolver_filtro_nodos(expresion: Optional[str], topologia: TopologiaChaos) -> Set[str]:
    """Convierte una lista separada por comas (nombres, comodines o @categoria) en nodos."""
    if not expresion or not expresion.strip():
        return set()
    universo = topologia.todos
    seleccionados: Set[str] = set()
    for parte in expresion.split(","):
        patron = parte.strip()
        if not patron:
            continue
        if patron in ALIAS_CATEGORIAS:
            coincidencias = set(topologia.por_categoria(ALIAS_CATEGORIAS[patron]))
        elif any(c in patron for c in "*?[]"):
            coincidencias = {n for n in universo if fnmatchcase(n, patron)}
        elif patron in universo:
            coincidencias = {patron}
        else:
            coincidencias = set()

        if not coincidencias:
            raise ValueError(
                f"El filtro '{patron}' no coincide con ningún nodo elegible. "
                f"Disponibles: {', '.join(universo)}"
            )
        seleccionados.update(coincidencias)
    return seleccionados


class AdaptadorDockerCLI:
    """Ejecuta consultas de estado y envíos de señales sobre contenedores vía Docker CLI."""

    def nodos_corriendo(self, candidatos: Sequence[str]) -> Set[str]:
        if not candidatos:
            return set()
        resultado = subprocess.run(
            ["docker", "ps", "--filter", "status=running", "--format", "{{.Names}}"],
            check=True,
            capture_output=True,
            text=True,
        )
        activos = {linea.strip() for linea in resultado.stdout.splitlines() if linea.strip()}
        return activos & set(candidatos)

    def matar(self, contenedor: str, senal: str = "KILL") -> bool:
        resultado = subprocess.run(
            ["docker", "kill", "-s", senal, contenedor],
            capture_output=True,
            text=True,
        )
        if resultado.returncode == 0:
            return True
        detalle = (resultado.stderr or resultado.stdout).strip()
        LOG.warning("No se pudo terminar '%s' con señal %s: %s", contenedor, senal, detalle)
        return False


class SelectorVictimas:
    """Selecciona víctimas respetando cooldown y mínimos vivos de health checkers y clientes."""

    def __init__(
        self,
        topologia: TopologiaChaos,
        rng: random.Random,
        cooldown: float = 20.0,
        min_health_checkers: int = 1,
        min_clientes: int = 1,
        permitidos: Optional[Iterable[str]] = None,
        excluidos: Optional[Iterable[str]] = None,
    ):
        if min_health_checkers < 1:
            raise ValueError("Debe permanecer al menos 1 health_checker vivo en todo momento")
        if min_clientes < 0:
            raise ValueError("min_clientes no puede ser negativo")
        if cooldown < 0:
            raise ValueError("El cooldown no puede ser negativo")

        self.topologia = topologia
        self.rng = rng
        self.cooldown = cooldown
        self.min_health_checkers = min_health_checkers
        self.min_clientes = min_clientes
        self.permitidos = set(permitidos) if permitidos is not None else None
        self.excluidos = set(excluidos or ())
        self._ultimo_ataque: Dict[str, float] = {}
        self._conjunto_hc = set(topologia.health_checkers)
        self._conjunto_clientes = set(topologia.clientes)

    def elegir(self, corriendo: Set[str], cantidad: int, ahora: float) -> List[str]:
        if cantidad <= 0:
            return []

        hc_vivos = len(corriendo & self._conjunto_hc)
        clientes_vivos = len(corriendo & self._conjunto_clientes)

        candidatos = [
            nodo for nodo in self.topologia.todos
            if nodo in corriendo
            and (self.permitidos is None or nodo in self.permitidos)
            and nodo not in self.excluidos
            and (
                nodo not in self._ultimo_ataque
                or (ahora - self._ultimo_ataque[nodo]) >= self.cooldown
            )
        ]
        self.rng.shuffle(candidatos)

        elegidos: List[str] = []
        for nodo in candidatos:
            if len(elegidos) >= cantidad:
                break
            if nodo in self._conjunto_hc:
                if hc_vivos <= self.min_health_checkers:
                    continue
                hc_vivos -= 1
            elif nodo in self._conjunto_clientes:
                if clientes_vivos <= self.min_clientes:
                    continue
                clientes_vivos -= 1
            elegidos.append(nodo)

        return elegidos

    def registrar_ataque(self, nodo: str, instante: float) -> None:
        self._ultimo_ataque[nodo] = instante


def _registrar_en_archivo(ruta: Path, contenedor: str) -> None:
    datos = f"{contenedor}\n".encode("utf-8")
    fd = os.open(ruta, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        total = 0
        while total < len(datos):
            escritos = os.write(fd, datos[total:])
            if escritos <= 0:
                raise OSError(f"Escritura incompleta en el registro {ruta}")
            total += escritos
    finally:
        os.close(fd)


def ejecutar_chaos(
    selector: SelectorVictimas,
    docker,
    detener: threading.Event,
    intervalo: float = 12.0,
    espera_inicial: float = 5.0,
    cantidad: int = 1,
    rondas: int = 0,
    senal: str = "KILL",
    simulacion: bool = False,
    ruta_registro: Optional[Path] = None,
    reloj=time.monotonic,
) -> int:
    """Ejecuta el bucle periódico de inyección de fallas hasta recibir señal o agotar rondas."""
    if espera_inicial > 0 and detener.wait(timeout=espera_inicial):
        return 0

    ronda = 0
    total_derribos = 0
    universo = selector.topologia.todos

    while not detener.is_set():
        ronda += 1
        corriendo = docker.nodos_corriendo(universo)
        ahora = reloj()
        victimas = selector.elegir(corriendo, cantidad=cantidad, ahora=ahora)

        if not victimas:
            LOG.info(
                "[Ronda %d] Sin víctimas elegibles en este ciclo (activos=%d, respetando cooldown y mínimos vivos).",
                ronda,
                len(corriendo),
            )
        for victima in victimas:
            categoria = selector.topologia.categoria(victima)
            if simulacion:
                selector.registrar_ataque(victima, ahora)
                total_derribos += 1
                LOG.info("[Ronda %d] [SIMULACIÓN] Se enviaría SIG%s a '%s' (%s)", ronda, senal, victima, categoria)
                continue

            LOG.warning("[Ronda %d] Matando contenedor '%s' (%s) con SIG%s...", ronda, victima, categoria, senal)
            if docker.matar(victima, senal=senal):
                selector.registrar_ataque(victima, ahora)
                total_derribos += 1
                if ruta_registro is not None:
                    _registrar_en_archivo(ruta_registro, victima)

        if rondas > 0 and ronda >= rondas:
            break
        if detener.wait(timeout=intervalo):
            break

    LOG.info("Chaos Monkey finalizado limpiamente tras %d ronda(s) y %d caída(s) inyectada(s).", ronda, total_derribos)
    return 0


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose", type=Path, default=COMPOSE_DEFECTO,
                        help="Archivo Compose del despliegue (por defecto: %(default)s)")
    parser.add_argument("--intervalo", type=float, default=12.0,
                        help="Segundos entre rondas de fallas (por defecto: %(default)s)")
    parser.add_argument("--espera-inicial", type=float, default=5.0,
                        help="Segundos antes de la primera falla (por defecto: %(default)s)")
    parser.add_argument("--cantidad", type=int, default=1,
                        help="Cantidad máxima de contenedores a derribar por ronda (por defecto: %(default)s)")
    parser.add_argument("--cooldown", type=float, default=20.0,
                        help="Segundos mínimos antes de volver a elegir el mismo nodo (por defecto: %(default)s)")
    parser.add_argument("--rondas", type=int, default=0,
                        help="Cantidad de rondas a ejecutar; 0 para correr hasta SIGINT/SIGTERM (por defecto: %(default)s)")
    parser.add_argument("--semilla", type=int, default=None,
                        help="Semilla opcional para reproducir exactamente la secuencia aleatoria")
    parser.add_argument("--objetivos", type=str, default=None,
                        help="Lista opcional separada por comas de nodos, comodines (ej. client*) o @workers/@gateway/@health_checkers/@clientes")
    parser.add_argument("--excluir", type=str, default=None,
                        help="Lista opcional separada por comas de nodos o categorías a excluir")
    parser.add_argument("--min-health-checkers", type=int, default=1,
                        help="Mínimo de nodos health_checker que deben permanecer vivos (mínimo 1; por defecto: %(default)s)")
    parser.add_argument("--min-clientes", type=int, default=None,
                        help="Mínimo de clientes vivos (por defecto: 1 en modo aleatorio, 0 si --objetivos apunta solo a clientes)")
    parser.add_argument("--senal", type=str, default="KILL",
                        help="Señal enviada con docker kill (por defecto: %(default)s)")
    parser.add_argument("--registro", type=Path, default=None,
                        help="Archivo opcional donde registrar una línea por cada contenedor derribado")
    parser.add_argument("--simulacion", action="store_true",
                        help="Muestra qué víctimas elegiría sin ejecutar docker kill")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = construir_parser()
    args = parser.parse_args(argv)

    if args.intervalo <= 0:
        parser.error("--intervalo debe ser mayor que cero")
    if args.espera_inicial < 0:
        parser.error("--espera-inicial no puede ser negativa")
    if args.cantidad < 1:
        parser.error("--cantidad debe ser un entero positivo")
    if args.rondas < 0:
        parser.error("--rondas no puede ser negativo")
    if args.min_health_checkers < 1:
        parser.error("--min-health-checkers debe ser al menos 1 para que el sistema pueda recuperarse")

    try:
        topologia = cargar_topologia_compose(args.compose.resolve())
        permitidos = resolver_filtro_nodos(args.objetivos, topologia) if args.objetivos else None
        excluidos = resolver_filtro_nodos(args.excluir, topologia) if args.excluir else set()

        if args.min_clientes is not None:
            min_clientes = args.min_clientes
        elif permitidos and permitidos.issubset(set(topologia.clientes)):
            min_clientes = 0
        else:
            min_clientes = 1

        rng = random.Random(args.semilla)
        selector = SelectorVictimas(
            topologia=topologia,
            rng=rng,
            cooldown=args.cooldown,
            min_health_checkers=args.min_health_checkers,
            min_clientes=min_clientes,
            permitidos=permitidos,
            excluidos=excluidos,
        )
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        detalle = error.stderr.strip() if isinstance(error, subprocess.CalledProcessError) and error.stderr else str(error)
        LOG.error("No se pudo inicializar Chaos Monkey: %s", detalle)
        return 2

    detener = threading.Event()

    def _manejar_senal(signum, _frame):
        LOG.info("Señal %s recibida; deteniendo Chaos Monkey...", signal.Signals(signum).name)
        detener.set()

    signal.signal(signal.SIGINT, _manejar_senal)
    signal.signal(signal.SIGTERM, _manejar_senal)

    LOG.info(
        "Chaos Monkey activo | candidatos=%d (workers=%d, gateway=%d, hc=%d, clientes=%d) | "
        "intervalo=%.1fs | cantidad=%d | cooldown=%.1fs | min_hc=%d | min_clientes=%d%s",
        len(topologia.todos),
        len(topologia.workers),
        len(topologia.gateways),
        len(topologia.health_checkers),
        len(topologia.clientes),
        args.intervalo,
        args.cantidad,
        args.cooldown,
        args.min_health_checkers,
        min_clientes,
        f" | semilla={args.semilla}" if args.semilla is not None else "",
    )

    try:
        return ejecutar_chaos(
            selector=selector,
            docker=AdaptadorDockerCLI(),
            detener=detener,
            intervalo=args.intervalo,
            espera_inicial=args.espera_inicial,
            cantidad=args.cantidad,
            rondas=args.rondas,
            senal=args.senal,
            simulacion=args.simulacion,
            ruta_registro=args.registro,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        LOG.error("Error durante la ejecución de Chaos Monkey: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
