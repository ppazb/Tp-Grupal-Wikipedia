"""Emisor periódico de latidos (heartbeats) por UDP hacia los nodos Health Checker."""

from contextlib import contextmanager
import logging
import os
import socket
import threading
from typing import Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

PREFIJO_LATIDO = "HEARTBEAT"
INTERVALO_LATIDO_DEFECTO = 1.0

ENV_CONTAINER_NAME = "CONTAINER_NAME"
ENV_TARGETS = "HEALTH_CHECKER_TARGETS"
ENV_INTERVAL = "HEARTBEAT_INTERVAL"


def parsear_destinos(destinos_str: str) -> List[Tuple[str, int]]:
    """
    Parsea una lista de destinos con formato 'host1:puerto1,host2:puerto2'.
    Ejemplo: 'health_checker-1:8001,health_checker-2:8001,health_checker-3:8001'
    """
    destinos: List[Tuple[str, int]] = []
    if not destinos_str or not destinos_str.strip():
        return destinos

    for item in destinos_str.split(","):
        limpio = item.strip()
        if not limpio:
            continue
        partes = limpio.split(":")
        if len(partes) != 2 or not partes[0].strip():
            raise ValueError(
                f"Formato de destino de health checker inválido: '{limpio}'. Debe ser 'host:puerto'"
            )
        host = partes[0].strip()
        puerto = int(partes[1].strip())
        if not 1 <= puerto <= 65535:
            raise ValueError(f"Puerto fuera de rango en destino '{limpio}': {puerto}")
        destinos.append((host, puerto))
    return destinos


class EmisorLatidos:
    """
    Hilo secundario que envía periódicamente datagramas UDP de latido (Push)
    con el identificador del nodo a todos los réplicas del Health Checker.
    """

    def __init__(
        self,
        nombre_nodo: str,
        destinos: List[Tuple[str, int]],
        intervalo: float = INTERVALO_LATIDO_DEFECTO,
    ):
        if not nombre_nodo or not nombre_nodo.strip():
            raise ValueError("El nombre del nodo emisor de latidos no puede estar vacío")
        if intervalo <= 0:
            raise ValueError("El intervalo de latidos debe ser mayor que cero")

        self.nombre_nodo = nombre_nodo.strip()
        self.destinos = list(destinos)
        self.intervalo = intervalo
        self._mensaje_bytes = f"{PREFIJO_LATIDO}:{self.nombre_nodo}".encode("utf-8")

        self._lock = threading.Lock()
        self._evento_detener = threading.Event()
        self._activo = False
        self._socket: Optional[socket.socket] = None
        self._hilo: Optional[threading.Thread] = None

    def start(self) -> None:
        """Abre el socket UDP e inicia el hilo emisor de latidos."""
        if not self.destinos:
            return

        with self._lock:
            if self._activo:
                return
            self._activo = True
            self._evento_detener.clear()
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._hilo = threading.Thread(
            target=self._bucle_emision,
            name=f"heartbeat-tx-{self.nombre_nodo}",
            daemon=True,
        )
        self._hilo.start()
        logger.info(
            "Emisor de latidos iniciado para nodo '%s' -> destinos: %s (intervalo: %.1fs)",
            self.nombre_nodo,
            self.destinos,
            self.intervalo,
        )

    def stop(self) -> None:
        """Detiene el hilo emisor de forma limpia y libera el descriptor del socket."""
        with self._lock:
            if not self._activo:
                return
            self._activo = False
            self._evento_detener.set()
            sock = self._socket
            self._socket = None

        if self._hilo and self._hilo.is_alive():
            self._hilo.join(timeout=2.0)

        if sock is not None:
            try:
                sock.close()
            except OSError as error:
                logger.debug("Error al cerrar socket de latidos en '%s': %s", self.nombre_nodo, error)

        logger.info("Emisor de latidos detenido para nodo '%s'", self.nombre_nodo)

    def _bucle_emision(self) -> None:
        """Envía latidos periódicamente usando Event.wait"""
        while not self._evento_detener.is_set():
            self._enviar_ronda()
            self._evento_detener.wait(timeout=self.intervalo)

    def _enviar_ronda(self) -> None:
        with self._lock:
            sock = self._socket
            activo = self._activo

        if not activo or sock is None:
            return

        esperados = len(self._mensaje_bytes)
        for host, puerto in self.destinos:
            if self._evento_detener.is_set():
                break
            try:
                enviados = sock.sendto(self._mensaje_bytes, (host, puerto))
                if enviados != esperados:
                    logger.warning(
                        "Latido enviado parcialmente a %s:%d (%d/%d bytes)",
                        host,
                        puerto,
                        enviados,
                        esperados,
                    )
            except OSError as error:
                logger.debug(
                    "No se pudo enviar latido desde '%s' a %s:%d: %s",
                    self.nombre_nodo,
                    host,
                    puerto,
                    error,
                )


@contextmanager
def iniciar_latidos_desde_entorno(
    nombre_defecto: str = "", apagado=None
) -> Iterator[Optional[EmisorLatidos]]:
    """
    Context manager que inicializa y detiene automáticamente el emisor de latidos
    a partir de las variables de entorno del contenedor.
    Si HEALTH_CHECKER_TARGETS no está definido, opera como no-op para no afectar pruebas unitarias.
    """
    destinos_env = os.environ.get(ENV_TARGETS, "").strip()
    if not destinos_env:
        yield None
        return

    nombre_nodo = (
        os.environ.get(ENV_CONTAINER_NAME, "").strip()
        or nombre_defecto.strip()
        or socket.gethostname()
    )
    intervalo = float(os.environ.get(ENV_INTERVAL, str(INTERVALO_LATIDO_DEFECTO)))
    destinos = parsear_destinos(destinos_env)

    emisor = EmisorLatidos(nombre_nodo=nombre_nodo, destinos=destinos, intervalo=intervalo)
    if apagado is not None:
        apagado.registrar_limpieza(emisor.stop)

    emisor.start()
    try:
        yield emisor
    finally:
        emisor.stop()
