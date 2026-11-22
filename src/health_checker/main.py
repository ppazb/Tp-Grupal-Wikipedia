"""Punto de entrada del servicio Health Checker con algoritmo de consenso Bully y monitoreo UDP."""

import logging
import os
import signal
import sys
import threading
from typing import List, Optional, Tuple

from src.health_checker.bully import BullyElection
from src.health_checker.monitor import HeartbeatMonitor
from src.health_checker.recovery import DOCKER_SOCKET_PATH, DockerRestarter

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] (health_checker_%(process)d) %(name)s: %(message)s",
)
logger = logging.getLogger("health_checker")


def parse_peers(peers_str: str) -> List[Tuple[int, str, int]]:
    """
    Parsea una cadena con formato: 'id:host:puerto,id:host:puerto'
    Ejemplo: '1:health_checker-1:8000,2:health_checker-2:8000,3:health_checker-3:8000'
    """
    peers = []
    if not peers_str.strip():
        return peers
    for item in peers_str.split(","):
        item = item.strip()
        if not item:
            continue
        partes = item.split(":")
        if len(partes) != 3:
            raise ValueError(f"Formato de peer inválido: '{item}'. Debe ser 'id:host:puerto'")
        peer_id = int(partes[0])
        host = partes[1]
        port = int(partes[2])
        peers.append((peer_id, host, port))
    return peers


def main() -> int:
    """Carga los parámetros desde variables de entorno e inicia consenso y monitoreo."""
    try:
        node_id = int(os.environ.get("HEALTH_CHECKER_ID", "1"))
        listen_port = int(os.environ.get("HEALTH_CHECKER_PORT", "8000"))
        heartbeat_port = int(os.environ.get("HEALTH_CHECKER_HEARTBEAT_PORT", "8001"))
        peers_env = os.environ.get("HEALTH_CHECKER_PEERS", "")
        peers = parse_peers(peers_env)

        heartbeat_interval = float(os.environ.get("BULLY_HEARTBEAT_INTERVAL", "1.0"))
        leader_timeout = float(os.environ.get("BULLY_LEADER_TIMEOUT", "3.0"))
        election_timeout = float(os.environ.get("BULLY_ELECTION_TIMEOUT", "1.5"))

        worker_timeout = float(os.environ.get("WORKER_HEARTBEAT_TIMEOUT", "4.0"))
        worker_check_interval = float(os.environ.get("WORKER_CHECK_INTERVAL", "0.5"))
        docker_socket_path = os.environ.get("DOCKER_SOCKET_PATH", DOCKER_SOCKET_PATH)

    except ValueError as e:
        logger.error("Error en configuración de variables de entorno: %s", e)
        return 1

    stop_event = threading.Event()
    monitor_ref: Optional[HeartbeatMonitor] = None
    restarter = DockerRestarter(socket_path=docker_socket_path)

    def on_leader_change(leader_id: Optional[int], is_me: bool) -> None:
        if is_me:
            logger.info(">>> SOY EL LÍDER (Nodo %d) - Plano de Control Activo <<<", node_id)
            if monitor_ref is not None:
                monitor_ref.reset_notifications()
        else:
            logger.info("--- Nodo %d en modo SEGUIDOR. Líder actual: %s ---", node_id, leader_id)

    bully = BullyElection(
        node_id=node_id,
        listen_port=listen_port,
        peers=peers,
        heartbeat_interval=heartbeat_interval,
        leader_timeout=leader_timeout,
        election_timeout=election_timeout,
        on_leader_change=on_leader_change,
    )

    def on_node_failure(node_name: str, elapsed: float) -> None:
        if bully.is_leader():
            logger.error(
                "[LÍDER %d] Caída detectada por timeout del nodo '%s' (%.1fs sin latidos). "
                "Ejecutando recuperación...",
                node_id,
                node_name,
                elapsed,
            )
            restarter.restart_container(node_name)
        else:
            logger.warning(
                "[SEGUIDOR %d] Caída detectada del nodo '%s' (%.1fs sin latidos). "
                "El Líder (%s) gestionará su recuperación.",
                node_id,
                node_name,
                elapsed,
                bully.leader_id,
            )

    monitor = HeartbeatMonitor(
        listen_port=heartbeat_port,
        node_timeout=worker_timeout,
        check_interval=worker_check_interval,
        on_node_failure=on_node_failure,
    )
    monitor_ref = monitor

    def handle_signal(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    monitor.start()
    bully.start()

    try:
        stop_event.wait()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        bully.stop()
        monitor.stop()

    logger.info("Health Checker %d finalizado.", node_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
