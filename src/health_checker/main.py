"""Punto de entrada del servicio Health Checker con algoritmo de consenso Bully."""

import logging
import os
import signal
import sys
import time
from typing import List, Tuple

from src.health_checker.bully import BullyElection

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
    """Cargamos todos los parametros desde el compose"""
    try:
        node_id = int(os.environ.get("HEALTH_CHECKER_ID", "1")) #Id del nodo
        listen_port = int(os.environ.get("HEALTH_CHECKER_PORT", "8000")) #puerto al que se conecta para checkear
        peers_env = os.environ.get("HEALTH_CHECKER_PEERS", "") #string del tipo: id:ip:port
        peers = parse_peers(peers_env) #obtenemos una lista de tupla para que cada nodo conosca a los demas

        heartbeat_interval = float(os.environ.get("BULLY_HEARTBEAT_INTERVAL", "1.0"))#cada cuanto tiempo queremos checkear
        leader_timeout = float(os.environ.get("BULLY_LEADER_TIMEOUT", "3.0")) #Timeout que le damos al lider para escuchar de el y no asumir que esta caido
        election_timeout = float(os.environ.get("BULLY_ELECTION_TIMEOUT", "1.5")) #

    except ValueError as e:
        logger.error("Error en configuración de variables de entorno: %s", e)
        return 1

    detenido = False

    def on_leader_change(leader_id: int, is_me: bool) -> None:
        if is_me:
            logger.info(">>> SOY EL LÍDER (Nodo %d) - Plano de Control Activo <<<", node_id)
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

    def handle_signal(sig, frame):
        nonlocal detenido
        #logger.info("Señal de apagado recibida (%s). Deteniendo Health Checker %d...", sig, node_id)
        detenido = True
        bully.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    #logger.info("Iniciando Health Checker %d en puerto %d...", node_id, listen_port)
    bully.start()

    try:
        while not detenido:
            time.sleep(1.0)
    except KeyboardInterrupt:
        handle_signal(signal.SIGINT, None)

    logger.info("Health Checker %d finalizado.", node_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
