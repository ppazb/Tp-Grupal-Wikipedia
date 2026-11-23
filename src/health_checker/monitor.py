"""Monitor receptor de latidos (heartbeats) UDP de los nodos del plano de datos."""

import logging
import socket
import threading
import time
from typing import Callable, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

MSG_HEARTBEAT = "HEARTBEAT"
DEFAULT_NODE_TIMEOUT = 4.0
DEFAULT_CHECK_INTERVAL = 0.5
SOCKET_TIMEOUT = 0.5
MAX_DATAGRAM_BYTES = 1024


class HeartbeatMonitor:
    """
    Escucha datagramas UDP de latidos provenientes de los workers y del gateway.
    Mantiene una tabla en memoria con el último timestamp de cada nodo y detecta
    caídas por timeout sin consultar a Docker.
    """

    def __init__(
        self,
        listen_port: int,
        node_timeout: float = DEFAULT_NODE_TIMEOUT,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        on_node_failure: Optional[Callable[[str, float], None]] = None,
        initial_nodes: Optional[Iterable[str]] = None,
    ):
        if not 1 <= listen_port <= 65535:
            raise ValueError(f"Puerto de escucha inválido: {listen_port}")
        if node_timeout <= 0:
            raise ValueError("El timeout de nodo debe ser mayor que cero")
        if check_interval <= 0:
            raise ValueError("El intervalo de chequeo debe ser mayor que cero")

        self.listen_port = listen_port
        self.node_timeout = node_timeout
        self.check_interval = check_interval
        self.on_node_failure = on_node_failure
        self._initial_nodes = tuple(
            n.strip() for n in (initial_nodes or ()) if n and n.strip()
        )

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._running = False

        self._last_seen: Dict[str, float] = {}
        self._last_notified: Dict[str, float] = {}

        self._socket: Optional[socket.socket] = None
        self._receiver_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Abre el socket UDP, pre-carga nodos iniciales y lanza los hilos de monitoreo."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()

            ahora = time.time()
            for node_name in self._initial_nodes:
                self._last_seen.setdefault(node_name, ahora)

            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._socket.bind(("0.0.0.0", self.listen_port))
            self._socket.settimeout(SOCKET_TIMEOUT)

        if self._initial_nodes:
            logger.info(
                "HeartbeatMonitor pre-cargado con %d nodos iniciales: %s",
                len(self._initial_nodes),
                list(self._initial_nodes),
            )

        self._receiver_thread = threading.Thread(
            target=self._receive_loop,
            name=f"hb-monitor-rx-{self.listen_port}",
            daemon=True,
        )
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name=f"hb-monitor-watchdog-{self.listen_port}",
            daemon=True,
        )

        self._receiver_thread.start()
        self._watchdog_thread.start()
        logger.info(
            "HeartbeatMonitor escuchando latidos de nodos en puerto UDP %d (timeout: %.1fs)",
            self.listen_port,
            self.node_timeout,
        )

    def stop(self) -> None:
        """Detiene limpiamente los hilos y cierra el socket UDP."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()
            sock = self._socket
            self._socket = None

        if sock is not None:
            try:
                sock.close()
            except OSError as error:
                logger.debug("Error cerrando socket del HeartbeatMonitor: %s", error)

        if self._receiver_thread and self._receiver_thread.is_alive():
            self._receiver_thread.join(timeout=1.5)
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=1.5)

        logger.info("HeartbeatMonitor detenido correctamente")

    def reset_notifications(self) -> None:
        """
        Reinicia el registro de alertas emitidas para que un nuevo Líder evalúe
        de inmediato cualquier nodo que continúe caído.
        """
        with self._lock:
            self._last_notified.clear()

    def get_last_seen(self) -> Dict[str, float]:
        """Devuelve una copia segura para hilos de los timestamps de última vista."""
        with self._lock:
            return dict(self._last_seen)

    def _receive_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                sock = self._socket
            if sock is None:
                break

            try:
                data, _ = sock.recvfrom(MAX_DATAGRAM_BYTES)
            except socket.timeout:
                continue
            except OSError:
                break

            if not data:
                continue

            texto = data.decode("utf-8", errors="replace").strip()
            partes = texto.split(":", 1)
            if len(partes) != 2:
                continue

            tipo, node_name = partes[0].strip(), partes[1].strip()
            if tipo != MSG_HEARTBEAT or not node_name:
                continue

            ahora = time.time()
            with self._lock:
                es_nuevo = node_name not in self._last_seen
                estaba_caido = node_name in self._last_notified
                self._last_seen[node_name] = ahora
                if estaba_caido:
                    del self._last_notified[node_name]

            if es_nuevo:
                logger.info("Monitor: nuevo nodo registrado '%s'", node_name)
            elif estaba_caido:
                logger.info("Monitor: nodo '%s' recuperado (volvió a emitir latidos)", node_name)

    def _watchdog_loop(self) -> None:
        while not self._stop_event.wait(timeout=self.check_interval):
            ahora = time.time()
            caidos = []

            with self._lock:
                for node_name, last_ts in self._last_seen.items():
                    elapsed = ahora - last_ts
                    if elapsed > self.node_timeout:
                        ultima_alerta = self._last_notified.get(node_name, 0.0)
                        if ahora - ultima_alerta >= self.node_timeout:
                            self._last_notified[node_name] = ahora
                            caidos.append((node_name, elapsed))

            for node_name, elapsed in caidos:
                if self.on_node_failure is not None:
                    try:
                        self.on_node_failure(node_name, elapsed)
                    except Exception as error:
                        logger.error(
                            "Error en callback on_node_failure para '%s': %s",
                            node_name,
                            error,
                        )
