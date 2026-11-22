"""Implementación del algoritmo de elección de líder Bully (El Matón) vía UDP."""

from enum import Enum
import logging
import socket
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

#Posibles estados del nodo
class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class BullyElection:
    """
    Coordinador de elección de líder mediante el algoritmo Bully sobre sockets UDP.

    Garantiza que el nodo vivo con mayor identificador numérico asuma el rol de Líder
    y emita hearthbeat periódicos hacia los demás nodos. Si el Líder cae,
    los seguidores detectan la caida y eligen un nuevo líder.
    """
    #Tipos de mensaje
    MSG_ELECTION = "ELECTION"
    MSG_OK = "OK"
    MSG_COORDINATOR = "COORDINATOR"
    MSG_HEARTBEAT = "HEARTBEAT"

    def __init__(
        self,
        node_id: int,
        listen_port: int,
        peers: List[Tuple[int, str, int]],
        heartbeat_interval: float = 1.0,
        leader_timeout: float = 3.0,
        election_timeout: float = 1.5,
        coordinator_timeout: float = 3.0,
        on_leader_change: Optional[Callable[[Optional[int], bool], None]] = None,
    ):
        """
        Iniciamos el BullyElection, con los parametros recibidos
        node_id: ID único entero de este nodo (mayor ID = mayor prioridad).
        listen_port: Puerto UDP donde escucha mensajes de consenso este nodo.
        peers: Lista de tuplas (peer_id, host, port) de los demás nodos del grupo.
        heartbeat_interval: Frecuencia en segundos con la que el líder emite latidos.
        leader_timeout: Tiempo sin latidos del líder antes de convocar elecciones.
        election_timeout: Tiempo de espera para recibir 'OK' tras enviar 'ELECTION'.
        coordinator_timeout: Tiempo de espera para 'COORDINATOR' tras recibir 'OK'.
        on_leader_change: Callback (leader_id, is_me) invocado al cambiar el líder.
        """
        self.node_id = node_id
        self.listen_port = listen_port
        self.peers: Dict[int, Tuple[str, int]] = {
            pid: (host, port) for pid, host, port in peers if pid != node_id
        }

        self.heartbeat_interval = heartbeat_interval
        self.leader_timeout = leader_timeout
        self.election_timeout = election_timeout
        self.coordinator_timeout = coordinator_timeout
        self.on_leader_change = on_leader_change

        self._lock = threading.Lock() #lock para concurrencia
        self._state = NodeState.FOLLOWER # todos los nodos comienzan como follower
        self._leader_id: Optional[int] = None #En un inicio no hay lider
        self._last_leader_heartbeat = time.time() #tiempo desde el ultimo check
        self._ok_received = threading.Event() #evento para manejar el ok
        self._coordinator_received = threading.Event() #evento para acepta al coordinador

        self._running = False #en un inciio no hay eleccion
        self._socket: Optional[socket.socket] = None #conexion udp

        self._receiver_thread: Optional[threading.Thread] = None 
        self._watchdog_thread: Optional[threading.Thread] = None
        self._heartbeat_thread:Optional[threading.Thread] = None

    @property
    def state(self) -> NodeState:
        with self._lock:
            return self._state

    @property
    def leader_id(self) -> Optional[int]:
        with self._lock:
            return self._leader_id

    def is_leader(self) -> bool:
        with self._lock:
            return self._state == NodeState.LEADER

    def start(self) -> None:
        """Inicia el socket UDP y los hilos de escucha, vigilancia y latidos."""
        with self._lock:
            if self._running:
                return
            self._running = True

            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #declaro socket udp con ip4
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #para reusar la dir enc aso de caida
            self._socket.bind(("0.0.0.0", self.listen_port)) # escucho todas las interfaces de red y escucho por el puerto indicado
            self._socket.settimeout(0.5) # tiempo para cerrar el socket si no recivo nada

            self._last_leader_heartbeat = time.time()

        #Thread para recivir los datos
        self._receiver_thread = threading.Thread(
            target=self._receive_loop, name=f"bully-rx-{self.node_id}", daemon=True
        )
        #Thread para percatarme de que el lider siga vivo
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, name=f"bully-watchdog-{self.node_id}", daemon=True
        )
        #thread para comunicar que estoy vivo a los demas (lider)
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name=f"bully-tx-hb-{self.node_id}", daemon=True
        )

        self._receiver_thread.start()
        self._watchdog_thread.start()
        self._heartbeat_thread.start()

        logger.info(
            "BullyNode %d iniciado en puerto %d. Peers: %s",
            self.node_id,
            self.listen_port,
            list(self.peers.keys()),
        )

        # Arranco una eleccion inicial, por al lanza, no hay lider establecido
        threading.Thread(
            target=self.start_election, name=f"bully-init-elect-{self.node_id}", daemon=True
        ).start()

    def stop(self) -> None:
        """Detiene de forma limpia todos los hilos y cierra el socket."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass

        if self._receiver_thread and self._receiver_thread.is_alive():
            self._receiver_thread.join(timeout=1.0)
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=1.0)
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)

        logger.info("BullyNode %d detenido correctamente", self.node_id)


    def _send_msg(self, target_id: int, msg: str) -> None:
        if target_id not in self.peers or not self._socket:
            return
        host, port = self.peers[target_id]
        try:
            self._socket.sendto(msg.encode("utf-8"), (host, port))
        except OSError as e:
            logger.debug("BullyNode %d: error enviando a nodo %d (%s:%d): %s", self.node_id, target_id, host, port, e)

    def _broadcast(self, msg: str, only_higher: bool = False, only_lower: bool = False) -> None:
        for pid in list(self.peers.keys()):
            if only_higher and pid <= self.node_id:
                continue
            if only_lower and pid >= self.node_id:
                continue
            self._send_msg(pid, msg)

    def start_election(self) -> None:
        """Dispara el proceso de elección Bully."""
        with self._lock:
            if not self._running:
                return
            self._state = NodeState.CANDIDATE
            #seteamos la flags a false
            self._ok_received.clear()
            self._coordinator_received.clear()
            logger.info("BullyNode %d: iniciando ELECCIÓN", self.node_id)

        higher_peers = [pid for pid in self.peers if pid > self.node_id]

        if not higher_peers:
            # Nadie tiene mayor ID que yo: soy el líder inmediatamente
            self._declare_leader()
            return

        # Enviar ELECTION a todos los nodos con mayor ID
        msg = f"{self.MSG_ELECTION}:{self.node_id}"
        self._broadcast(msg, only_higher=True)

        # Esperar respuesta 'OK' de algún nodo de mayor ID
        received_ok = self._ok_received.wait(timeout=self.election_timeout)

        with self._lock:
            if not self._running or self._state != NodeState.CANDIDATE:
                return

        if not received_ok:
            # Ningún nodo mayor respondió: soy el líder
            logger.info("BullyNode %d: no recibió OK de nodos superiores. Asumiendo LIDERAZGO", self.node_id)
            self._declare_leader()
        else:
            # Al menos un nodo superior respondió: esperar que anuncie COORDINATOR
            logger.debug("BullyNode %d: recibió OK. Esperando mensaje COORDINATOR...", self.node_id)
            received_coord = self._coordinator_received.wait(timeout=self.coordinator_timeout)

            with self._lock:
                if not self._running or self._state != NodeState.CANDIDATE:
                    return

            if not received_coord:
                # El nodo superior no completó la elección a tiempo (pudo caer): reintentar
                logger.warning("BullyNode %d: timeout esperando COORDINATOR. Reiniciando elección", self.node_id)
                self.start_election()

    def _declare_leader(self) -> None:
        """Declara a este nodo como líder e informa a todos los demás."""
        with self._lock:
            if not self._running:
                return
            #me declaro como lider, y actualizo el id de lider
            self._state = NodeState.LEADER
            self._leader_id = self.node_id
            callback = self.on_leader_change

        logger.info(">>> BullyNode %d es ahora el LÍDER <<<", self.node_id)

        # Informar a todos los peers
        msg = f"{self.MSG_COORDINATOR}:{self.node_id}"
        self._broadcast(msg)

        if callback:
            try:
                callback(self.node_id, True)
            except Exception as e:
                logger.error("Error en callback on_leader_change: %s", e)

    def _receive_loop(self) -> None:
        while self._running:
            try:
                data, _ = self._socket.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break

            texto = data.decode("utf-8", errors="replace").strip()
            partes = texto.split(":")
            if len(partes) != 2:
                continue

            tipo, sender_str = partes
            try:
                sender_id = int(sender_str)
            except ValueError:
                continue

            self._dispatch_msg(tipo, sender_id)

    def _dispatch_msg(self, tipo: str, sender_id: int) -> None:
        if tipo == self.MSG_ELECTION:
            # Un nodo con menor ID convocó elección: responder OK y disputar si corresponde
            if sender_id < self.node_id:
                self._send_msg(sender_id, f"{self.MSG_OK}:{self.node_id}")
                # Si no éramos candidatos ni líder, lanzamos nuestra propia elección
                with self._lock:
                    iniciar = (self._state == NodeState.FOLLOWER)
                if iniciar:
                    threading.Thread(
                        target=self.start_election, name=f"bully-elect-response-{self.node_id}", daemon=True
                    ).start()

        elif tipo == self.MSG_OK:
            if sender_id > self.node_id:
                self._ok_received.set()

        elif tipo == self.MSG_COORDINATOR:
            with self._lock:
                if sender_id > self.node_id or self._leader_id is None or sender_id == self._leader_id or self._state != NodeState.LEADER:
                    self._leader_id = sender_id
                    self._state = NodeState.FOLLOWER
                    self._last_leader_heartbeat = time.time()
                    self._coordinator_received.set()
                    self._ok_received.set()
                    callback = self.on_leader_change
                    logger.info("BullyNode %d: nuevo líder reconocido -> Nodo %d", self.node_id, sender_id)
                else:
                    # Alguien de menor ID pretende ser coordinador cuando yo soy líder: disputar
                    callback = None
                    threading.Thread(
                        target=self.start_election, name=f"bully-dispute-{self.node_id}", daemon=True
                    ).start()

            if callback:
                try:
                    callback(sender_id, False)
                except Exception as e:
                    logger.error("Error en callback on_leader_change: %s", e)

        elif tipo == self.MSG_HEARTBEAT:
            with self._lock:
                if sender_id == self._leader_id:
                    self._last_leader_heartbeat = time.time()
                elif sender_id > self.node_id and self._state == NodeState.LEADER:
                    # Un nodo de mayor ID está enviando heartbeats como líder: ceder liderazgo
                    self._leader_id = sender_id
                    self._state = NodeState.FOLLOWER
                    self._last_leader_heartbeat = time.time()
                    callback = self.on_leader_change
                    logger.info("BullyNode %d cede liderazgo al nodo superior %d", self.node_id, sender_id)
                    if callback:
                        try:
                            callback(sender_id, False)
                        except Exception as e:
                            logger.error("Error en callback on_leader_change: %s", e)


    def _heartbeat_loop(self) -> None:
        """El líder envía periódicamente latidos a todos sus seguidores."""
        while self._running:
            time.sleep(self.heartbeat_interval)
            with self._lock:
                es_lider = (self._state == NodeState.LEADER)

            if es_lider and self._running:
                msg = f"{self.MSG_HEARTBEAT}:{self.node_id}"
                self._broadcast(msg, only_lower=True)

    def _watchdog_loop(self) -> None:
        """Los seguidores vigilan periódicamente que el líder siga emitiendo latidos."""
        while self._running:
            time.sleep(0.5)
            with self._lock:
                if self._state == NodeState.LEADER:
                    continue
                tiempo_sin_latido = time.time() - self._last_leader_heartbeat
                timeout = self.leader_timeout

            if tiempo_sin_latido > timeout:
                logger.warning(
                    "BullyNode %d: TIMEOUT de latidos del líder (%0.1fs sin respuesta). Convocando elección",
                    self.node_id,
                    tiempo_sin_latido,
                )
                with self._lock:
                    self._last_leader_heartbeat = time.time()  # reset temporal para evitar spam
                self.start_election()
