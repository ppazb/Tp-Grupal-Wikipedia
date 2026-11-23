"""Pruebas unitarias y de integración del módulo de recuperación DockerRestarter + Bully + Monitor."""

from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import threading
import unittest

from src.common.runtime.latidos import EmisorLatidos
from src.health_checker.bully import BullyElection
from src.health_checker.main import crear_manejador_falla
from src.health_checker.monitor import HeartbeatMonitor
from src.health_checker.recovery import DockerRestarter


class FakeDockerUnixServer:
    """Servidor AF_UNIX de prueba que simula el endpoint POST /containers/<name>/start de Docker."""

    def __init__(self, socket_path: str, status_line: str = "HTTP/1.1 204 No Content"):
        self.socket_path = socket_path
        self.status_line = status_line
        self.requests = []
        self.request_event = threading.Event()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._sock = None
        self._thread = None

    def start(self):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self.socket_path)
        self._sock.listen(5)
        self._sock.settimeout(0.2)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _serve(self):
        while not self._stop_event.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            with conn:
                data = conn.recv(4096).decode("utf-8", errors="replace")
                primera_linea = data.splitlines()[0] if data else ""
                with self._lock:
                    self.requests.append(primera_linea)
                respuesta = f"{self.status_line}\r\nConnection: close\r\n\r\n".encode("utf-8")
                conn.sendall(respuesta)
                self.request_event.set()


class TestDockerRecovery(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory(prefix="test-docker-sock-")
        self.addCleanup(self._tmp.cleanup)
        self.sock_path = str(Path(self._tmp.name) / "docker.sock")

    def test_restart_container_envia_post_y_procesa_204_304_y_404(self):
        """Verifica que DockerRestarter envíe la petición HTTP sobre socket Unix y valide códigos."""
        server = FakeDockerUnixServer(self.sock_path, status_line="HTTP/1.1 204 No Content")
        server.start()
        self.addCleanup(server.stop)

        restarter = DockerRestarter(socket_path=self.sock_path, timeout=2.0)

        self.assertTrue(restarter.restart_container("q4_frequency-1"))
        self.assertEqual(server.requests[-1], "POST /containers/q4_frequency-1/start HTTP/1.1")

        server.status_line = "HTTP/1.1 304 Not Modified"
        self.assertTrue(restarter.restart_container("date_router-1"))

        server.status_line = "HTTP/1.1 404 Not Found"
        self.assertFalse(restarter.restart_container("inexistente"))

    def test_integracion_consenso_y_recuperacion_solo_el_lider_reinicia(self):
        """
        Escenario completo: 3 health checkers (con Bully y HeartbeatMonitor) vigilan a
        'q4_frequency-1'. Cuando el worker cae, los 3 detectan el timeout, pero ÚNICAMENTE
        el Líder envía la orden de reinicio al socket Unix de Docker.
        """
        server = FakeDockerUnixServer(self.sock_path, status_line="HTTP/1.1 204 No Content")
        server.start()
        self.addCleanup(server.stop)

        restarter = DockerRestarter(socket_path=self.sock_path, timeout=2.0)

        bully_ports = [19301, 19302, 19303]
        hb_ports = [19311, 19312, 19313]
        peers = [(i + 1, "127.0.0.1", bully_ports[i]) for i in range(3)]

        bully_nodes = []
        monitors = []
        quien_reinicio = []

        for idx in range(3):
            node_id = idx + 1
            bully = BullyElection(
                node_id=node_id,
                listen_port=bully_ports[idx],
                peers=peers,
                heartbeat_interval=0.15,
                leader_timeout=0.5,
                election_timeout=0.3,
                coordinator_timeout=0.5,
            )

            def make_on_failure(b_node, nid):
                def _on_failure(node_name: str, elapsed: float):
                    if b_node.is_leader():
                        quien_reinicio.append((nid, node_name))
                        restarter.restart_container(node_name)
                return _on_failure

            monitor = HeartbeatMonitor(
                listen_port=hb_ports[idx],
                node_timeout=0.4,
                check_interval=0.05,
                on_node_failure=make_on_failure(bully, node_id),
            )
            bully_nodes.append(bully)
            monitors.append(monitor)

        def cleanup_cluster():
            for m in monitors:
                m.stop()
            for b in bully_nodes:
                b.stop()

        self.addCleanup(cleanup_cluster)

        for m in monitors:
            m.start()
        for b in bully_nodes:
            b.start()

        # Esperar convergencia de líder (Nodo 3)
        espera = threading.Event()
        for _ in range(40):
            if bully_nodes[2].is_leader() and bully_nodes[0].leader_id == 3 and bully_nodes[1].leader_id == 3:
                break
            espera.wait(timeout=0.05)
        self.assertTrue(bully_nodes[2].is_leader(), "El nodo 3 debe ser el líder")

        # Worker comienza a emitir latidos a los 3 monitores
        emisor = EmisorLatidos(
            nombre_nodo="q4_frequency-1",
            destinos=[("127.0.0.1", p) for p in hb_ports],
            intervalo=0.08,
        )
        self.addCleanup(emisor.stop)
        emisor.start()

        for _ in range(30):
            if all("q4_frequency-1" in m.get_last_seen() for m in monitors):
                break
            espera.wait(timeout=0.05)
        for m in monitors:
            self.assertIn("q4_frequency-1", m.get_last_seen())

        # Simulamos caída del worker q4_frequency-1
        emisor.stop()

        # Esperar que el Líder dispare la petición al socket Unix de Docker
        recibido = server.request_event.wait(timeout=1.5)
        self.assertTrue(recibido, "El líder debió enviar la petición de reinicio a Docker")

        # Verificar que SOLO el nodo 3 (Líder) ejecutó el reinicio y una única vez en la ventana
        self.assertEqual(len(server.requests), 1)
        self.assertEqual(server.requests[0], "POST /containers/q4_frequency-1/start HTTP/1.1")
        self.assertEqual(quien_reinicio, [(3, "q4_frequency-1")])

    def test_recuperacion_de_health_checker_caido_por_nuevo_lider(self):
        """
        Verifica que los health_checkers se vigilen entre sí reutilizando EmisorLatidos
        y HeartbeatMonitor: al caer el Líder (health_checker-3), health_checker-2 asume
        el liderazgo y reinicia el contenedor 'health_checker-3'.
        """
        server = FakeDockerUnixServer(self.sock_path, status_line="HTTP/1.1 204 No Content")
        server.start()
        self.addCleanup(server.stop)

        restarter = DockerRestarter(socket_path=self.sock_path, timeout=2.0)

        bully_ports = [19401, 19402, 19403]
        hb_ports = [19411, 19412, 19413]
        peers = [(i + 1, "127.0.0.1", bully_ports[i]) for i in range(3)]
        all_hc_names = ["health_checker-1", "health_checker-2", "health_checker-3"]

        bully_nodes = []
        monitors = []
        emisores = []
        quien_reinicio = []

        for idx in range(3):
            node_id = idx + 1
            own_name = f"health_checker-{node_id}"
            initial_nodes = [n for n in all_hc_names if n != own_name]

            def make_on_leader_change(mon_idx):
                def _on_leader(leader_id, is_me):
                    if is_me and mon_idx < len(monitors):
                        monitors[mon_idx].reset_notifications()
                return _on_leader

            bully = BullyElection(
                node_id=node_id,
                listen_port=bully_ports[idx],
                peers=peers,
                heartbeat_interval=0.1,
                leader_timeout=0.35,
                election_timeout=0.2,
                coordinator_timeout=0.35,
                on_leader_change=make_on_leader_change(idx),
            )

            def make_on_failure(b_node, nid, self_name):
                def _on_failure(node_name: str, elapsed: float):
                    if node_name != self_name and b_node.is_leader():
                        quien_reinicio.append((nid, node_name))
                        restarter.restart_container(node_name)
                return _on_failure

            monitor = HeartbeatMonitor(
                listen_port=hb_ports[idx],
                node_timeout=0.5,
                check_interval=0.05,
                on_node_failure=make_on_failure(bully, node_id, own_name),
                initial_nodes=initial_nodes,
            )
            emisor = EmisorLatidos(
                nombre_nodo=own_name,
                destinos=[("127.0.0.1", p) for p in hb_ports],
                intervalo=0.08,
            )
            bully_nodes.append(bully)
            monitors.append(monitor)
            emisores.append(emisor)

        def cleanup_cluster():
            for e in emisores:
                e.stop()
            for m in monitors:
                m.stop()
            for b in bully_nodes:
                b.stop()

        self.addCleanup(cleanup_cluster)

        for m in monitors:
            m.start()
        for b in bully_nodes:
            b.start()
        for e in emisores:
            e.start()

        espera = threading.Event()
        for _ in range(40):
            if bully_nodes[2].is_leader():
                break
            espera.wait(timeout=0.05)
        self.assertTrue(bully_nodes[2].is_leader())

        # Cae completamente health_checker-3 (detenemos su Bully, su Monitor y su EmisorLatidos)
        emisores[2].stop()
        bully_nodes[2].stop()
        monitors[2].stop()

        # Esperar a que health_checker-2 asuma como líder y reinicie a health_checker-3
        recibido = server.request_event.wait(timeout=2.5)
        self.assertTrue(recibido, "El nuevo líder (health_checker-2) debió reiniciar a health_checker-3")
        self.assertIn("POST /containers/health_checker-3/start HTTP/1.1", server.requests)
        self.assertEqual(quien_reinicio[0], (2, "health_checker-3"))

    def test_reinicios_en_paralelo_y_sin_duplicar_reinicio_en_curso(self):
        """
        Verifica que dos caídas simultáneas ejecuten restart_container en paralelo
        en hilos aparte, y que una caída repetida del mismo nodo con reinicio en
        curso no dispare un segundo reinicio hasta que termine el primero.
        """
        class FakeBully:
            leader_id = 3

            def is_leader(self) -> bool:
                return True

        class FakeRestarter:
            def __init__(self):
                self.lock = threading.Lock()
                self.llamadas = []
                self.liberar = threading.Event()
                self.iniciados = {}
                self.cantidad_por_nodo = {}

            def registrar_espera(self, nombre: str) -> threading.Event:
                evento = threading.Event()
                with self.lock:
                    self.iniciados[nombre] = evento
                return evento

            def restart_container(self, container_name: str) -> bool:
                with self.lock:
                    self.llamadas.append(container_name)
                    self.cantidad_por_nodo[container_name] = (
                        self.cantidad_por_nodo.get(container_name, 0) + 1
                    )
                    evento = self.iniciados.get(container_name)
                if evento is not None:
                    evento.set()
                self.liberar.wait(timeout=5.0)
                return True

        restarter = FakeRestarter()
        inicio_a = restarter.registrar_espera("date_router-1")
        inicio_b = restarter.registrar_espera("q4_frequency-1")
        on_failure = crear_manejador_falla(
            node_id=3,
            own_name="health_checker-3",
            bully=FakeBully(),
            restarter=restarter,
        )

        # 1. Cae date_router-1: entra en restart_container y queda bloqueado en liberar
        on_failure("date_router-1", 4.1)
        self.assertTrue(inicio_a.wait(timeout=2.0), "Debió iniciar el reinicio de date_router-1")

        # 2. Caída repetida del mismo nodo mientras el reinicio sigue en curso: no duplica
        on_failure("date_router-1", 4.6)

        # 3. Cae un segundo nodo en paralelo mientras date_router-1 sigue bloqueado
        on_failure("q4_frequency-1", 4.2)
        self.assertTrue(
            inicio_b.wait(timeout=2.0),
            "El segundo nodo debió iniciar su reinicio en paralelo sin esperar al primero",
        )

        with restarter.lock:
            self.assertEqual(restarter.cantidad_por_nodo.get("date_router-1"), 1)
            self.assertEqual(restarter.cantidad_por_nodo.get("q4_frequency-1"), 1)

        # Liberamos ambos reinicios y esperamos a que terminen sus hilos
        restarter.liberar.set()
        for hilo in threading.enumerate():
            if hilo.name in ("recuperacion-date_router-1", "recuperacion-q4_frequency-1"):
                hilo.join(timeout=2.0)

        # 4. Una vez finalizado el intento, otra caída del mismo nodo sí vuelve a lanzarse
        segundo_inicio_a = restarter.registrar_espera("date_router-1")
        on_failure("date_router-1", 4.1)
        self.assertTrue(
            segundo_inicio_a.wait(timeout=2.0),
            "Al finalizar el intento anterior, el nodo debe poder reiniciarse nuevamente",
        )
        for hilo in threading.enumerate():
            if hilo.name == "recuperacion-date_router-1":
                hilo.join(timeout=2.0)

        with restarter.lock:
            self.assertEqual(restarter.cantidad_por_nodo.get("date_router-1"), 2)


if __name__ == "__main__":
    unittest.main()
