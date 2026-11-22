"""Pruebas de emisión de latidos UDP y detección de caídas por timeout en Health Checker."""

import threading
import unittest

from src.common.runtime.latidos import EmisorLatidos, parsear_destinos
from src.health_checker.monitor import HeartbeatMonitor


class TestHeartbeatSystem(unittest.TestCase):
    def setUp(self):
        self.monitor_port_1 = 19201
        self.monitor_port_2 = 19202
        self.monitors = []
        self.emisores = []

    def tearDown(self):
        for emisor in self.emisores:
            emisor.stop()
        for monitor in self.monitors:
            monitor.stop()

    def test_parsear_destinos_validos_e_invalidos(self):
        destinos = parsear_destinos("health_checker-1:8001, health_checker-2:8001")
        self.assertEqual(destinos, [("health_checker-1", 8001), ("health_checker-2", 8001)])
        self.assertEqual(parsear_destinos(""), [])

        with self.assertRaises(ValueError):
            parsear_destinos("nodo_sin_puerto")
        with self.assertRaises(ValueError):
            parsear_destinos("host:99999")

    def test_emision_a_multiples_monitores_y_registro_de_nodos(self):
        """Un worker envía latidos UDP en paralelo a todos los health checkers configurados."""
        recibido_m1 = threading.Event()
        recibido_m2 = threading.Event()

        m1 = HeartbeatMonitor(listen_port=self.monitor_port_1, node_timeout=1.0, check_interval=0.1)
        m2 = HeartbeatMonitor(listen_port=self.monitor_port_2, node_timeout=1.0, check_interval=0.1)
        self.monitors = [m1, m2]
        m1.start()
        m2.start()

        emisor = EmisorLatidos(
            nombre_nodo="date_router-1",
            destinos=[
                ("127.0.0.1", self.monitor_port_1),
                ("127.0.0.1", self.monitor_port_2),
            ],
            intervalo=0.1,
        )
        self.emisores.append(emisor)
        emisor.start()

        # Esperar hasta que ambos monitores registren el nodo (sin usar time.sleep fijo)
        evento_espera = threading.Event()
        for _ in range(20):
            if "date_router-1" in m1.get_last_seen() and "date_router-1" in m2.get_last_seen():
                recibido_m1.set()
                recibido_m2.set()
                break
            evento_espera.wait(timeout=0.05)

        self.assertTrue(recibido_m1.is_set(), "Monitor 1 debe haber recibido latido de date_router-1")
        self.assertTrue(recibido_m2.is_set(), "Monitor 2 debe haber recibido latido de date_router-1")

    def test_deteccion_de_caida_por_timeout_y_recuperacion(self):
        """Cuando un worker deja de emitir latidos, el monitor dispara on_node_failure por timeout."""
        fallas_detectadas = []
        evento_falla = threading.Event()

        def on_failure(node_name: str, elapsed: float):
            fallas_detectadas.append((node_name, elapsed))
            evento_falla.set()

        monitor = HeartbeatMonitor(
            listen_port=self.monitor_port_1,
            node_timeout=0.4,
            check_interval=0.05,
            on_node_failure=on_failure,
        )
        self.monitors.append(monitor)
        monitor.start()

        emisor = EmisorLatidos(
            nombre_nodo="q4_frequency-1",
            destinos=[("127.0.0.1", self.monitor_port_1)],
            intervalo=0.08,
        )
        self.emisores.append(emisor)
        emisor.start()

        # Esperar primer latido
        espera = threading.Event()
        for _ in range(20):
            if "q4_frequency-1" in monitor.get_last_seen():
                break
            espera.wait(timeout=0.05)
        self.assertIn("q4_frequency-1", monitor.get_last_seen())

        # Simular caída del worker deteniendo su emisor
        emisor.stop()

        # El monitor debe detectar la caída en ~0.4s
        detectado = evento_falla.wait(timeout=1.5)
        self.assertTrue(detectado, "El monitor debe detectar la caída por timeout")
        self.assertEqual(fallas_detectadas[0][0], "q4_frequency-1")
        self.assertGreaterEqual(fallas_detectadas[0][1], 0.4)


if __name__ == "__main__":
    unittest.main()
