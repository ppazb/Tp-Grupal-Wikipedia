"""Pruebas unitarias y de integración local para el algoritmo Bully de elección de líder."""

import logging
import time
import unittest

from src.health_checker.bully import BullyElection, NodeState

logging.basicConfig(level=logging.INFO)


class TestBullyElection(unittest.TestCase):
    def setUp(self):
        # Puertos locales en rango libre para pruebas
        self.port1 = 19101
        self.port2 = 19102
        self.port3 = 19103

        self.peers = [
            (1, "127.0.0.1", self.port1),
            (2, "127.0.0.1", self.port2),
            (3, "127.0.0.1", self.port3),
        ]

        self.nodes = []

    def tearDown(self):
        for node in self.nodes:
            node.stop()

    def test_inicializacion_y_eleccion_del_nodo_mayor(self):
        """Con 3 nodos activos, el nodo con mayor ID (3) debe ser elegido líder."""
        node1 = BullyElection(
            node_id=1,
            listen_port=self.port1,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        node2 = BullyElection(
            node_id=2,
            listen_port=self.port2,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        node3 = BullyElection(
            node_id=3,
            listen_port=self.port3,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        self.nodes = [node1, node2, node3]

        for node in self.nodes:
            node.start()

        # Esperar convergencia de consenso (máx 2.5s)
        time.sleep(2.0)

        self.assertTrue(node3.is_leader(), "El nodo 3 debe ser el líder")
        self.assertEqual(node1.leader_id, 3, "El nodo 1 debe reconocer al nodo 3 como líder")
        self.assertEqual(node2.leader_id, 3, "El nodo 2 debe reconocer al nodo 3 como líder")
        self.assertEqual(node1.state, NodeState.FOLLOWER)
        self.assertEqual(node2.state, NodeState.FOLLOWER)
        self.assertEqual(node3.state, NodeState.LEADER)

    def test_caida_del_lider_y_reeleccion(self):
        """Al caer el líder (nodo 3), el nodo con mayor ID restante (nodo 2) asume el liderazgo."""
        node1 = BullyElection(
            node_id=1,
            listen_port=self.port1,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        node2 = BullyElection(
            node_id=2,
            listen_port=self.port2,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        node3 = BullyElection(
            node_id=3,
            listen_port=self.port3,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        self.nodes = [node1, node2, node3]

        for node in self.nodes:
            node.start()

        time.sleep(2.0)
        self.assertEqual(node2.leader_id, 3)

        # Simulamos la caída abrupta del líder (nodo 3)
        node3.stop()

        # Esperar a que el watchdog detecte la ausencia de latidos y elija a nodo 2 (tiempo timeout + eleccion)
        time.sleep(2.5)

        self.assertTrue(node2.is_leader(), "El nodo 2 debe asumir como nuevo líder tras la caída del nodo 3")
        self.assertEqual(node1.leader_id, 2, "El nodo 1 debe reconocer al nodo 2 como el nuevo líder")

    def test_recuperacion_del_lider_original(self):
        """Si el nodo 3 revive, debe desplazar al nodo 2 y reasumir el liderazgo (Bully)."""
        node1 = BullyElection(
            node_id=1,
            listen_port=self.port1,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        node2 = BullyElection(
            node_id=2,
            listen_port=self.port2,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        self.nodes = [node1, node2]

        for node in self.nodes:
            node.start()

        time.sleep(1.5)
        self.assertTrue(node2.is_leader(), "Con solo 1 y 2, el nodo 2 debe ser líder")

        # Ahora revive el nodo 3
        node3 = BullyElection(
            node_id=3,
            listen_port=self.port3,
            peers=self.peers,
            heartbeat_interval=0.2,
            leader_timeout=0.6,
            election_timeout=0.4,
            coordinator_timeout=0.6,
        )
        self.nodes.append(node3)
        node3.start()

        time.sleep(2.0)

        self.assertTrue(node3.is_leader(), "El nodo 3 revivido debe ser el nuevo líder")
        self.assertEqual(node2.leader_id, 3, "El nodo 2 debe ceder el liderazgo al nodo 3")
        self.assertEqual(node1.leader_id, 3, "El nodo 1 debe actualizar su líder al nodo 3")


if __name__ == "__main__":
    unittest.main()
