"""Ayudas para probar workers con WAL sin RabbitMQ: middleware falso y 'reinicios' del proceso."""

import json
import os
import tempfile
import unittest
from pathlib import Path

from types import SimpleNamespace

from src.common.io_adapters.wal import WAL
from src.common.protocol.dto import Datos, Entrada, EntregaActual, IdentidadProceso
from src.common.protocol.registros import Lote
from src.workers.base_worker import id_de_salidas

CONFIG_BASE = Path(__file__).parents[1] / 'sigterm' / 'config.json'
ORIGEN = IdentidadProceso('productor', 0)


def id_salida(id_entrada, indice=0):
    """id_entrada con el que salen las salidas del lote recibido (id_entrada, indice)."""
    return id_de_salidas(SimpleNamespace(id_entrada=id_entrada, indice=indice))


class Presupuesto:
    def __init__(self, maximo):
        self.maximo, self.usados = maximo, 0

    def admitir(self, registro):
        if self.maximo is not None and self.usados >= self.maximo:
            return False
        self.usados += 1
        return True


class MiddlewareFalso:
    def __init__(self):
        self.entrega_actual = None
        self.publicados = []   # EnvioDatos publicados
        self.finalizadas = []  # (grupo, entrada)
        self.pendientes = {'otra'}  # entradas sin cierre: no vacío evita cerrar la ejecución completa
        self.fallar_al_finalizar = False
        self.registros_por_mensaje = None

    def crear_presupuesto_lote(self, entrada, esquema):
        return Presupuesto(self.registros_por_mensaje)

    def entradas_pendientes(self, id_ejecucion):
        return frozenset(self.pendientes)

    def publicar(self, envio):
        self.publicados.append(envio)

    def finalizar_entrada(self, grupo, entrada):
        if self.fallar_al_finalizar:
            raise RuntimeError('caída simulada')
        self.finalizadas.append((grupo, entrada))


class PruebaConWal(unittest.TestCase):
    """Base de las pruebas de workers con WAL: cada nuevo_worker() equivale a reiniciar el proceso."""

    clase_worker = None
    grupo = None

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.wal = os.path.join(self._dir.name, 'wal')
        config = json.loads(CONFIG_BASE.read_text())
        config['proceso'] = {'grupo': self.grupo, 'instancia': 0}
        self.config = os.path.join(self._dir.name, 'config.json')
        Path(self.config).write_text(json.dumps(config))
        self.workers = []

    def tearDown(self):
        for worker in self.workers:
            worker._wals.cerrar()

    def nuevo_worker(self):
        worker = self.clase_worker(self.config, directorio_wal=self.wal)
        worker.middleware = worker._middleware = MiddlewareFalso()
        self.workers.append(worker)
        return worker

    def caer(self, worker):
        """Simula la caída del proceso: se pierde la memoria, queda el disco."""
        worker._wals.cerrar()

    def perder_ultimo_registro(self, id_ejecucion):
        """Simula una caída justo antes de persistir el último registro (el worker debe estar caído)."""
        ruta = os.path.join(self.wal, id_ejecucion.encode('utf-8').hex() + '.log')
        with WAL(ruta) as wal:
            registros = wal.leer()
        os.remove(ruta)
        with WAL(ruta) as wal:
            for registro in registros[:-1]:
                wal.agregar(registro)

    def entregar(self, worker, entrada, lote, id_entrada, indice=0, origen=ORIGEN):
        """El middleware entrega un lote al worker."""
        worker.middleware.entrega_actual = EntregaActual(entrada, origen, id_entrada, indice)
        worker._procesar(Datos(entrada, lote))
        worker.middleware.entrega_actual = None

    def cerrar(self, worker, entrada):
        """El middleware entrega el cierre de una entrada (no hay lote en curso)."""
        worker.middleware.entrega_actual = None
        worker._finalizar(entrada)

    def ids(self, worker):
        return [(e.id_entrada, e.indice) for e in worker.middleware.publicados]

    def archivos_wal(self):
        return sorted(os.listdir(self.wal))
