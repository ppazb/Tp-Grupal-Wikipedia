"""Entorno aislado de RabbitMQ y procesos auxiliares para escenarios de integración."""

import base64
from contextlib import ExitStack
from dataclasses import replace
import multiprocessing
from queue import Empty
import signal
import unittest
from urllib.request import Request, urlopen
from uuid import uuid4

from src.common.protocol.dto import IdentidadProceso
from src.middleware.broker_connection import ConexionBroker
from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.inicializar import inicializar_capacidad
from src.middleware.rabbitmq import MiddlewareRabbitMQ
from src.middleware.topologia import TopologiaGrupos

PLAZO_ESCENARIO = 60
PLAZO_PROCESO = 10
PLAZO_BLOQUEO = 0.2


def vencer_plazo(numero, marco):
    raise AssertionError('El escenario excedió su plazo; posible bloqueo de comunicación')


def administrar_virtual_host(configuracion, metodo):
    """Crea o elimina únicamente el virtual host del escenario en el broker de pruebas."""
    broker = configuracion.broker
    credencial = base64.b64encode(f'{broker.usuario}:{broker.contrasena}'.encode()).decode()
    solicitud = Request(
        f'http://{broker.host}:15672/api/vhosts/{broker.virtual_host}',
        method=metodo, headers={'Authorization': f'Basic {credencial}'},
    )
    with urlopen(solicitud, timeout=PLAZO_PROCESO) as respuesta:
        if respuesta.status not in (201, 204):
            raise AssertionError(f'Administración del virtual host: HTTP {respuesta.status}')


class EscenarioRabbitMQ(unittest.TestCase):
    """Aísla cada caso y libera conexiones, procesos y colas incluso ante errores."""

    def setUp(self):
        anterior = signal.signal(signal.SIGALRM, vencer_plazo)
        self.addCleanup(signal.signal, signal.SIGALRM, anterior)
        self.addCleanup(signal.alarm, 0)
        signal.alarm(PLAZO_ESCENARIO)
        self.virtual_host = f'escenario_{uuid4().hex}'
        self.configuracion = self.configurar()
        administrar_virtual_host(self.configuracion, 'PUT')
        self.addCleanup(administrar_virtual_host, self.configuracion, 'DELETE')
        self.recursos = ExitStack()
        self.addCleanup(self.recursos.close)
        inicializar_capacidad(self.configuracion)
        self.inspector = self.recursos.enter_context(ConexionBroker(self.configuracion.broker))
        self.topologia = TopologiaGrupos(self.configuracion)

    def configurar(self, grupo=None, instancia=0):
        base = cargar_configuracion(self.archivo_configuracion)
        return replace(
            base, broker=replace(base.broker, virtual_host=self.virtual_host),
            proceso=IdentidadProceso(grupo or self.grupo_fuente, instancia),
        )

    def abrir(self, grupo, instancia=0, **opciones):
        return self.recursos.enter_context(MiddlewareRabbitMQ(
            self.configurar(grupo, instancia), **opciones,
        ))

    def lanzar(self, funcion, grupo, *argumentos, instancia=0):
        contexto = multiprocessing.get_context('spawn')
        eventos = contexto.Queue(maxsize=64)
        proceso = contexto.Process(
            target=funcion,
            args=(self.configurar(grupo, instancia), eventos, *argumentos),
        )
        proceso.start()
        self.addCleanup(self.limpiar_proceso, proceso, eventos)
        return proceso, eventos

    def limpiar_proceso(self, proceso, eventos):
        if proceso.is_alive():
            # Solo se fuerza al auxiliar de prueba cuando falló o terminó el escenario.
            proceso.terminate()
        proceso.join(PLAZO_PROCESO)
        if proceso.is_alive():
            proceso.kill()
            proceso.join(PLAZO_PROCESO)
        eventos.close()
        eventos.join_thread()
        self.assertFalse(proceso.is_alive(), 'No se pudo cerrar el proceso auxiliar')
        proceso.close()

    def esperar(self, eventos):
        try:
            return eventos.get(timeout=PLAZO_PROCESO)
        except Empty:
            self.fail('El proceso auxiliar no informó progreso dentro del plazo')

    def finalizar(self, proceso):
        proceso.join(PLAZO_PROCESO)
        self.assertEqual(proceso.exitcode, 0, 'El proceso auxiliar falló o quedó bloqueado')

    def comprobar_bloqueado(self, eventos):
        # La notificación anterior proviene del bucle de E/S de la reserva real.
        self.assertEqual(self.esperar(eventos), 'esperando')
        with self.assertRaises(Empty):
            eventos.get(timeout=PLAZO_BLOQUEO)

    def recibir(self, middleware):
        recibidos = []
        middleware.recibir(recibidos.append)
        self.assertEqual(len(recibidos), 1)
        return recibidos[0]

    def estado(self, cola):
        return self.inspector.canal.queue_declare(queue=cola, passive=True).method

    def comprobar_capacidad_recuperada(self):
        for cola in self.topologia.colas_datos():
            with self.subTest(cola=cola):
                datos = self.estado(cola)
                permisos = self.estado(self.topologia.cola_capacidad(cola))
                self.assertEqual((datos.message_count, datos.consumer_count), (0, 0))
                self.assertEqual((permisos.message_count, permisos.consumer_count), (1, 0))


def observar_reserva(middleware, eventos, numero=1):
    """Notifica desde el bucle de E/S al esperar la reserva indicada, sin sustituirla."""
    adquirir = middleware._capacidad.adquirir
    llamadas = 0

    def observado(cola):
        nonlocal llamadas
        llamadas += 1
        if llamadas == numero:
            middleware._conexion.programar(lambda: eventos.put('esperando'))
        return adquirir(cola)

    middleware._capacidad.adquirir = observado
