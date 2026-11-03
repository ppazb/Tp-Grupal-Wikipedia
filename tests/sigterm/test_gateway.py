"""Apagado del gateway con TCP, RabbitMQ y sus hilos reales."""

import os
from pathlib import Path
import signal
import socket

from src.common.external_protocol import TipoFrame, codificar_filas, enviar_frame, recibir_frame
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, ResultadoQ1
from src.middleware.topologia import TopologiaGrupos
from tests.soporte import EscenarioRabbitMQ
from .gateway import ejecutar_gateway
from .soporte import entrada_prueba

MAX_BYTES = 65536
PLAZO = 15
FILA = dict(abstract='hotel tree', date_modified='2019', image_content_url='', in_language='en',
            name='Artículo', sections=(), url='http://wiki/articulo')


class Gateway(EscenarioRabbitMQ):
    archivo_configuracion = Path(__file__).with_name('config.json')
    grupo_fuente = 'gateway'

    def iniciar(self, modo='normal'):
        self.proceso, self.eventos = self.lanzar(ejecutar_gateway, 'gateway', modo)
        avisos = [self.esperar(self.eventos), self.esperar(self.eventos)]
        self.assertIn('receptor', avisos)
        self.puerto = next(aviso[1] for aviso in avisos if isinstance(aviso, tuple))

    def cliente(self):
        cliente = socket.socket()
        self.addCleanup(cliente.close)
        cliente.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        cliente.settimeout(PLAZO)
        cliente.connect(('127.0.0.1', self.puerto))
        aviso = self.esperar(self.eventos)
        self.assertEqual(aviso[0], 'cliente')
        return cliente, aviso[1]

    def enviar(self, cliente):
        enviar_frame(cliente, TipoFrame.DATOS, codificar_filas([FILA], MAX_BYTES), MAX_BYTES)

    def apagar(self, cierres=None):
        os.kill(self.proceso.pid, signal.SIGTERM)
        self.proceso.join(PLAZO)
        if self.proceso.is_alive():
            self.proceso.kill()
            self.proceso.join(PLAZO)
            self.fail('El gateway excedió el plazo; se envió SIGKILL')
        self.assertEqual(self.proceso.exitcode, 0)
        self.assertEqual(self.esperar(self.eventos), 'limpio')
        for grupo in self.configuracion.grupos:
            colas = TopologiaGrupos(self.configurar(grupo.nombre)).colas_recepcion()
            if colas is not None:
                self.assertEqual(self.estado(colas.control).message_count, (cierres or {}).get(colas.control, 0),
                                 'Se fabricó un EOF al apagar')
        for cola in self.topologia.colas_datos():
            self.assertEqual(self.estado(cola).consumer_count, 0)
            self.assertEqual(self.estado(self.topologia.cola_capacidad(cola)).consumer_count, 0)

    def test_sigterm_sin_clientes(self):
        self.iniciar()
        self.apagar()

    def test_sigterm_clientes_concurrentes_y_frame_incompleto(self):
        self.iniciar()
        inactivo, _ = self.cliente()
        parcial, _ = self.cliente()
        parcial.sendall(b'\0\0')
        activo, _ = self.cliente()
        self.enviar(activo)
        self.assertEqual(recibir_frame(activo, MAX_BYTES)[0], TipoFrame.ACK)
        with self.abrir('date_router', informar_error=self.fail) as receptor:
            self.recibir(receptor)
        segundo, _ = self.cliente()
        self.enviar(segundo)
        self.assertEqual(recibir_frame(segundo, MAX_BYTES)[0], TipoFrame.ACK)
        self.apagar()

    def test_sigterm_esperando_fin_de_resultados(self):
        self.iniciar()
        cliente, _ = self.cliente()
        self.enviar(cliente)
        self.assertEqual(recibir_frame(cliente, MAX_BYTES)[0], TipoFrame.ACK)
        enviar_frame(cliente, TipoFrame.EOF, (1).to_bytes(8, 'big'), MAX_BYTES)
        self.assertEqual(recibir_frame(cliente, MAX_BYTES)[0], TipoFrame.ACK)
        control = TopologiaGrupos(self.configurar('date_router')).colas_recepcion().control
        self.assertEqual(self.estado(control).message_count, 1)
        self.apagar({control: 1})
        self.assertEqual(cliente.recv(MAX_BYTES), b'', 'Se informó FIN sin recibir los resultados')

    def test_sigterm_publicacion_sin_capacidad(self):
        fuente, datos = entrada_prueba('date_router')
        with self.abrir(fuente, informar_error=self.fail) as productor:
            productor.publicar('date_router', datos)
        self.iniciar('capacidad')
        cliente, _ = self.cliente()
        self.enviar(cliente)
        self.assertEqual(self.esperar(self.eventos), 'sin_capacidad')
        self.apagar()
        cola = TopologiaGrupos(self.configurar('gateway')).destino_datos('date_router', datos.entrada.nombre).cola
        self.assertEqual(self.estado(cola).message_count, 1)
        self.assertEqual(cliente.recv(MAX_BYTES), b'', 'Se confirmó una publicación bloqueada')

    def test_sigterm_durante_envio_de_resultados(self):
        self.iniciar('envio')
        cliente, ejecucion = self.cliente()
        lote = Lote(Esquema.RESULTADO_Q1, (ResultadoQ1('x' * 60000, '2019'),))
        with self.abrir('q1_formatter', informar_error=self.fail) as productor:
            productor.publicar('gateway_resultados', Datos(Entrada(ejecucion, 'resultados_finales_q1'), lote))
        self.assertEqual(self.esperar(self.eventos), 'enviando')
        self.apagar()

    def test_sigterm_esperando_ack_de_error(self):
        self.iniciar()
        cliente, _ = self.cliente()
        enviar_frame(cliente, TipoFrame.ACK, b'', MAX_BYTES)
        self.assertEqual(recibir_frame(cliente, MAX_BYTES)[0], TipoFrame.ERROR)
        self.apagar()
