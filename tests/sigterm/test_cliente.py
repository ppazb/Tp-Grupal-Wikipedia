"""Cliente real con Parquet local y un gateway TCP controlado por el escenario."""

import multiprocessing
import os
from pathlib import Path
import signal
import socket
import tempfile
import threading
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from src.common.external_protocol import TipoFrame, recibir_frame, enviar_frame, codificar_resultado
from src.common.protocol.registros import Esquema, FragmentoImagen, Lote


MAX_BYTES = 65536
PLAZO = 10


def ejecutar_cliente(entorno, avisos):
    """Ejecuta el cliente y verifica sus recursos antes de terminar el intérprete."""
    os.environ.update(entorno)
    from src.client.src import client
    anteriores = {numero: signal.getsignal(numero) for numero in (signal.SIGTERM, signal.SIGINT)}
    descriptores = set(os.listdir('/proc/self/fd'))
    recibir_resultados = client._recibir_resultados

    def observar_resultados(*args):
        avisos.send('esperando_resultados')
        return recibir_resultados(*args)

    client._recibir_resultados = observar_resultados
    resultado = client.ejecutar_client()
    assert set(os.listdir('/proc/self/fd')) == descriptores, 'Quedaron descriptores abiertos'
    assert all(signal.getsignal(n) == manejador for n, manejador in anteriores.items())
    assert len(threading.enumerate()) == 1
    assert not multiprocessing.active_children()
    avisos.send(resultado)
    avisos.close()


class Cliente(unittest.TestCase):
    def setUp(self):
        self.temporal = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporal.cleanup)
        self.raiz = Path(self.temporal.name)
        pq.write_table(pa.Table.from_pylist([{
            'abstract': 'Un artículo de prueba', 'date_modified': '2020-01-01',
            'image': None, 'in_language': None, 'name': 'Artículo',
            'sections': '[]', 'url': 'https://ejemplo/articulo',
        }]), self.raiz / 'entrada.parquet')
        self.servidor = socket.socket()
        self.addCleanup(self.servidor.close)
        self.servidor.bind(('127.0.0.1', 0))
        self.servidor.listen(1)
        self.servidor.settimeout(PLAZO)
        entorno = dict(INPUT_DIR=str(self.raiz), OUTPUT_DIR=str(self.raiz / 'salida'),
                       CLIENT_ID='prueba', BATCH_SIZE='1', MAX_FRAME_BYTES=str(MAX_BYTES),
                       SERVER_HOST='127.0.0.1', SERVER_PORT=str(self.servidor.getsockname()[1]),
                       CONNECT_TIMEOUT_SECONDS='5', SOCKET_TIMEOUT_SECONDS='60')
        contexto = multiprocessing.get_context('spawn')
        self.avisos, envio = contexto.Pipe(duplex=False)
        self.addCleanup(self.avisos.close)
        self.proceso = contexto.Process(target=ejecutar_cliente, args=(entorno, envio))
        self.proceso.start()
        envio.close()
        self.addCleanup(self.limpiar_proceso)
        self.conexion, _ = self.servidor.accept()
        self.addCleanup(self.conexion.close)
        self.conexion.settimeout(PLAZO)
        self.assertEqual(recibir_frame(self.conexion, MAX_BYTES)[0], TipoFrame.DATOS)

    def limpiar_proceso(self):
        if self.proceso.is_alive():
            self.proceso.kill()
        self.proceso.join(PLAZO)
        self.proceso.close()

    def finalizar(self, resultado, senal=True):
        if senal:
            os.kill(self.proceso.pid, signal.SIGTERM)
        self.proceso.join(PLAZO)
        if self.proceso.is_alive():
            self.proceso.kill()
            self.proceso.join(PLAZO)
            self.fail('El cliente excedió el plazo de apagado; se envió SIGKILL')
        self.assertEqual(self.proceso.exitcode, 0)
        self.assertTrue(self.avisos.poll(PLAZO))
        self.assertEqual(self.avisos.recv(), resultado)
        try:
            restante = self.conexion.recv(MAX_BYTES)
        except ConnectionResetError:
            # Un frame parcial puede quedar sin leer al cerrar el extremo cliente.
            restante = b''
        self.assertEqual(restante, b'', 'Se emitieron datos o EOF después de detenerse')

    def confirmar_entrada(self):
        enviar_frame(self.conexion, TipoFrame.ACK, (1).to_bytes(8, 'big'), MAX_BYTES)
        self.assertEqual(recibir_frame(self.conexion, MAX_BYTES)[0], TipoFrame.EOF)
        enviar_frame(self.conexion, TipoFrame.ACK, (1).to_bytes(8, 'big'), MAX_BYTES)
        self.assertTrue(self.avisos.poll(PLAZO))
        self.assertEqual(self.avisos.recv(), 'esperando_resultados')

    def test_sigterm_esperando_ack_no_envia_eof(self):
        self.finalizar(143)

    def test_sigterm_con_frame_incompleto(self):
        self.conexion.sendall(b'\x00\x00')
        self.finalizar(143)

    def test_sigterm_esperando_resultados_elimina_imagen_incompleta(self):
        # El ACK que sigue al resultado prueba que el cliente ya guardó el fragmento.
        lote = Lote(Esquema.FRAGMENTO_IMAGEN, (
            FragmentoImagen('pendiente', 'Imagen', 'url', 10, 0, b'abc'),))
        enviar_frame(self.conexion, TipoFrame.RESULTADO,
                     codificar_resultado('resultados_finales_q2', lote, MAX_BYTES), MAX_BYTES)
        self.confirmar_entrada()
        self.finalizar(143)
        self.assertEqual(list((self.raiz / 'salida/client_prueba/imagenes').iterdir()), [])

    def test_fin_normal_conserva_resultados_completos(self):
        lote = Lote(Esquema.FRAGMENTO_IMAGEN, (
            FragmentoImagen('completa', 'Imagen', 'url', 3, 0, b'abc'),))
        enviar_frame(self.conexion, TipoFrame.RESULTADO,
                     codificar_resultado('resultados_finales_q2', lote, MAX_BYTES), MAX_BYTES)
        self.confirmar_entrada()
        enviar_frame(self.conexion, TipoFrame.FIN, b'', MAX_BYTES)
        self.finalizar(0, senal=False)
        self.assertEqual((self.raiz / 'salida/client_prueba/imagenes/completa.img').read_bytes(), b'abc')


def enviar_hasta_saturar(conexion, avisos):
    """Llena un socket cuyo receptor no drena, usando el transporte del cliente."""
    from src.common.runtime.apagado import Apagado
    from src.client.src.conexion import ConexionInterrumpible
    with conexion, Apagado() as apagado:
        conexion.settimeout(60)
        transporte = ConexionInterrumpible(conexion, apagado)
        try:
            transporte.sendall(b'x' * (8 * 1024 * 1024))
        except InterruptedError:
            avisos.send('interrumpido')
        else:
            raise AssertionError('El envío saturado terminó sin ser interrumpido')
    avisos.close()


class EnvioCliente(unittest.TestCase):
    def test_sigterm_despierta_envio_saturado(self):
        contexto = multiprocessing.get_context('spawn')
        emisor, receptor = socket.socketpair()
        with emisor, receptor:
            emisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            receptor.settimeout(PLAZO)
            lectura, escritura = contexto.Pipe(duplex=False)
            proceso = contexto.Process(target=enviar_hasta_saturar, args=(emisor, escritura))
            proceso.start()
            escritura.close()
            try:
                self.assertEqual(receptor.recv(1), b'x')
                os.kill(proceso.pid, signal.SIGTERM)
                proceso.join(PLAZO)
                self.assertFalse(proceso.is_alive(), 'El envío no despertó por SIGTERM')
                self.assertEqual(proceso.exitcode, 0)
                self.assertTrue(lectura.poll(PLAZO))
                self.assertEqual(lectura.recv(), 'interrumpido')
            finally:
                if proceso.is_alive():
                    proceso.kill()
                    proceso.join(PLAZO)
                proceso.close()
                lectura.close()
