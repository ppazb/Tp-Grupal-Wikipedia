"""Operaciones TCP interrumpibles mediante el descriptor del apagado común."""

from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
import socket
from time import monotonic


class ConexionInterrumpible:
    """Espera disponibilidad o apagado, conservando el timeout de envío del socket."""

    def __init__(self, conexion, apagado):
        self._conexion = conexion
        self._apagado = apagado
        self._timeout = conexion.gettimeout()
        conexion.setblocking(False)

    def comprobar_detencion(self):
        if self._apagado.solicitado:
            raise InterruptedError('Se solicitó detener el cliente')

    def _esperar(self, evento, limite=None):
        self.comprobar_detencion()
        with DefaultSelector() as selector:
            selector.register(self._apagado.fileno(), EVENT_READ)
            selector.register(self._conexion, evento)
            plazo = None if limite is None else max(0, limite - monotonic())
            disponibles = selector.select(plazo)
        self.comprobar_detencion()
        if not disponibles:
            raise socket.timeout('Se agotó el plazo de envío TCP')

    def recv(self, cantidad):
        """Recibe bytes disponibles o espera datos y señales sin sondeo periódico."""
        while True:
            self._esperar(EVENT_READ)
            try:
                return self._conexion.recv(cantidad)
            except BlockingIOError:
                continue

    def sendall(self, datos):
        """Completa escrituras parciales o abandona el frame ante apagado o timeout."""
        limite = None if self._timeout is None else monotonic() + self._timeout
        pendiente = memoryview(datos)
        while pendiente:
            self._esperar(EVENT_WRITE, limite)
            try:
                escritos = self._conexion.send(pendiente)
            except BlockingIOError:
                continue
            if escritos == 0:
                raise ConnectionError('La conexión TCP se cerró durante el envío')
            pendiente = pendiente[escritos:]
