"""Distribuye una solicitud de detención a los hilos y sus esperas de RabbitMQ."""

from contextlib import contextmanager
import socket
import threading


class _AvisoHilo:
    """Descriptor exclusivo de un hilo; no instala manejadores de señales."""

    def __init__(self, detener):
        self._detener = detener
        self._lectura, self._escritura = socket.socketpair()
        self._lectura.setblocking(False)
        self._escritura.setblocking(False)

    @property
    def solicitado(self):
        return self._detener.is_set()

    def fileno(self):
        return self._lectura.fileno()

    def notificar(self):
        try:
            if self._escritura.send(b'\0') != 1:
                raise OSError('No se pudo notificar el apagado del hilo')
        except BlockingIOError:
            pass  # El descriptor lleno ya tiene un aviso pendiente.

    def consumir_avisos(self):
        while True:
            try:
                if not self._lectura.recv(4096):
                    raise OSError('Se cerró el descriptor de apagado del hilo')
            except BlockingIOError:
                return

    def cerrar(self):
        self._lectura.close()
        self._escritura.close()


class DetencionGateway:
    """Comparte el estado de detención y despierta un descriptor por conexión AMQP.

    set() se invoca fuera del manejador de señal. El registro y cierre de avisos
    se sincronizan para no escribir sobre un descriptor que otro hilo cerró.
    """

    def __init__(self):
        self._evento = threading.Event()
        self._lock = threading.Lock()
        self._avisos = set()

    def is_set(self):
        return self._evento.is_set()

    def wait(self):
        return self._evento.wait()

    def set(self):
        with self._lock:
            if self.is_set():
                return
            self._evento.set()
            errores = []
            for aviso in self._avisos:
                try:
                    aviso.notificar()
                except OSError as error:
                    errores.append(error)
            if errores:
                raise ExceptionGroup('Falló la notificación de apagado a los hilos', errores)

    @contextmanager
    def aviso(self):
        """Mantiene el descriptor abierto hasta que su hilo cierre el middleware."""
        aviso = _AvisoHilo(self)
        try:
            with self._lock:
                self._avisos.add(aviso)
                if self.is_set():
                    aviso.notificar()
            yield aviso
        finally:
            with self._lock:
                self._avisos.discard(aviso)
                aviso.cerrar()
