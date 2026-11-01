
import logging
import threading

from ...common.external_protocol import TipoFrame, codificar_resultado, enviar_frame
from ...workers.base_worker import Worker

LOG = logging.getLogger('gateway')

# Recibo resultados desde los workers y reenvio al cliente correspondiente

class SesionCliente:
    """Conexión de un cliente. Los hilos del cliente y del receptor comparten el socket,
    por eso todo envío pasa por enviar(), que los serializa."""

    def __init__(self, socket, max_bytes):
        self._socket = socket
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self.rota = False
        # Se activa cuando llegaron todos los resultados o cuando la sesión ya no sirve
        self.completa = threading.Event()

    def enviar(self, tipo, cuerpo):
        with self._lock:
            if self.rota:
                raise ConnectionError("La conexión del cliente ya no está disponible")
            try:
                enviar_frame(self._socket, tipo, cuerpo, self._max_bytes)
            except OSError:
                self.marcar_rota()
                raise

    def marcar_rota(self):
        self.rota = True
        self.completa.set()


class RegistroSesiones:
    """Asocia cada ejecución con la sesión del cliente que la inició."""

    def __init__(self):
        self._lock = threading.Lock()
        self._sesiones = {}

    def registrar(self, id_ejecucion, sesion):
        with self._lock:
            self._sesiones[id_ejecucion] = sesion

    def quitar(self, id_ejecucion):
        with self._lock:
            self._sesiones.pop(id_ejecucion, None)

    def obtener(self, id_ejecucion):
        with self._lock:
            return self._sesiones.get(id_ejecucion)

    def cerrar_todas(self):
        """Despierta a quienes esperan resultados cuando el gateway se detiene."""
        with self._lock:
            sesiones = tuple(self._sesiones.values())
        for sesion in sesiones:
            sesion.marcar_rota()


class ReceptorResultados(Worker):
    """Consume los resultados del grupo del gateway y los entrega al cliente de su ejecución.

    Cada lote recibido viaja como un único frame RESULTADO. La ejecución se da por
    completa cuando se cerraron todas sus entradas, no antes.
    """

    def __init__(self, middleware, sesiones, max_bytes):
        super().__init__(middleware)
        self._sesiones = sesiones
        self._max_bytes = max_bytes

    def procesar(self, datos):
        id_ejecucion = datos.entrada.id_ejecucion
        sesion = self._sesiones.obtener(id_ejecucion)
        if sesion is None:
            LOG.debug("Resultados de la ejecución %s descartados: su cliente ya no está", id_ejecucion)
            return
        try:
            sesion.enviar(
                TipoFrame.RESULTADO,
                codificar_resultado(datos.entrada.nombre, datos.contenido, self._max_bytes),
            )
        except OSError:
            LOG.warning("Ejecución %s: el cliente se desconectó, se descartan sus resultados", id_ejecucion)
        except ValueError:
            LOG.exception("Ejecución %s: no se pudo reenviar un lote de %s", id_ejecucion, datos.entrada.nombre)

    def finalizar_entrada(self, entrada):
        if self._middleware.entradas_pendientes(entrada.id_ejecucion):
            return
        sesion = self._sesiones.obtener(entrada.id_ejecucion)
        if sesion is not None:
            LOG.info("Ejecución %s: todos los resultados fueron recibidos", entrada.id_ejecucion)
            sesion.completa.set()
