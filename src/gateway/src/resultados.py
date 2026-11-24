
import logging
import socket
import threading
import time

from ...common.external_protocol import TipoFrame, codificar_resultado, empaquetar_resultado, enviar_frame
from ...common.protocol.primitivas import BYTES_U64
from ...workers.base_worker import Worker

LOG = logging.getLogger('gateway')

# Recibo resultados desde los workers, los guardo en disco y un emisor por conexión los entrega al cliente

class SesionCliente:
    """Conexión de un cliente. Los hilos del cliente y del emisor comparten el socket,
    por eso todo envío pasa por enviar(), que los serializa."""

    def __init__(self, conexion, max_bytes):
        self._socket = conexion
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._rota = threading.Event()
        self._al_romperse = None

    def enviar(self, tipo, cuerpo):
        with self._lock:
            if self.rota:
                raise ConnectionError("La conexión del cliente ya no está disponible")
            try:
                enviar_frame(self._socket, tipo, cuerpo, self._max_bytes)
            except OSError:
                self.marcar_rota()
                raise

    @property
    def rota(self):
        return self._rota.is_set()

    def al_romperse(self, avisar):
        """Registra a quién despertar cuando la conexión deja de servir."""
        self._al_romperse = avisar
        if self.rota:
            avisar()

    def cerrar(self):
        """Corta la conexión para despertar a quien espera en ella."""
        self.marcar_rota()
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def marcar_rota(self):
        self._rota.set()
        avisar = self._al_romperse
        if avisar is not None:
            avisar()


def emitir_resultados(ejecucion, sesion, ultima_secuencia):
    """Envía por la sesión los resultados de la ejecución desde la secuencia siguiente a ultima_secuencia.

    Espera en la condición de la ejecución (sin polling) a que haya un resultado nuevo, la ejecución
    se complete o la conexión se rompa. Con el cursor al final de la ejecución completa envía FIN,
    lo anota y borra los logs. Termina al romperse la sesión o cerrarse la ejecución.
    """
    cursor = ultima_secuencia
    while True:
        with ejecucion.condicion:
            ejecucion.condicion.wait_for(
                lambda: sesion.rota or ejecucion.cerrada
                or cursor < ejecucion.cantidad_resultados or ejecucion.completa
            )
            if sesion.rota or ejecucion.cerrada:
                return
            hay_resultado = cursor < ejecucion.cantidad_resultados
            contenido = ejecucion.leer_resultado(cursor + 1) if hay_resultado else None

        try:
            if not hay_resultado:
                sesion.enviar(TipoFrame.FIN, b"")
                if ejecucion.finalizar():
                    LOG.info("Ejecución %s: resultados entregados al cliente. Ejecución de inicio a fin: %.1f s",
                             ejecucion.id, time.monotonic() - ejecucion.inicio)
                return
            cursor += 1
            sesion.enviar(TipoFrame.RESULTADO, empaquetar_resultado(cursor, contenido))
        except OSError:
            LOG.warning("Ejecución %s: se perdió la conexión con el cliente mientras se enviaban resultados",
                        ejecucion.id)
            return


class ReceptorResultados(Worker):
    """Consume los resultados del grupo del gateway y los guarda en el log de su ejecución.

    No le escribe al socket: cada lote aceptado se persiste con fsync y recién después se confirma
    al broker. Un emisor por conexión (emitir_resultados) los entrega al cliente.
    """

    def __init__(self, middleware, registro, max_bytes):
        super().__init__(middleware)
        self._registro = registro
        self._max_bytes = max_bytes

    def procesar(self, datos):
        id_ejecucion = datos.entrada.id_ejecucion
        ejecucion = self._registro.obtener(id_ejecucion)
        if ejecucion is None:
            LOG.debug("Resultados de la ejecución %s descartados: es desconocida, terminó o fue abandonada",
                      id_ejecucion)
            return
        entrega = self._middleware.entrega_actual
        clave = (datos.entrada.nombre, entrega.id_entrada, entrega.indice)
        if ejecucion.ya_vista(clave):
            LOG.debug("Ejecución %s: resultado duplicado %s descartado", id_ejecucion, clave)
            return
        try:
            # La secuencia se antepone al enviar, por eso el contenido deja lugar para ella
            contenido = codificar_resultado(datos.entrada.nombre, datos.contenido, self._max_bytes - BYTES_U64,
                                            comprobar_detencion=self.comprobar_detencion)
        except ValueError:
            LOG.exception("Ejecución %s: no se pudo guardar un lote de %s", id_ejecucion, datos.entrada.nombre)
            return
        if not ejecucion.agregar_resultado(clave, contenido):
            LOG.debug("Ejecución %s: resultado %s descartado", id_ejecucion, clave)

    def finalizar_entrada(self, entrada):
        ejecucion = self._registro.obtener(entrada.id_ejecucion)
        if ejecucion is None:
            return
        if ejecucion.registrar_entrada_cerrada(entrada.nombre):
            LOG.info("Ejecución %s: todos los resultados fueron recibidos", entrada.id_ejecucion)
