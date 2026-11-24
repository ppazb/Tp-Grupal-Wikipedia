from contextlib import closing
import logging
import os
import socket
import time

from ...common.external_protocol import (
    TipoFrame, codificar_reanudar, decodificar_resultado, decodificar_reanudado, decodificar_sesion,
    enviar_frame, recibir_frame,
)
from ...common.runtime.apagado import Apagado
from .conexion import ConexionInterrumpible
from .message_handler import LoteDemasiadoGrande, ManejadorMensajes
from .parquet_handler import iterar_lotes
from .result_writer import EscritorResultados


LOG = logging.getLogger(__name__)


# Configuración desde el entorno 
INPUT_DIR = os.environ["INPUT_DIR"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]
CLIENT_ID = os.environ["CLIENT_ID"]
BATCH_SIZE = int(os.environ["BATCH_SIZE"])
MAX_FRAME_BYTES = int(os.environ["MAX_FRAME_BYTES"])
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
CONNECT_TIMEOUT_SECONDS = float(os.environ["CONNECT_TIMEOUT_SECONDS"])
SOCKET_TIMEOUT_SECONDS = float(os.environ["SOCKET_TIMEOUT_SECONDS"])
RECONEXION_MAX_SEGUNDOS = float(os.environ.get("RECONEXION_MAX_SEGUNDOS", "60"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
# Backoff entre intentos de reconexion:
ESPERA_RECONEXION_MIN_SEGUNDOS = 0.5
ESPERA_RECONEXION_MAX_SEGUNDOS = 5.0

# Cada cliente guarda sus resultados en OUTPUT_DIR/client_<id>
DIRECTORIO_RESULTADOS = os.path.join(OUTPUT_DIR, f"client_{CLIENT_ID}")


def _abrir_conexion():
    conexion = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=CONNECT_TIMEOUT_SECONDS)
    conexion.settimeout(SOCKET_TIMEOUT_SECONDS)
    return conexion


class SesionGateway:
    """
    Ejecución del cliente en el gateway, con una conexión que se restablece sola.

    Guarda en memoria (no en disco, porque un cliente que muere se relanza como nuevo) el id de la
    ejecución, la última secuencia de resultado escrita, el lote DATOS sin confirmar
    y si el EOF quedó sin confirmar.
    
    Ante un corte de la conexión reconecta con backoff, manda REANUDAR y, según lo que el
    gateway diga haber procesado, reenvía el lote o el EOF pendiente o los da por confirmados.
    No se reconecta ante InterruptedError (apagado) ni ante un frame ERROR del gateway.
    """

    def __init__(self, max_bytes, detener, escritor, abrir_conexion=_abrir_conexion,
                 reconexion_max_segundos=RECONEXION_MAX_SEGUNDOS):
        self._max_bytes = max_bytes
        self._detener = detener
        self._escritor = escritor
        self._abrir_conexion = abrir_conexion
        self._reconexion_max_segundos = reconexion_max_segundos
        self._conexion = None
        self._transporte = None
        self.id_ejecucion = None
        self.ultima_secuencia = 0
        self.lote_pendiente = None #(número, cuerpo) del ultimo DATOS sin confirmar
        self.eof_pendiente = False
        # lo que el gateway ya informo en el último REANUDADO
        self.lotes_procesados = 0
        self.eof_recibido = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._cerrar_conexion()

    def comprobar_detencion(self):
        if self._detener.solicitado:
            raise InterruptedError("Se solicitó detener el cliente")

    # CONEXION

    def iniciar(self):
        """Conecta y pide una ejecución nueva."""
        self._reconectar()

    def _cerrar_conexion(self):
        conexion, self._conexion, self._transporte = self._conexion, None, None
        if conexion is not None:
            conexion.close()

    def _conectar(self):
        self._conexion = self._abrir_conexion()
        self._transporte = ConexionInterrumpible(self._conexion, self._detener)
        self._transporte.comprobar_detencion()

    def _reconectar(self):
        """Abre una conexión nueva y la deja lista: INICIO si no hay ejecución, REANUDAR si la hay."""
        limite = time.monotonic() + self._reconexion_max_segundos
        espera = ESPERA_RECONEXION_MIN_SEGUNDOS
        self._cerrar_conexion()
        while True:
            self.comprobar_detencion()
            try:
                self._conectar()
                if self.id_ejecucion is None:
                    self._pedir_sesion()
                else:
                    self._reanudar()
                return
            except InterruptedError:
                raise
            except OSError as error:
                self._cerrar_conexion()
                restante = limite - time.monotonic()
                if restante <= 0:
                    raise ConnectionError(
                        f"No se pudo reconectar con el gateway en {self._reconexion_max_segundos:g} s: {error}"
                    ) from error
                LOG.warning("No se pudo conectar con el gateway (%s); se reintenta en %.1f s", error, min(espera, restante))
                # espera con backoff
                if self._detener.esperar(min(espera, restante)):
                    raise InterruptedError("Se solicitó detener el cliente") from error
                espera = min(espera * 2, ESPERA_RECONEXION_MAX_SEGUNDOS)

    def _pedir_sesion(self):
        enviar_frame(self._transporte, TipoFrame.INICIO, b"", self._max_bytes)
        tipo, cuerpo = self._leer_respuesta("la sesión")
        self._descartar_si_error(tipo, cuerpo)
        if tipo is not TipoFrame.SESION:
            raise ValueError("El gateway no respondió INICIO con SESION")
        self.id_ejecucion = decodificar_sesion(cuerpo)
        LOG.info("Ejecución %s iniciada", self.id_ejecucion)

    def _reanudar(self):
        enviar_frame(self._transporte, TipoFrame.REANUDAR,
                     codificar_reanudar(self.id_ejecucion, self.ultima_secuencia, self._max_bytes), self._max_bytes)
        tipo, cuerpo = self._leer_respuesta("la reanudación")
        self._descartar_si_error(tipo, cuerpo)
        if tipo is not TipoFrame.REANUDADO:
            raise ValueError("El gateway no respondió REANUDAR con REANUDADO")
        self.lotes_procesados, self.eof_recibido = decodificar_reanudado(cuerpo)
        LOG.info("Ejecución %s reanudada: el gateway tiene %d lotes procesados (EOF recibido: %s)",
                 self.id_ejecucion, self.lotes_procesados, self.eof_recibido)

    def _con_reconexion(self, intento, confirmado_al_reconectar=None):
        """Ejecuta intento(); si se corta la conexión, reconecta y reintenta salvo que ya esté confirmado."""
        while True:
            try:
                return intento()
            except InterruptedError:
                raise
            except OSError as error:
                LOG.warning("Se perdió la conexión con el gateway: %s", error)
                self._reconectar()
                if confirmado_al_reconectar is not None and confirmado_al_reconectar():
                    return None

    # RECEPCION

    # Lee la próxima respuesta del gateway que no sea un resultado. Los resultados pueden llegar
    # en cualquier momento, así que se guardan al pasar
    def _leer_respuesta(self, descripcion):
        while True:
            respuesta = recibir_frame(self._transporte, self._max_bytes, interrumpido=lambda: self._detener.solicitado)
            if respuesta is None:
                raise ConnectionError(f"El gateway cerró la conexión antes de confirmar {descripcion}")

            tipo, cuerpo = respuesta
            if tipo is not TipoFrame.RESULTADO:
                return tipo, cuerpo
            self._guardar_resultado(cuerpo)

    # Un resultado con secuencia ya escrita (reenvío tras reconectar) se ignora: se escribe una sola vez
    def _guardar_resultado(self, cuerpo):
        secuencia, nombre, lote = decodificar_resultado(cuerpo, comprobar_detencion=self._transporte.comprobar_detencion)
        if secuencia <= self.ultima_secuencia:
            return
        self._escritor.escribir(nombre, lote)
        self.ultima_secuencia = secuencia

    # Responde con ACK al ERROR del gateway para que pueda cerrar la conexión
    def _descartar_si_error(self, tipo, cuerpo):
        if tipo is not TipoFrame.ERROR:
            return
        try:
            enviar_frame(self._transporte, TipoFrame.ACK, b"", self._max_bytes)
        except InterruptedError:
            raise
        except OSError:
            LOG.debug("No se pudo confirmar el ERROR al gateway")
        raise RuntimeError(cuerpo.decode("utf-8", errors="replace"))

    def _esperar_ack(self, esperado, descripcion):
        tipo, cuerpo = self._leer_respuesta(descripcion)
        self._descartar_si_error(tipo, cuerpo)
        if tipo is not TipoFrame.ACK or not ManejadorMensajes.validar_ack(cuerpo, esperado):
            raise ValueError(f"El gateway no confirmó correctamente {descripcion}")

    #ENVIO

    def enviar_lote(self, numero, cuerpo):
        """Envía el lote DATOS y espera su ACK; si se corta la conexión lo reenvía salvo que el gateway ya lo tenga."""
        self.lote_pendiente = (numero, cuerpo)

        def intento():
            enviar_frame(self._transporte, TipoFrame.DATOS, cuerpo, self._max_bytes)
            self._esperar_ack(numero, "el lote")

        self._con_reconexion(intento, lambda: self.lotes_procesados >= numero)
        self.lote_pendiente = None

    def enviar_eof(self, cuerpo, lotes):
        """Envía el EOF y espera su ACK; si se corta la conexión lo reenvía salvo que el gateway ya lo tenga."""
        self.eof_pendiente = True

        def intento():
            enviar_frame(self._transporte, TipoFrame.EOF, cuerpo, self._max_bytes)
            self._esperar_ack(lotes, "el EOF")

        self._con_reconexion(intento, lambda: self.eof_recibido)
        self.eof_pendiente = False

    # Tras el EOF el gateway sigue enviando resultados hasta que termina la ejecución (FIN)
    def recibir_resultados(self):
        def intento():
            tipo, cuerpo = self._leer_respuesta("los resultados")
            self._descartar_si_error(tipo, cuerpo)
            if tipo is not TipoFrame.FIN:
                raise ValueError("El gateway envió un frame inesperado mientras entregaba resultados")

        self._con_reconexion(intento)


def _recibir_resultados(sesion):
    sesion.recibir_resultados()


# Envia las filas en uno o mas lotes, un lote que no entra en un frame se divide a la mitad
def _enviar_filas(sesion, manejador, filas):

    pendientes = [filas]
    while pendientes:
        sesion.comprobar_detencion()
        lote = pendientes.pop(0)
        # el número lo da la cantidad de lotes ya confirmados: un único lote queda sin confirmar a la vez
        numero = manejador.lotes_confirmados + 1
        try:
            cuerpo = manejador.serializar_lote(numero, lote)
        
        except LoteDemasiadoGrande as error:
            if len(lote) == 1:
                # Se asume que una fila siempre entra entera: si no, es un error, no un descarte.
                raise LoteDemasiadoGrande(f"Una fila no entra en un frame: {error}") from error
            mitad = len(lote) // 2
            pendientes[0:0] = (lote[:mitad], lote[mitad:])
            continue

        sesion.enviar_lote(numero, cuerpo)
        # se cuenta recién al ser confirmado por el gateway
        manejador.lote_confirmado()  


# Envía todos los Parquet del directorio: un único EOF al final
def enviar_parquets(ruta_directorio, sesion, max_bytes, tamano_lote):
    inicio = time.monotonic()
    manejador = ManejadorMensajes(max_bytes, comprobar_detencion=sesion.comprobar_detencion)
    filas_enviadas = 0
    # Se lee siempre del mismo iterador: tras una reconexión no se vuelve a leer nada
    with closing(iterar_lotes(ruta_directorio, tamano_lote, sesion.comprobar_detencion)) as lotes:
        for filas in lotes:
            sesion.comprobar_detencion()
            _enviar_filas(sesion, manejador, filas)
            filas_enviadas += len(filas)

    sesion.comprobar_detencion()

    sesion.enviar_eof(manejador.serializar_eof(), manejador.lotes_confirmados)

    LOG.info("EOF confirmado: %d filas enviadas en %d lotes; esperando resultados", filas_enviadas, manejador.lotes_confirmados)
    _recibir_resultados(sesion)
    sesion.comprobar_detencion()
    LOG.info("Resultados completos, guardados en %s. Ejecución de inicio a fin: %.1f s",
             DIRECTORIO_RESULTADOS, time.monotonic() - inicio)
    return 0


def ejecutar_client() -> int:
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        if BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE debe ser mayor que cero")
        if MAX_FRAME_BYTES <= 0:
            raise ValueError("MAX_FRAME_BYTES debe ser mayor que cero")

        if CONNECT_TIMEOUT_SECONDS <= 0 or SOCKET_TIMEOUT_SECONDS <= 0:
            raise ValueError("Los timeouts de conexión y socket deben ser positivos")
        if RECONEXION_MAX_SEGUNDOS <= 0:
            raise ValueError("RECONEXION_MAX_SEGUNDOS debe ser mayor que cero")
        with Apagado() as detener:
            def comprobar_detencion():
                if detener.solicitado:
                    raise InterruptedError("Se solicitó detener el cliente")

            escritor = EscritorResultados(DIRECTORIO_RESULTADOS, comprobar_detencion)
            detener.registrar_limpieza(escritor.cerrar)
            comprobar_detencion()
            with SesionGateway(MAX_FRAME_BYTES, detener, escritor) as sesion:
                sesion.iniciar()
                return enviar_parquets(INPUT_DIR, sesion, MAX_FRAME_BYTES, BATCH_SIZE)

    except InterruptedError:
        LOG.warning("Envío interrumpido antes de completar la ejecución")
        return 143
    except (ValueError, OSError) as error:
        LOG.error("No se pudieron enviar los Parquet al gateway: %s", error)
        return 1
    except Exception:
        LOG.exception("Falló el proceso cliente")
        return 2


def main() -> int:
    return ejecutar_client()
