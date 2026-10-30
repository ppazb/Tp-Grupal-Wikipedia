import logging
import os
import signal
import socket
import threading

from ...common.external_protocol import TipoFrame, enviar_frame, recibir_frame
from .message_handler import LoteDemasiadoGrande, ManejadorMensajes
from .parquet_handler import iterar_lotes


LOG = logging.getLogger(__name__)


# Configuración desde el entorno 
INPUT_DIR = os.environ["INPUT_DIR"]
BATCH_SIZE = int(os.environ["BATCH_SIZE"])
MAX_FRAME_BYTES = int(os.environ["MAX_FRAME_BYTES"])
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
CONNECT_TIMEOUT_SECONDS = float(os.environ["CONNECT_TIMEOUT_SECONDS"])
SOCKET_TIMEOUT_SECONDS = float(os.environ["SOCKET_TIMEOUT_SECONDS"])
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


# Responde con ACK al ERROR del gateway para que pueda cerrar la conexión
def _confirmar_error(socket, max_bytes):
    try:
        enviar_frame(socket, TipoFrame.ACK, b"", max_bytes)
    except OSError:
        LOG.debug("No se pudo confirmar el ERROR al gateway")

# Lee la respuesta del gateway y valida que confirme esperado lotes.
def _esperar_confirmacion(socket, max_bytes, manejador, esperado, descripcion, detener):
    respuesta = recibir_frame(socket, max_bytes, interrumpido=detener.is_set)
    if respuesta is None:
        raise ConnectionError(f"El gateway cerró la conexión antes de confirmar {descripcion}")
    
    tipo, contenido = respuesta

    if tipo is TipoFrame.ERROR:
        _confirmar_error(socket, max_bytes)
        raise RuntimeError(contenido.decode("utf-8", errors="replace"))
    
    if tipo is not TipoFrame.ACK or not manejador.validar_ack(contenido, esperado):
        raise ConnectionError(f"El gateway no confirmó correctamente {descripcion}")

# Envia las filas en uno o mas lotes, un lote que no entra en un frame se divide a la mitad
def _enviar_filas(socket, manejador, max_bytes, filas, detener):

    pendientes = [filas]
    while pendientes:
        lote = pendientes.pop(0)
        try:
            cuerpo = manejador.serializar_lote(lote)
        
        except LoteDemasiadoGrande as error:
            if len(lote) == 1:
                # Se asume que una fila siempre entra entera: si no, es un error, no un descarte.
                raise LoteDemasiadoGrande(f"Una fila no entra en un frame: {error}") from error
            mitad = len(lote) // 2
            pendientes[0:0] = (lote[:mitad], lote[mitad:])
            continue

        enviar_frame(socket, TipoFrame.DATOS, cuerpo, max_bytes)
        _esperar_confirmacion(socket, max_bytes, manejador, manejador.lotes_confirmados + 1, "el lote", detener)
        # se cuenta recién al ser confirmado por el gateway
        manejador.lote_confirmado()  


# Envía todos los Parquet del directorio: un único EOF al final
def enviar_parquets(ruta_directorio, socket, max_bytes, tamano_lote, detener):
    manejador = ManejadorMensajes(max_bytes)
    filas_enviadas = 0
    for filas in iterar_lotes(ruta_directorio, tamano_lote):
        if detener.is_set():
            LOG.warning("Interrumpido antes del EOF")
            return 143

        _enviar_filas(socket, manejador, max_bytes, filas, detener)
        filas_enviadas += len(filas)

    enviar_frame(socket, TipoFrame.EOF, manejador.serializar_eof(), max_bytes)
    _esperar_confirmacion(socket, max_bytes, manejador, manejador.lotes_confirmados, "el EOF", detener)

    LOG.info("EOF confirmado: %d filas enviadas en %d lotes", filas_enviadas, manejador.lotes_confirmados)
    return 0


def ejecutar_client() -> int:
    detener = threading.Event()

    def solicitar_detencion(signum, frame):
        detener.set()

    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    #sigterm y sigint
    signal.signal(signal.SIGTERM, solicitar_detencion)
    signal.signal(signal.SIGINT, solicitar_detencion)

    try:
        if BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE debe ser mayor que cero")
        if MAX_FRAME_BYTES <= 0:
            raise ValueError("MAX_FRAME_BYTES debe ser mayor que cero")

        with socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=CONNECT_TIMEOUT_SECONDS) as conexion:
            conexion.settimeout(SOCKET_TIMEOUT_SECONDS)
            return enviar_parquets(INPUT_DIR, conexion, MAX_FRAME_BYTES, BATCH_SIZE, detener)

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