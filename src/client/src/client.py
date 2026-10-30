import logging
import os
import signal
import socket
import threading

import pyarrow.parquet as parquet

from ...common.external_protocol import TipoFrame, enviar_frame, recibir_frame
from .message_handler import LoteDemasiadoGrande, ManejadorMensajes


LOG = logging.getLogger(__name__)
COLUMNAS_PARQUET = (
    "abstract",
    "date_modified",
    "image",
    "in_language",
    "name",
    "sections",
    "url",
)

# Variables de entorno, se agregan default temporalmente, luego de merge 
# TODO: quitar el default y dejarlo todo en yml
INPUT_FILE = os.environ.get("INPUT_FILE")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "32"))
MAX_FRAME_BYTES = int(os.environ.get("MAX_FRAME_BYTES", "65536"))
SERVER_HOST = os.environ.get("SERVER_HOST", "gateway")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "5555"))
CONNECT_TIMEOUT_SECONDS = float(os.environ.get("CONNECT_TIMEOUT_SECONDS", "10"))
SOCKET_TIMEOUT_SECONDS = float(os.environ.get("SOCKET_TIMEOUT_SECONDS", "1"))
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


def enviar_parquet(ruta_archivo, socket, max_bytes, tamano_lote, detener):
    archivo = parquet.ParquetFile(ruta_archivo)
    faltantes = set(COLUMNAS_PARQUET) - set(archivo.schema_arrow.names)
    if faltantes:
        raise ValueError(f"Faltan columnas en el Parquet: {', '.join(sorted(faltantes))}")

    manejador = ManejadorMensajes(max_bytes)
    filas_enviadas = 0
    for lote_arrow in archivo.iter_batches(batch_size=tamano_lote, columns=list(COLUMNAS_PARQUET)):
        
        if detener.is_set():
            LOG.warning("Interrumpido antes del EOF")
            return 143
        
        filas = lote_arrow.to_pylist()
        if not filas:
            continue
        
        _enviar_filas(socket, manejador, max_bytes, filas, detener)
        filas_enviadas += len(filas)
        LOG.info("Lotes admitidos por el gateway: %d", manejador.lotes_confirmados)

    enviar_frame(socket, TipoFrame.EOF, manejador.serializar_eof(), max_bytes)
    _esperar_confirmacion(socket, max_bytes, manejador, manejador.lotes_confirmados, "el EOF", detener)

    LOG.info("EOF confirmado: %d filas enviadas en %d lotes", filas_enviadas, manejador.lotes_confirmados)
    return 0


def ejecutar_client() -> int:
    detener = threading.Event()

    def solicitar_detencion(signum, frame):
        detener.set()

    logging.basicConfig(level=LOG_LEVEL,format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    #sigterm y sigint
    signal.signal(signal.SIGTERM, solicitar_detencion)
    signal.signal(signal.SIGINT, solicitar_detencion)

    try:
        if not INPUT_FILE:
            raise ValueError("INPUT_FILE es obligatorio: ruta del Parquet a enviar")
        if BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE debe ser mayor que cero")
        if MAX_FRAME_BYTES <= 0:
            raise ValueError("MAX_FRAME_BYTES debe ser mayor que cero")

        with socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=CONNECT_TIMEOUT_SECONDS) as conexion:
            conexion.settimeout(SOCKET_TIMEOUT_SECONDS)
            return enviar_parquet(INPUT_FILE, conexion, MAX_FRAME_BYTES, BATCH_SIZE, detener)

    except InterruptedError:
        LOG.warning("Envío interrumpido antes de completar la ejecución")
        return 143
    except (ValueError, OSError) as error:
        LOG.error("No se pudo enviar el Parquet al gateway: %s", error)
        return 1
    except Exception:
        LOG.exception("Falló el proceso cliente")
        return 2


def main() -> int:
    return ejecutar_client()