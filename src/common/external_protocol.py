
from enum import IntEnum
import socket

from .protocol.primitivas import BYTES_U8, BYTES_U16, BYTES_U32, BYTES_U64, EscritorBinario, LectorBinario
from .protocol.registros import Esquema
from .protocol.serializer import escribir_lote, leer_lote

# El nombre se captura antes de que los parámetros `socket` lo oculten en las funciones de abajo
_TIMEOUT = socket.timeout

# Campos de una fila cruda, en el orden en que viajan; todos son textos (None se toma como "").
# Los campos struct del Parquet (image, in_language) llegan ya aplanados.
# "sections" es la excepción: viaja como lista de textos (CAMPOS_LISTA).
CAMPOS_FILA = (
    "abstract", "date_modified", "image_content_url", "in_language", "name", "sections", "url",
)
CAMPOS_LISTA = frozenset({"sections"})


class TipoFrame(IntEnum):
    DATOS = 0x01
    EOF = 0x02
    ACK = 0x03
    ERROR = 0x04
    RESULTADO = 0x05 # gateway -> cliente: un lote de resultados de una consulta
    FIN = 0x06 # gateway -> cliente: no quedan más resultados de la ejecución
    INICIO = 0x07 # cliente -> gateway (cuerpo vacio): pide una ejecución nueva
    SESION = 0x08 # gateway -> cliente: id de la ejecución asignada
    REANUDAR = 0x09 # cliente -> gateway: id de la ejecución y última secuencia de resultado recibida
    REANUDADO = 0x0A # gateway -> cliente: lotes procesados y si ya llegó el EOF


# Envia un frame TCP con el tipo y cuerpo indicados, respetando el limite de bytes.
# | 4 bytes longitud (big-endian)| 1 byte tipo | N bytes cuerpo |
def enviar_frame(socket: socket.socket, tipo: TipoFrame, cuerpo: bytes, max_bytes: int) -> None:
    if type(cuerpo) is not bytes:
        raise TypeError("El cuerpo del frame debe ser bytes")
    
    if type(max_bytes) is not int or max_bytes <= 0 or len(cuerpo) > max_bytes:
        raise ValueError("El cuerpo del frame supera el max_bytes permitido")
    
    longitud = 1 + len(cuerpo)
    socket.sendall(longitud.to_bytes(4, "big") + bytes((tipo,)) + cuerpo)


# Recibe un frame TCP y devuelve una tupla (tipo, cuerpo) o none si se cerró la conexión.
# Permitir cierre: identificar si es un momento donde el cierre de la conexión es aceptable (termine de mandar frame) o no (mitad frame, quedo cortado)
# Interrumpido se consulta solo si el socket tiene timeout, para poder interrumpir la recepción de un frame TCP en curso.
def recibir_frame(socket: socket.socket, max_bytes: int,*, permitir_cierre: bool = False,interrumpido=None) -> tuple[TipoFrame, bytes] | None:
    
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("El límite de bytes debe ser un entero positivo")
    
    cabecera = recibir_exactamente(socket, 4, permitir_cierre=permitir_cierre, interrumpido=interrumpido,)
    # recibi_exactamente devuelve None si se cerró la conexion 
    if cabecera is None:
        return None
    longitud = int.from_bytes(cabecera, "big")
    if not 1 <= longitud <= max_bytes + 1:
        raise ValueError("El frame TCP tiene una longitud inválida")
    
    frame = recibir_exactamente(socket, longitud, interrumpido=interrumpido)
    try:
        tipo = TipoFrame(frame[0])
    except ValueError as error:
        raise ValueError("El frame TCP tiene un tipo desconocido") from error
    
    return tipo, frame[1:]


#Recibe un scocket y una cantidad de bytes, y devuelve exactamente esa cantidad de bytes o None si se cerró la conexión.
def recibir_exactamente(socket: socket.socket,cantidad: int,*,permitir_cierre: bool = False,interrumpido=None) -> bytes | None:
    if type(cantidad) is not int or cantidad < 0:
        raise ValueError("La cantidad de bytes debe ser un entero no negativo")
    
    partes = bytearray()
    while len(partes) < cantidad:
        try:
            fragmento = socket.recv(cantidad - len(partes))

        #si hay timeout y hay una funcion interrumpido, se consulta si se interrumpio la recepcion TCP
        except _TIMEOUT:
            if interrumpido is not None and interrumpido():
                raise InterruptedError("La recepción TCP fue interrumpida")
            continue #volver a intentar recibir 
        
        if not fragmento:
            #si permito cierre y no recibi partes, es porque se cerro la conexion en un momento aceptable, devuelvo None
            if permitir_cierre and not partes:
                return None
            raise ConnectionError("La conexión TCP se cerró durante un frame")
        partes.extend(fragmento)
    return bytes(partes)


# Filas de un lote: | u32 cantidad de filas | por fila, un campo de CAMPOS_FILA cada uno |
# Un campo de texto es un texto u32-prefijado; un campo de CAMPOS_LISTA es | u32 cantidad | N textos u32-prefijados |
def _escribir_filas(escritor, filas) -> None:
    escritor.entero(len(filas), BYTES_U32)

    for fila in filas:
        for campo in CAMPOS_FILA:
            if campo in CAMPOS_LISTA:
                escritor.entero(len(fila[campo]), BYTES_U32)
                for texto in fila[campo]:
                    escritor.texto(texto)
            else:
                escritor.texto(fila[campo])


# Cuerpo de las filas sin número de lote. Eleva ErrorSerializacion (ValueError) si no entra en max_bytes.
def codificar_filas(filas, max_bytes: int, *, comprobar_detencion=None) -> bytes:
    escritor = EscritorBinario(max_bytes, comprobar_detencion=comprobar_detencion)
    _escribir_filas(escritor, filas)
    return escritor.resultado()


# Cuerpo de un frame DATOS: | u64 número de lote (desde 1) | filas |
def codificar_datos(numero_lote: int, filas, max_bytes: int, *, comprobar_detencion=None) -> bytes:
    escritor = EscritorBinario(max_bytes, comprobar_detencion=comprobar_detencion)
    escritor.entero(numero_lote, BYTES_U64)
    _escribir_filas(escritor, filas)
    return escritor.resultado()


def _leer_campo(lector, campo):
    if campo in CAMPOS_LISTA:
        return tuple(lector.texto() for _ in range(lector.entero(BYTES_U32)))
    return lector.texto()


def _leer_filas(lector) -> list[dict]:
    filas = []

    for _ in range(lector.entero(BYTES_U32)):
        filas.append({campo: _leer_campo(lector, campo) for campo in CAMPOS_FILA})

    return filas


def decodificar_filas(cuerpo: bytes, *, comprobar_detencion=None) -> list[dict]:
    lector = LectorBinario(cuerpo, comprobar_detencion=comprobar_detencion)
    filas = _leer_filas(lector)
    lector.finalizar()
    return filas


# Devuelve (número de lote, filas) de un frame DATOS.
def decodificar_datos(cuerpo: bytes, *, comprobar_detencion=None) -> tuple[int, list[dict]]:
    lector = LectorBinario(cuerpo, comprobar_detencion=comprobar_detencion)
    numero_lote = lector.entero(BYTES_U64)
    filas = _leer_filas(lector)
    lector.finalizar()
    return numero_lote, filas


# Contenido de un resultado: | texto nombre de la entrada | u16 esquema | u32 cantidad | registros |
# Es un lote del protocolo interno tal cual salió del último worker, para no reenviar registro por registro.
# Es lo que el gateway guarda en disco; la secuencia se antepone recién al enviarlo.
# Eleva ErrorSerializacion (ValueError) si no entra en max_bytes.
def codificar_resultado(nombre_entrada: str, lote, max_bytes: int, *, comprobar_detencion=None) -> bytes:
    escritor = EscritorBinario(max_bytes, comprobar_detencion=comprobar_detencion)
    escritor.texto(nombre_entrada)
    escribir_lote(escritor, lote)
    return escritor.resultado()


# Cuerpo de un frame RESULTADO: | u64 secuencia (desde 1) | contenido de codificar_resultado |
# La secuencia identifica al resultado dentro de la ejecución. El contenido debe haberse
# codificado con max_bytes - BYTES_U64 para que el cuerpo entre en el frame.
def empaquetar_resultado(secuencia: int, contenido: bytes) -> bytes:
    escritor = EscritorBinario(BYTES_U64 + len(contenido))
    escritor.entero(secuencia, BYTES_U64)
    escritor.agregar(contenido)
    return escritor.resultado()


# Devuelve (secuencia, nombre de la entrada, lote). Un registro inválido eleva ErrorSerializacion (ValueError).
def decodificar_resultado(cuerpo: bytes, *, comprobar_detencion=None):
    lector = LectorBinario(cuerpo, comprobar_detencion=comprobar_detencion)
    secuencia = lector.entero(BYTES_U64)
    nombre = lector.texto()
    esquema = Esquema(lector.entero(BYTES_U16))
    lote, errores = leer_lote(lector, esquema)
    if errores:
        raise ValueError(f"El lote de resultados tiene registros inválidos: {errores[0].detalle}")
    return secuencia, nombre, lote


# Cuerpo de SESION: | texto id_ejecucion |
def codificar_sesion(id_ejecucion: str, max_bytes: int) -> bytes:
    escritor = EscritorBinario(max_bytes)
    escritor.texto(id_ejecucion)
    return escritor.resultado()


def decodificar_sesion(cuerpo: bytes) -> str:
    lector = LectorBinario(cuerpo)
    id_ejecucion = lector.texto()
    lector.finalizar()
    return id_ejecucion


# Cuerpo de REANUDAR: | texto id_ejecucion | u64 última secuencia de resultado recibida (0 si ninguna) |
def codificar_reanudar(id_ejecucion: str, ultima_secuencia: int, max_bytes: int) -> bytes:
    escritor = EscritorBinario(max_bytes)
    escritor.texto(id_ejecucion)
    escritor.entero(ultima_secuencia, BYTES_U64)
    return escritor.resultado()


def decodificar_reanudar(cuerpo: bytes) -> tuple[str, int]:
    lector = LectorBinario(cuerpo)
    id_ejecucion = lector.texto()
    ultima_secuencia = lector.entero(BYTES_U64)
    lector.finalizar()
    return id_ejecucion, ultima_secuencia


# Cuerpo de REANUDADO: | u64 lotes procesados | u8 EOF recibido (0 o 1) |
def codificar_reanudado(lotes_procesados: int, eof_recibido: bool) -> bytes:
    escritor = EscritorBinario(BYTES_U64 + BYTES_U8)
    escritor.entero(lotes_procesados, BYTES_U64)
    escritor.entero(1 if eof_recibido else 0, BYTES_U8)
    return escritor.resultado()


def decodificar_reanudado(cuerpo: bytes) -> tuple[int, bool]:
    lector = LectorBinario(cuerpo)
    lotes_procesados = lector.entero(BYTES_U64)
    eof_recibido = lector.entero(BYTES_U8)
    lector.finalizar()
    if eof_recibido not in (0, 1):
        raise ValueError("El indicador de EOF de REANUDADO debe ser 0 o 1")
    return lotes_procesados, bool(eof_recibido)
