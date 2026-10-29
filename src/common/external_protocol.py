
from enum import IntEnum
import socket
import struct

from .protocol.primitivas import BYTES_U32, EscritorBinario, LectorBinario

# Campos de una fila cruda, en el orden en que viajan; todos son textos (None se toma como "").
# Los campos struct del Parquet (image, in_language) llegan ya aplanados.
CAMPOS_FILA = (
    "abstract", "date_modified", "image_content_url", "in_language", "name", "sections", "url",
)


class TipoFrame(IntEnum):
    DATOS = 0x01
    EOF = 0x02
    ACK = 0x03
    ERROR = 0x04


# Envia un frame TCP con el tipo y cuerpo indicados, respetando el limite de bytes.
# | 4 bytes longitud (big-endian)| 1 byte tipo | N bytes cuerpo |
def enviar_frame(socket: socket.socket, tipo: TipoFrame, cuerpo: bytes, max_bytes: int) -> None:
    if type(cuerpo) is not bytes:
        raise TypeError("El cuerpo del frame debe ser bytes")
    
    if type(max_bytes) is not int or max_bytes <= 0 or len(cuerpo) > max_bytes:
        raise ValueError("El cuerpo del frame supera el max_bytes permitido")
    
    longitud = 1 + len(cuerpo)
    socket.sendall(struct.pack("!I", longitud) + bytes((tipo,)) + cuerpo)


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
    longitud = struct.unpack("!I", cabecera)[0]
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
        except socket.timeout:
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


# Cuerpo de un frame DATOS: | u32 cantidad de filas | por fila, un texto u32-prefijado por campo de CAMPOS_FILA |
# Eleva ErrorSerializacion (ValueError) si no entra en max_bytes.
def codificar_filas(filas, max_bytes: int) -> bytes:
    escritor = EscritorBinario(max_bytes)
    escritor.entero(len(filas), BYTES_U32)
    
    for fila in filas:
        for campo in CAMPOS_FILA:
            escritor.texto(fila[campo])
    
    return escritor.resultado()


def decodificar_filas(cuerpo: bytes) -> list[dict[str, str]]:
    lector = LectorBinario(cuerpo)
    filas = []
    
    for _ in range(lector.entero(BYTES_U32)):
        filas.append({campo: lector.texto() for campo in CAMPOS_FILA})
    lector.finalizar()
    
    return filas
