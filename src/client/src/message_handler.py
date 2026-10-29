import struct
import uuid

from ...common.protocol.dto import CierreProductor, Entrada, IdentidadProceso, SobreMensaje, TipoMensaje
from ...common.protocol.registros import Esquema, Lote

# Para que el gateway pueda identificar que el mensaje viene de un cliente
IDENTIDAD_CLIENTE = IdentidadProceso("client", 0) 
BYTES_8 = 8

# Maneja la serializacion de mensajes y el conteo de lotes enviados y confirmados
#tambien con uuid me aseguro que cada cliente tenga un ID unico
class ManejadorMensajes:
    def __init__(self, codificador, nombre_entrada):
        self._codificador = codificador
        self.entrada = Entrada(str(uuid.uuid4()), nombre_entrada)
        self.lotes_confirmados = 0

    @property
    def id_ejecucion(self):
        return self.entrada.id_ejecucion

    def serializar_lote(self, registros):
        return self._codificador.codificar(
            SobreMensaje(self.entrada, TipoMensaje.DATOS, IDENTIDAD_CLIENTE,
            Lote(Esquema.ARTICULO, tuple(registros)),
        ))

    def serializar_eof(self):
        return self._codificador.codificar(
            SobreMensaje(self.entrada, TipoMensaje.EOF, IDENTIDAD_CLIENTE,
            CierreProductor(self.lotes_confirmados),
        ))

    def validar_ack(self, contenido, esperado):
        # True si el ACK es valido y con la cantidad de lotes esperados 
        #!Q = unsigned long long (8 bytes) big-endian
        return len(contenido) == BYTES_8 and struct.unpack("!Q", contenido)[0] == esperado

    def lote_confirmado(self):
        self.lotes_confirmados += 1