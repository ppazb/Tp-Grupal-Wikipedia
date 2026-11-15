"""Registros binarios delimitados e íntegros para el log de escritura anticipada."""

from dataclasses import dataclass
from zlib import crc32

from .archivos import ArchivoTruncado, leer_completo


FIRMA_REGISTRO = b'WALR'
FIRMA_CONFIRMACION = b'FINR'
BYTES_SECUENCIA = 8
BYTES_LONGITUD = 4
BYTES_CRC = 4
BYTES_CABECERA = len(FIRMA_REGISTRO) + BYTES_SECUENCIA + BYTES_LONGITUD
BYTES_CIERRE = BYTES_CRC + len(FIRMA_CONFIRMACION)
MAX_SECUENCIA = (1 << (8 * BYTES_SECUENCIA)) - 1
MAX_LONGITUD = (1 << (8 * BYTES_LONGITUD)) - 1


class ErrorWAL(Exception):
    """El almacenamiento no pudo completar una operación o su uso es inválido."""


class WALCorrupto(ErrorWAL):
    """Un registro completo tiene formato, secuencia o integridad inválidos."""


class WALOcupado(ErrorWAL):
    """Otro propietario mantiene abierto el archivo de log."""


class ColaIncompleta(ErrorWAL):
    """El archivo termina antes de completar el último registro."""


@dataclass(frozen=True)
class RegistroWAL:
    """Operación binaria con una secuencia local al archivo, independiente del ID de lote."""

    secuencia: int
    contenido: bytes


def delimitar_registro(secuencia, contenido):
    """Construye cabecera y cierre sin copiar el contenido de la operación."""
    cabecera = (FIRMA_REGISTRO + secuencia.to_bytes(BYTES_SECUENCIA, 'big')
                + len(contenido).to_bytes(BYTES_LONGITUD, 'big'))
    crc_cabecera = crc32(cabecera).to_bytes(BYTES_CRC, 'big')
    crc_registro = crc32(contenido, crc32(cabecera)).to_bytes(BYTES_CRC, 'big')
    return cabecera + crc_cabecera, crc_registro + FIRMA_CONFIRMACION


def leer_registro(archivo, secuencia, max_bytes, comprobar_detencion):
    """Lee una operación acotada; distingue EOF limpio, cola incompleta y corrupción."""
    try:
        cabecera = leer_completo(archivo, BYTES_CABECERA, admitir_fin=True,
                                comprobar_detencion=comprobar_detencion)
        if cabecera is None:
            return None
        crc_cabecera = leer_completo(archivo, BYTES_CRC,
                                    comprobar_detencion=comprobar_detencion)
        longitud = _validar_cabecera(cabecera, crc_cabecera, secuencia, max_bytes)
        contenido = leer_completo(archivo, longitud, comprobar_detencion=comprobar_detencion)
        cierre = leer_completo(archivo, BYTES_CIERRE, comprobar_detencion=comprobar_detencion)
    except ArchivoTruncado as error:
        raise ColaIncompleta('Último registro del WAL incompleto') from error
    crc_esperado = crc32(contenido, crc32(cabecera))
    if cierre[BYTES_CRC:] != FIRMA_CONFIRMACION or int.from_bytes(cierre[:BYTES_CRC], 'big') != crc_esperado:
        raise WALCorrupto(f'Integridad inválida en el registro {secuencia}')
    return RegistroWAL(secuencia, bytes(contenido))


def _validar_cabecera(cabecera, crc, secuencia, max_bytes):
    if cabecera[:len(FIRMA_REGISTRO)] != FIRMA_REGISTRO or int.from_bytes(crc, 'big') != crc32(cabecera):
        raise WALCorrupto(f'Cabecera inválida en el registro {secuencia}')
    inicio = len(FIRMA_REGISTRO)
    numero = int.from_bytes(cabecera[inicio:inicio + BYTES_SECUENCIA], 'big')
    if numero != secuencia:
        raise WALCorrupto(f'Se esperaba el registro {secuencia}, se encontró {numero}')
    longitud = int.from_bytes(cabecera[-BYTES_LONGITUD:], 'big')
    if longitud > max_bytes:
        raise ErrorWAL(f'El registro {secuencia} supera max_bytes_por_registro; no se modifica el archivo')
    return longitud
