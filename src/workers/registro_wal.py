"""
Formato común de los registros de WAL: [tipo][identidad del lote][campos propios del worker].

La identidad (origen, id_entrada, indice) permite reconstruir la deduplicación al arrancar.
Cada worker define sus tipos y sus campos propios.
"""

from collections import namedtuple

from src.common.protocol.dto import EntregaActual, IdentidadProceso
from src.common.protocol.primitivas import (
    BYTES_U8, BYTES_U32, EscritorBinario, LectorBinario,
)

MAX_BYTES_REGISTRO = 1 << 28

IdentidadLote = namedtuple('IdentidadLote', 'origen id_entrada indice')


def codificar(tipo, entrega, escribir_campos=None):
    """Arma un registro; escribir_campos(escritor) agrega los campos propios del worker."""
    escritor = EscritorBinario(MAX_BYTES_REGISTRO)
    escritor.entero(tipo, BYTES_U8)
    escritor.texto(entrega.origen.grupo)
    escritor.entero(entrega.origen.instancia, BYTES_U32)
    escritor.texto(entrega.id_entrada)
    escritor.entero(entrega.indice, BYTES_U32)
    if escribir_campos is not None:
        escribir_campos(escritor)
    return escritor.resultado()


def decodificar(datos):
    """
    Devuelve (tipo, identidad, lector); el lector queda en los campos propios.
    Quien los lee tiene q terminar con lector.finalizar().
    """
    lector = LectorBinario(datos)
    tipo = lector.entero(BYTES_U8)
    grupo = lector.texto()
    instancia = lector.entero(BYTES_U32)
    id_entrada = lector.texto()
    indice = lector.entero(BYTES_U32)
    return tipo, IdentidadLote(IdentidadProceso(grupo, instancia), id_entrada, indice), lector


def entrega_de(entrada, identidad):
    """Reconstruye la entrega de un registro, para anotarla en la deduplicación."""
    return EntregaActual(entrada, identidad.origen, identidad.id_entrada, identidad.indice)
