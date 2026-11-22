"""Registro binario de cambios locales del seguimiento de EOF."""

from enum import IntEnum
from pathlib import Path

from src.common.io_adapters.wal import WAL
from src.common.protocol.dto import Entrada, IdentidadProceso
from src.common.protocol.primitivas import EscritorBinario, LectorBinario, BYTES_U8, BYTES_U32, BYTES_U64
from .interfaz import ErrorSeguimientoEntrada


MAX_BYTES_REGISTRO = 16_000_000


class CambioEOF(IntEnum):
    CONFIGURACION = 0
    LOTE_RESUELTO = 1
    CIERRE_PRODUCTOR = 2
    PREPARACION_ENVIADA = 3
    PREPARADA = 4
    AVANCE_ENVIADO = 5
    AVANCE_RECIBIDO = 6
    FIN_ENVIADO = 7
    FIN_RECIBIDO = 8
    FIN_ENTREGADO = 9
    LIBERAR_EJECUCION = 10


CON_PRODUCTOR = frozenset((CambioEOF.LOTE_RESUELTO, CambioEOF.CIERRE_PRODUCTOR))
CON_VALOR = frozenset((CambioEOF.CIERRE_PRODUCTOR, CambioEOF.PREPARACION_ENVIADA,
                      CambioEOF.PREPARADA, CambioEOF.AVANCE_ENVIADO, CambioEOF.AVANCE_RECIBIDO))


def codificar_configuracion(configuracion):
    """Describe la identidad, las réplicas locales y los productores de cada entrada."""
    escritor = EscritorBinario(MAX_BYTES_REGISTRO)
    escritor.entero(CambioEOF.CONFIGURACION.value, BYTES_U8)
    escritor.texto(configuracion.proceso.grupo)
    escritor.entero(configuracion.proceso.instancia, BYTES_U32)
    escritor.entero(configuracion.obtener_grupo(configuracion.proceso.grupo).instancias, BYTES_U32)
    entradas = sorted({ruta.entrada for ruta in configuracion.rutas
                       if ruta.destino == configuracion.proceso.grupo})
    escritor.entero(len(entradas), BYTES_U32)
    for entrada in entradas:
        escritor.texto(entrada)
        productores = sorted(configuracion.productores_entrada(configuracion.proceso.grupo, entrada),
                             key=lambda productor: (productor.grupo, productor.instancia))
        escritor.entero(len(productores), BYTES_U32)
        for productor in productores:
            escritor.texto(productor.grupo)
            escritor.entero(productor.instancia, BYTES_U32)
    return escritor.resultado()


class RegistroEOF:
    """Persiste transiciones y entrega los registros completos para reconstruir la barrera."""

    def __init__(self, directorio, configuracion):
        self._wal = WAL(Path(directorio) / 'coordinacion.wal')
        try:
            esperada = codificar_configuracion(configuracion)
            registros = self._wal.iterar()
            try:
                guardada = next(registros, None)
            finally:
                registros.close()
            if guardada is None:
                self._wal.agregar(esperada)
            elif guardada != esperada:
                raise ErrorSeguimientoEntrada('La configuración de EOF no coincide con el estado persistido')
        except BaseException:
            self._wal.cerrar()
            raise

    def registrar(self, cambio, entrada, *, productor=None, valor=0, instancia=0):
        """Escribe y sincroniza una transición antes de devolver el control."""
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(cambio.value, BYTES_U8)
        escritor.texto(entrada.id_ejecucion)
        escritor.texto(entrada.nombre)
        if cambio in CON_PRODUCTOR:
            escritor.texto(productor.grupo)
            escritor.entero(productor.instancia, BYTES_U32)
        if cambio is CambioEOF.AVANCE_RECIBIDO:
            escritor.entero(instancia, BYTES_U32)
        if cambio in CON_VALOR:
            escritor.entero(valor, BYTES_U64)
        self._wal.agregar(escritor.resultado())

    def recuperar(self):
        """Lee las transiciones en orden sin cargar el archivo completo en memoria."""
        registros = self._wal.iterar()
        try:
            next(registros)  # La cabecera fue validada al abrir el registro.
            for datos in registros:
                lector = LectorBinario(datos)
                cambio = CambioEOF(lector.entero(BYTES_U8))
                if cambio is CambioEOF.CONFIGURACION:
                    raise ErrorSeguimientoEntrada('Cabecera de EOF repetida dentro del WAL')
                entrada = Entrada(lector.texto(), lector.texto())
                productor = None
                if cambio in CON_PRODUCTOR:
                    productor = IdentidadProceso(lector.texto(), lector.entero(BYTES_U32))
                instancia = lector.entero(BYTES_U32) if cambio is CambioEOF.AVANCE_RECIBIDO else 0
                valor = lector.entero(BYTES_U64) if cambio in CON_VALOR else 0
                lector.finalizar()
                yield cambio, entrada, productor, valor, instancia
        finally:
            registros.close()

    def cerrar_registro(self):
        """Cierra el archivo conservando las transiciones para el siguiente arranque."""
        self._wal.cerrar()
