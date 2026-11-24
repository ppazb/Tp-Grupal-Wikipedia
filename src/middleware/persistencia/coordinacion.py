"""Registro binario de cambios locales del cierre de entradas (EOF).

Transiciones: CONFIGURACION, LOTE_RESUELTO (productor, id_entrada, indice),
CIERRE_PRODUCTOR (productor, n), FIN_ENTREGADO, LIBERAR_EJECUCION y FOTO. Cada N registros
el WAL se reemplaza por la cabecera y una foto de tamaño fijo (contadores y ventanas).
"""

from enum import IntEnum
from pathlib import Path

from src.common.io_adapters.wal import WAL
from src.common.protocol.dto import Entrada, IdentidadProceso
from src.common.protocol.primitivas import EscritorBinario, LectorBinario, BYTES_U8, BYTES_U32, BYTES_U64
from ..eof.ventana import VentanaLotes
from ..interfaz import ErrorSeguimientoEntrada


MAX_BYTES_REGISTRO = 16_000_000  # 16 MB por registro binario; no limita el WAL completo.
REGISTROS_POR_FOTO = 10_000  # Cada tantos registros el WAL se reemplaza por cabecera + foto.


class CambioEOF(IntEnum):
    CONFIGURACION = 0
    LOTE_RESUELTO = 1
    CIERRE_PRODUCTOR = 2
    FIN_ENTREGADO = 3
    LIBERAR_EJECUCION = 4
    FOTO = 5


CON_PRODUCTOR = frozenset((CambioEOF.LOTE_RESUELTO, CambioEOF.CIERRE_PRODUCTOR))


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


def _ordenar(productores):
    return sorted(productores, key=lambda productor: (productor.grupo, productor.instancia))


def _escribir_productor(escritor, productor):
    escritor.texto(productor.grupo)
    escritor.entero(productor.instancia, BYTES_U32)


def _leer_productor(lector):
    return IdentidadProceso(lector.texto(), lector.entero(BYTES_U32))


class RegistroEOF:
    """Persiste transiciones y entrega los registros completos para reconstruir el cierre local."""

    def __init__(self, directorio, configuracion, *, registros_por_foto=REGISTROS_POR_FOTO):
        self._wal = WAL(Path(directorio) / 'coordinacion.wal')
        self._k = configuracion.limites.ventana_lotes
        self._registros_por_foto = registros_por_foto
        self._registros_desde_foto = 0
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

    @property
    def requiere_foto(self):
        """True cuando se acumularon N registros desde la última foto."""
        return self._registros_desde_foto >= self._registros_por_foto

    def registrar(self, cambio, entrada, *, productor=None, valor=0, id_entrada=None, indice=0):
        """Escribe y sincroniza una transición antes de devolver el control."""
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(cambio.value, BYTES_U8)
        escritor.texto(entrada.id_ejecucion)
        escritor.texto(entrada.nombre)
        if cambio in CON_PRODUCTOR:
            _escribir_productor(escritor, productor)
        if cambio is CambioEOF.LOTE_RESUELTO:
            escritor.texto(id_entrada)
            escritor.entero(indice, BYTES_U32)
        if cambio is CambioEOF.CIERRE_PRODUCTOR:
            escritor.entero(valor, BYTES_U64)
        self._wal.agregar(escritor.resultado())
        self._registros_desde_foto += 1

    def escribir_foto(self, entradas, retiradas):
        """Reemplaza el WAL por la cabecera y una foto del estado.

        entradas: {Entrada: (resueltos, ventanas, cierres, fin_entregado)} con dicts por productor.
        retiradas: {id_ejecucion: {Entrada: cierres}} con los cierres de ejecuciones retiradas.
        """
        cabecera = self._leer_cabecera()
        self._wal.reemplazar((cabecera, self._codificar_foto(entradas, retiradas)))
        self._registros_desde_foto = 0

    def _leer_cabecera(self):
        registros = self._wal.iterar()
        try:
            return next(registros)
        finally:
            registros.close()

    @staticmethod
    def _codificar_foto(entradas, retiradas):
        escritor = EscritorBinario(MAX_BYTES_REGISTRO)
        escritor.entero(CambioEOF.FOTO.value, BYTES_U8)
        escritor.entero(len(entradas), BYTES_U32)
        for entrada, (resueltos, ventanas, cierres, fin_entregado) in sorted(
                entradas.items(), key=lambda par: (par[0].id_ejecucion, par[0].nombre)):
            escritor.texto(entrada.id_ejecucion)
            escritor.texto(entrada.nombre)
            escritor.entero(int(fin_entregado), BYTES_U8)
            escritor.entero(len(resueltos), BYTES_U32)
            for productor in _ordenar(resueltos):
                _escribir_productor(escritor, productor)
                escritor.entero(resueltos[productor], BYTES_U64)
            escritor.entero(len(ventanas), BYTES_U32)
            for productor in _ordenar(ventanas):
                _escribir_productor(escritor, productor)
                escritor.binario(ventanas[productor].serializar())
            escritor.entero(len(cierres), BYTES_U32)
            for productor in _ordenar(cierres):
                _escribir_productor(escritor, productor)
                escritor.entero(cierres[productor], BYTES_U64)
        escritor.entero(len(retiradas), BYTES_U32)
        for id_ejecucion in sorted(retiradas):
            escritor.texto(id_ejecucion)
            por_entrada = retiradas[id_ejecucion]
            escritor.entero(len(por_entrada), BYTES_U32)
            for entrada in sorted(por_entrada, key=lambda e: e.nombre):
                escritor.texto(entrada.nombre)
                cierres = por_entrada[entrada]
                escritor.entero(len(cierres), BYTES_U32)
                for productor in _ordenar(cierres):
                    _escribir_productor(escritor, productor)
                    escritor.entero(cierres[productor], BYTES_U64)
        return escritor.resultado()

    def _decodificar_foto(self, lector):
        entradas = {}
        for _ in range(lector.entero(BYTES_U32)):
            entrada = Entrada(lector.texto(), lector.texto())
            fin_entregado = bool(lector.entero(BYTES_U8))
            resueltos = {}
            for _ in range(lector.entero(BYTES_U32)):
                productor = _leer_productor(lector)
                resueltos[productor] = lector.entero(BYTES_U64)
            ventanas = {}
            for _ in range(lector.entero(BYTES_U32)):
                productor = _leer_productor(lector)
                ventanas[productor] = VentanaLotes.deserializar(lector.bloque(), self._k)
            cierres = {}
            for _ in range(lector.entero(BYTES_U32)):
                productor = _leer_productor(lector)
                cierres[productor] = lector.entero(BYTES_U64)
            entradas[entrada] = (resueltos, ventanas, cierres, fin_entregado)
        retiradas = {}
        for _ in range(lector.entero(BYTES_U32)):
            id_ejecucion = lector.texto()
            por_entrada = {}
            for _ in range(lector.entero(BYTES_U32)):
                entrada = Entrada(id_ejecucion, lector.texto())
                cierres = {}
                for _ in range(lector.entero(BYTES_U32)):
                    productor = _leer_productor(lector)
                    cierres[productor] = lector.entero(BYTES_U64)
                por_entrada[entrada] = cierres
            retiradas[id_ejecucion] = por_entrada
        lector.finalizar()
        return entradas, retiradas

    def recuperar(self):
        """Lee las transiciones en orden sin cargar el archivo completo en memoria.

        Entrega (cambio, entrada, productor, valor, id_entrada, indice) por transición y,
        para una FOTO, (CambioEOF.FOTO, entradas, retiradas).
        """
        registros = self._wal.iterar()
        try:
            next(registros)  # La cabecera fue validada al abrir el registro.
            for datos in registros:
                self._registros_desde_foto += 1
                lector = LectorBinario(datos)
                cambio = CambioEOF(lector.entero(BYTES_U8))
                if cambio is CambioEOF.CONFIGURACION:
                    raise ErrorSeguimientoEntrada('Cabecera de EOF repetida dentro del WAL')
                if cambio is CambioEOF.FOTO:
                    self._registros_desde_foto = 0
                    entradas, retiradas = self._decodificar_foto(lector)
                    yield cambio, entradas, retiradas
                    continue
                entrada = Entrada(lector.texto(), lector.texto())
                productor = _leer_productor(lector) if cambio in CON_PRODUCTOR else None
                id_entrada = lector.texto() if cambio is CambioEOF.LOTE_RESUELTO else None
                indice = lector.entero(BYTES_U32) if cambio is CambioEOF.LOTE_RESUELTO else 0
                valor = lector.entero(BYTES_U64) if cambio is CambioEOF.CIERRE_PRODUCTOR else 0
                lector.finalizar()
                yield cambio, entrada, productor, valor, id_entrada, indice
        finally:
            registros.close()

    def cerrar_registro(self):
        """Cierra el archivo conservando las transiciones para el siguiente arranque."""
        self._wal.cerrar()
