"""Consolidación por ejecución mediante ordenación externa y fusiones de dos vías."""

from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from struct import calcsize
from sys import getsizeof
from tempfile import TemporaryDirectory

from src.common.protocol.primitivas import BYTES_U64, validar_entero
from src.common.protocol.registros import ConteoPalabra
from .archivos import (
    BUFFER_ARCHIVO, agrupar_conteos, escribir_conteos, fusionar_tramos, leer_conteos,
)


# Reserva conservadora para referencias de la lista y espacio auxiliar de sort.
BYTES_REFERENCIAS_POR_APORTE = 3 * calcsize('P')
BYTES_LISTA_VACIA = getsizeof([])


@dataclass
class _Ejecucion:
    niveles: dict[int, Path] = field(default_factory=dict)
    finalizada: bool = False


class ConsolidadorFrecuencias:
    """Suma parciales sin conservar el vocabulario completo en memoria.

    presupuesto_memoria_bytes acota la estimación del buffer de aportes de todas
    las ejecuciones juntas, incluidos objetos, referencias y ordenación. Además
    se usan hasta tres buffers de archivo y un número constante de registros al
    fusionar. Los metadatos ocupan un camino por nivel y ejecución activa.
    No representa un límite del RSS del proceso. Cada instancia se usa de forma
    secuencial; un error de almacenamiento exige cerrarla, sin reintentos internos.
    """

    def __init__(self, presupuesto_memoria_bytes: int, directorio_temporal=None,
                 *, comprobar_detencion=None):
        if (type(presupuesto_memoria_bytes) is not int
                or presupuesto_memoria_bytes <= BYTES_LISTA_VACIA):
            raise ValueError('El presupuesto debe permitir al menos el buffer vacío')
        if comprobar_detencion is not None and not callable(comprobar_detencion):
            raise TypeError("La comprobación de detención debe ser invocable")
        self._comprobar_detencion = comprobar_detencion
        self._presupuesto = presupuesto_memoria_bytes
        self._temporal = TemporaryDirectory(prefix='q4-conteos-', dir=directorio_temporal)
        self._directorio = Path(self._temporal.name)
        self._ejecuciones: dict[str, _Ejecucion] = {}
        self._buffer: list[tuple[str, str, int]] = []
        self._bytes_buffer = BYTES_LISTA_VACIA
        self._secuencia = 0
        self._cerrado = False
        self._fallido = False
        self._leyendo = False

    def acumular(self, id_ejecucion: str, parcial: ConteoPalabra) -> None:
        """Admite un parcial y vuelca el buffer antes de superar su presupuesto."""
        self._validar_disponible()
        self._validar_ejecucion(id_ejecucion)
        if type(parcial) is not ConteoPalabra:
            raise TypeError('Se esperaba un DTO ConteoPalabra')
        if type(parcial.palabra) is not str or not parcial.palabra.strip():
            raise ValueError('La palabra debe ser texto no vacío')
        validar_entero(parcial.cantidad, BYTES_U64)
        if parcial.cantidad == 0:
            raise ValueError('La cantidad del parcial debe ser positiva')
        estado = self._ejecuciones.get(id_ejecucion)
        if estado is not None and estado.finalizada:
            raise ValueError('La ejecución ya finalizó su entrada')
        if self._comprobar_detencion is not None:
            self._comprobar_detencion()
        aporte = (id_ejecucion, parcial.palabra, parcial.cantidad)
        costo = (getsizeof(aporte) + sum(getsizeof(valor) for valor in aporte)
                 + BYTES_REFERENCIAS_POR_APORTE + 4 * len(parcial.palabra))
        if BYTES_LISTA_VACIA + costo > self._presupuesto:
            raise ValueError('Un aporte individual supera el presupuesto de memoria')
        # Valida UTF-8 antes de admitir el aporte; el costo reserva su codificación.
        parcial.palabra.encode('utf-8')
        if self._bytes_buffer + costo > self._presupuesto:
            self._volcar()
        self._ejecuciones.setdefault(id_ejecucion, _Ejecucion())
        self._buffer.append(aporte)
        self._bytes_buffer += costo

    def finalizar_entrada(self, id_ejecucion: str) -> None:
        """Consolida en disco una ejecución y sella su entrada, incluso si estaba vacía."""
        self._validar_disponible()
        self._validar_ejecucion(id_ejecucion)
        estado = self._ejecuciones.setdefault(id_ejecucion, _Ejecucion())
        if estado.finalizada:
            return
        self._volcar()
        try:
            while len(estado.niveles) > 1:
                niveles = sorted(estado.niveles)[:2]
                primero, segundo = (estado.niveles[nivel] for nivel in niveles)
                destino = self._fusionar(primero, segundo)
                del estado.niveles[niveles[0]]
                estado.niveles[niveles[1]] = destino
            estado.finalizada = True
        except BaseException:
            self._fallido = True
            raise

    @contextmanager
    def iterar_conteos(self, id_ejecucion: str):
        """Abre un recorrido incremental del resultado; el contexto cierra su archivo.

        Requiere finalizar la entrada. Mientras el contexto esté abierto no se
        admiten mutaciones ni otros recorridos en esta instancia.
        """
        self._validar_disponible()
        self._validar_ejecucion(id_ejecucion)
        estado = self._ejecuciones.get(id_ejecucion)
        if estado is None or not estado.finalizada:
            raise ValueError('Se debe finalizar la entrada antes de recorrer sus conteos')
        self._leyendo = True
        try:
            if not estado.niveles:
                yield iter(())
            else:
                ruta = next(iter(estado.niveles.values()))
                with ruta.open('rb', buffering=BUFFER_ARCHIVO) as archivo:
                    yield leer_conteos(archivo, self._presupuesto, self._comprobar_detencion)
        finally:
            self._leyendo = False

    def liberar_ejecucion(self, id_ejecucion: str) -> None:
        """Elimina los archivos de una entrada finalizada sin afectar otras ejecuciones."""
        self._validar_disponible()
        self._validar_ejecucion(id_ejecucion)
        estado = self._ejecuciones.get(id_ejecucion)
        if estado is None:
            return
        if not estado.finalizada:
            raise ValueError('No se puede liberar una entrada sin finalizar')
        try:
            for ruta in estado.niveles.values():
                ruta.unlink()
            del self._ejecuciones[id_ejecucion]
        except BaseException:
            self._fallido = True
            raise

    def cerrar(self) -> None:
        """Cierra el almacenamiento y elimina el directorio temporal propio."""
        if self._leyendo:
            raise RuntimeError('No se puede cerrar durante un recorrido de conteos')
        self._cerrado = True
        self._temporal.cleanup()
        self._buffer.clear()
        self._ejecuciones.clear()
        self._bytes_buffer = BYTES_LISTA_VACIA

    def _volcar(self):
        if not self._buffer:
            return
        try:
            self._buffer.sort()
            for ejecucion, aportes in groupby(self._buffer, key=lambda aporte: aporte[0]):
                ruta = self._nueva_ruta()
                registros = (ConteoPalabra(palabra, cantidad) for _, palabra, cantidad in aportes)
                escribir_conteos(ruta, agrupar_conteos(registros), self._comprobar_detencion)
                self._incorporar_tramo(self._ejecuciones[ejecucion], ruta)
            self._buffer.clear()
            self._bytes_buffer = BYTES_LISTA_VACIA
        except BaseException:
            self._fallido = True
            raise

    def _incorporar_tramo(self, estado, ruta):
        # Cada nivel contiene hasta un tramo. Se fusionan solo pares del mismo
        # nivel para evitar reescribir todo el historial con cada nuevo volcado.
        nivel = 0
        while nivel in estado.niveles:
            ruta = self._fusionar(estado.niveles[nivel], ruta)
            del estado.niveles[nivel]
            nivel += 1
        estado.niveles[nivel] = ruta

    def _fusionar(self, primero, segundo):
        destino = self._nueva_ruta()
        fusionar_tramos(primero, segundo, destino, self._presupuesto, self._comprobar_detencion)
        primero.unlink()
        segundo.unlink()
        return destino

    def _nueva_ruta(self):
        self._secuencia += 1
        return self._directorio / f'tramo-{self._secuencia}.bin'

    def _validar_disponible(self):
        if self._cerrado or self._fallido:
            raise RuntimeError('El consolidador está cerrado o tuvo un error de almacenamiento')
        if self._leyendo:
            raise RuntimeError('Hay un recorrido de conteos abierto')

    @staticmethod
    def _validar_ejecucion(id_ejecucion):
        if type(id_ejecucion) is not str or not id_ejecucion.strip():
            raise ValueError('La ejecución debe tener un identificador no vacío')

    def __enter__(self):
        self._validar_disponible()
        return self

    def __exit__(self, tipo_error, error, traza):
        self.cerrar()
        return False
