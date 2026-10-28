"""Selección incremental y determinística de las palabras más frecuentes de Q4."""

from collections.abc import Iterable
from heapq import nsmallest

from src.common.protocol.primitivas import BYTES_U64, validar_entero
from src.common.protocol.registros import ConteoPalabra


CANTIDAD_PALABRAS_TOP = 20


def seleccionar_top(totales: Iterable[ConteoPalabra]) -> tuple[ConteoPalabra, ...]:
    """Devuelve hasta veinte totales por frecuencia descendente y palabra ascendente.

    Requiere un total completo por palabra de una única ejecución. El desempate
    usa el orden lexicográfico de Python por puntos de código Unicode, sin cambiar
    mayúsculas, tildes ni normalización. No depende del orden de llegada.
    Recorre la entrada una vez conservando hasta veinte candidatos. Una entrada
    vacía devuelve una tupla vacía; una menor a veinte conserva todos sus pares.
    """
    return tuple(nsmallest(CANTIDAD_PALABRAS_TOP, totales, key=_prioridad))


def _prioridad(total: ConteoPalabra) -> tuple[int, str]:
    """Valida el total y prioriza mayor cantidad, seguida de menor palabra."""
    if type(total) is not ConteoPalabra:
        raise TypeError('Se esperaba un DTO ConteoPalabra')
    if type(total.palabra) is not str or not total.palabra.strip():
        raise ValueError('La palabra debe ser texto no vacío')
    validar_entero(total.cantidad, BYTES_U64)
    if total.cantidad == 0:
        raise ValueError('El total de una palabra debe ser positivo')
    return -total.cantidad, total.palabra
