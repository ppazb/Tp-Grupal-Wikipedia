"""Registro local de limpiezas para recursos exclusivos de cada ejecución."""

import logging
from typing import Callable

from .interfaz import ErrorRecursosEjecucion


REGISTRO = logging.getLogger(__name__)


class RecursosPorEjecucion:
    """Registra y libera recursos exclusivos por ejecución.

    Las limpiezas deben ser idempotentes y sus nombres únicos por ejecución.
    El registro se usa secuencialmente; las limpiezas no pueden modificarlo.
    Los recursos compartidos quedan fuera de este registro.
    """

    def __init__(self):
        self._recursos = {}
        self._liberando = False

    def registrar(
        self, id_ejecucion: str, nombre: str, liberar: Callable[[], None],
    ) -> None:
        """Registra una limpieza identificada sin reemplazar recursos existentes."""
        self._validar_identificador(id_ejecucion)
        self._validar_identificador(nombre)
        self._validar_disponible()
        if not callable(liberar):
            raise ErrorRecursosEjecucion("La operación de limpieza debe ser invocable")
        recursos = self._recursos.get(id_ejecucion)
        if recursos is not None and nombre in recursos:
            raise ErrorRecursosEjecucion("El recurso ya está registrado en esa ejecución")
        self._recursos.setdefault(id_ejecucion, {})[nombre] = liberar

    def liberar_ejecucion(self, id_ejecucion: str) -> None:
        """Ejecuta las limpiezas de la ejecución y conserva las que fallan para reintentarlas."""
        self._validar_identificador(id_ejecucion)
        self._validar_disponible()
        recursos = self._recursos.get(id_ejecucion)
        if recursos is None:
            return
        self._liberando = True
        primer_error = None
        try:
            for nombre in tuple(recursos):
                try:
                    recursos[nombre]()
                except Exception as error:
                    REGISTRO.exception(
                        "No se pudo liberar el recurso %r de la ejecución %r",
                        nombre, id_ejecucion,
                    )
                    if primer_error is None:
                        primer_error = error
                else:
                    del recursos[nombre]
        finally:
            self._liberando = False
            if not recursos:
                del self._recursos[id_ejecucion]
        if primer_error is not None:
            raise ErrorRecursosEjecucion(
                "Quedaron recursos de la ejecución sin liberar; ver registro"
            ) from primer_error

    def _validar_disponible(self):
        """Rechaza modificaciones mientras se ejecutan limpiezas."""
        if self._liberando:
            raise ErrorRecursosEjecucion("No se puede modificar el registro durante una limpieza")

    def liberar_todos_los_recursos(self):
        """Intenta liberar todas las ejecuciones y conserva las limpiezas fallidas."""
        self._validar_disponible()
        errores = []
        for id_ejecucion in tuple(self._recursos):
            try:
                self.liberar_ejecucion(id_ejecucion)
            except Exception as error:
                errores.append(error)
        if errores:
            raise ErrorRecursosEjecucion("No se pudieron liberar todas las ejecuciones") from errores[0]

    @staticmethod
    def _validar_identificador(valor):
        """Exige un nombre no vacío para una ejecución o recurso."""
        if type(valor) is not str or not valor.strip():
            raise ErrorRecursosEjecucion("La ejecución y el recurso deben tener nombres no vacíos")
