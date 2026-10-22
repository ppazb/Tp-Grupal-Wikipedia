"""Registro local de limpiezas para recursos exclusivos de cada ejecución."""

import logging
from typing import Callable

from .interfaz import ErrorRecursosEjecucion


REGISTRO = logging.getLogger(__name__)


class RecursosPorEjecucion:
    """Agrupa recursos cuya vida útil termina con una ejecución local.

    Cada componente registra una operación que libera su recurso y elimina su
    estado asociado. Debe ser idempotente: si falla después de liberar una parte,
    puede volver a invocarse. Los nombres son únicos dentro de cada ejecución.

    Uso secuencial en el proceso propietario, igual que el resto del middleware.
    Las limpiezas no pueden modificar este registro ni iniciar otra liberación.
    No registrar conexiones, canales, colas ni otros recursos compartidos.

    Actualmente el transporte no crea recursos exclusivos por ejecución. Este
    registro permite incorporar su limpieza al agregar el estado de EOF, sin
    introducir registros ficticios por cada mensaje ni conservar IDs terminados.
    El armado del middleware deberá compartir este registro entre sus componentes
    y delegar aquí liberar_ejecucion una vez completado el procesamiento local.
    """

    def __init__(self):
        self._recursos = {}
        self._liberando = False

    def registrar(
        self, id_ejecucion: str, nombre: str, liberar: Callable[[], None],
    ) -> None:
        """Registra una limpieza sin reemplazar un recurso ya registrado.

        Si el registro falla, el llamador conserva la responsabilidad de liberar
        el recurso adquirido. No se ejecuta la operación durante el registro.
        """
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
        """Intenta todas las limpiezas y elimina solo los registros exitosos.

        Una ejecución desconocida no requiere trabajo. Si falla una limpieza,
        registra su causa y continúa con las restantes; informa el primer error
        al finalizar. Una interrupción del proceso se propaga, conservando las
        limpiezas que todavía no terminaron para que no se pierdan referencias.
        """
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
        if self._liberando:
            raise ErrorRecursosEjecucion("No se puede modificar el registro durante una limpieza")

    @staticmethod
    def _validar_identificador(valor):
        if type(valor) is not str or not valor.strip():
            raise ErrorRecursosEjecucion("La ejecución y el recurso deben tener nombres no vacíos")
