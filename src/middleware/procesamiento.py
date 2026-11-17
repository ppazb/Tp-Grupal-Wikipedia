"""Contrato y secuencia de procesamiento respaldados por una operación durable."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from uuid import UUID

from ..common.protocol.dto import IdentidadProceso
from .interfaz import ErrorPersistencia


@dataclass(frozen=True)
class OperacionProcesamiento:
    """Referencia a los cambios y salidas persistidos por un adaptador de negocio."""

    id_operacion: UUID
    responsable: IdentidadProceso


@dataclass(frozen=True)
class ConfirmacionProcesamiento:
    """Confirmación durable de los efectos y salidas de una entrada identificada."""

    id_mensaje: UUID
    operacion: OperacionProcesamiento


class ProcesadorRecuperable(ABC):
    """Adapta el almacenamiento del negocio al ciclo de recepción.

    El adaptador conserva un plan por ID de entrada con cambios, salidas e IDs
    originales. Su almacenamiento debe poder consultarse desde otra réplica.
    No se admiten cambios de negocio ni publicaciones antes de preparar el plan.
    Los métodos deben propagar sus errores y tolerar reintentos y reinicios.
    """

    @abstractmethod
    def preparar(self, contexto, datos) -> OperacionProcesamiento:
        """Persiste el plan completo y devuelve su referencia después de sincronizarlo.

        Repetir la llamada para contexto.id_mensaje recupera la misma operación,
        incluso si la primera escritura terminó sin devolver su resultado.
        El plan incluye los cambios recuperables y las publicaciones con sus
        contenidos, destinos, IDs y productor originales. No ejecuta sus efectos.
        La referencia conserva al responsable original, también si otra réplica
        encuentra un plan persistido antes de que se vinculara a la recepción.
        """

    @abstractmethod
    def aplicar(self, contexto, operacion: OperacionProcesamiento) -> None:
        """Aplica durablemente los cambios del plan sin repetir efectos ya aplicados.

        Debe poder recuperar lo que ejecutó otra réplica y restaurar el estado
        aunque el mensaje de entrada ya haya sido confirmado y no se reentregue.
        """

    @abstractmethod
    def publicar_pendientes(self, contexto, operacion: OperacionProcesamiento) -> None:
        """Completa las salidas del plan conservando su identidad y productor.

        Usa el middleware, sin exponer conexiones al negocio. Al retornar todas
        las salidas tienen confirmación durable; un reintento recupera las mismas
        publicaciones, no genera nuevos IDs. Si ejecuta otra réplica debe resolver
        el publicador original, sin cambiar el origen del sobre al publicador local.
        """

    @abstractmethod
    def confirmar(self, contexto, operacion: OperacionProcesamiento) -> ConfirmacionProcesamiento:
        """Persiste el cierre de la operación antes de devolver su confirmación.

        Solo confirma si los cambios son recuperables y las salidas están
        confirmadas durablemente. Repetir devuelve la misma confirmación.
        """


def ejecutar_procesamiento(procesador, datos, recepciones, comprobar_detencion):
    """Recupera o prepara una operación y exige su confirmación antes del ACK."""
    contexto = recepciones.actual
    operacion = recepciones.operacion_actual
    comprobar_detencion()
    if operacion is None:
        preparada = procesador.preparar(contexto, datos)
        operacion = recepciones.vincular_operacion(preparada)
    for paso in (procesador.aplicar, procesador.publicar_pendientes):
        comprobar_detencion()
        paso(contexto, operacion)
    comprobar_detencion()
    confirmacion = procesador.confirmar(contexto, operacion)
    esperada = ConfirmacionProcesamiento(contexto.id_mensaje, operacion)
    if type(confirmacion) is not ConfirmacionProcesamiento or confirmacion != esperada:
        raise ErrorPersistencia('El procesador no confirmó durablemente la operación de esta entrega')
    comprobar_detencion()
