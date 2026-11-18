"""Contrato y secuencia de procesamiento respaldados por una operación durable."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from uuid import UUID

from src.common.protocol.dto import IdentidadProceso
from src.middleware.interfaz import ErrorPersistencia


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
        """Persiste cambios y salidas del plan sin ejecutarlos y devuelve su referencia.

        Repetir por ID de entrada recupera la misma operación, sus IDs de salida y
        el productor original, incluso desde otra réplica."""

    @abstractmethod
    def aplicar(self, contexto, operacion: OperacionProcesamiento) -> None:
        """Aplica durablemente los cambios del plan sin repetir efectos ya aplicados.

        Recupera también efectos de otra réplica o de entradas ya confirmadas."""

    @abstractmethod
    def publicar_pendientes(self, contexto, operacion: OperacionProcesamiento) -> None:
        """Completa las salidas del plan mediante el middleware y espera su confirmación durable.

        Los reintentos conservan contenido, destino, ID y productor original."""

    @abstractmethod
    def confirmar(self, contexto, operacion: OperacionProcesamiento) -> ConfirmacionProcesamiento:
        """Persiste y devuelve la confirmación de cambios recuperables y salidas confirmadas.

        Repetir la operación devuelve la misma confirmación."""


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
