"""Estructuras compartidas por los procesos y el protocolo de comunicación."""

from dataclasses import dataclass
from enum import IntEnum
from typing import Generic, Optional, TypeVar, Union

from .registros import ErrorDecodificacionRegistro, Lote


Contenido = TypeVar("Contenido")


@dataclass(frozen=True)
class Entrada:
    """Dependencia lógica identificada por ejecución de cliente y nombre.

    id_ejecucion debe distinguir conexiones sucesivas del mismo cliente. nombre
    distingue entradas de una etapa, por ejemplo históricos y candidatos en Q5.
    """

    id_ejecucion: str
    nombre: str


@dataclass(frozen=True)
class Datos(Generic[Contenido]):
    """Registro o lote acotado ya decodificado para el negocio."""

    entrada: Entrada
    contenido: Contenido


@dataclass(frozen=True)
class EnvioDatos(Generic[Contenido]):
    """Describe una salida hacia un grupo y su clave opcional de afinidad.

    La clave se aplica al lote completo. Es una estructura local de publicación.
    """

    grupo: str #topic
    datos: Datos[Contenido]
    clave_afinidad: Optional[str] = None #routing key


@dataclass(frozen=True)
class FinEntrada:
    """Cierre lógico consolidado, posterior al procesamiento de todos los aportes.

    No equivale al EOF individual de un productor ni indica el fin del proceso.
    """

    entrada: Entrada


Evento = Union[Datos[Contenido], FinEntrada]


@dataclass(frozen=True)
class IdentidadProceso:
    """Instancia de un grupo de trabajo, numerada desde cero; no es un PID."""

    grupo: str
    instancia: int #client_id


class TipoMensaje(IntEnum):
    """Tipos admitidos en el protocolo interno entre procesos."""

    DATOS = 0x01
    EOF = 0x02
    PREPARAR_CIERRE = 0x03
    AVANCE_REPLICA = 0x04
    FIN_ENTRADA = 0x05


@dataclass(frozen=True)
class CierreProductor:
    """Cantidad final de lotes publicados por un productor hacia una entrada."""

    total_lotes: int


@dataclass(frozen=True)
class PrepararCierre:
    """Total anunciado por todos los productores de la entrada del grupo."""

    total_lotes: int


@dataclass(frozen=True)
class AvanceReplica:
    """Cantidad acumulada de lotes resueltos por una réplica."""

    lotes_resueltos: int


@dataclass(frozen=True)
class SobreMensaje:
    """Identifica la ejecución, entrada, tipo, origen y contenido de un mensaje.

    DATOS transporta un Lote. EOF transporta un CierreProductor e indica
    que el productor terminó sus aportes para esa entrada. La coordinación usa
    PrepararCierre, AvanceReplica y contenido nulo para FIN_ENTRADA.
    """

    entrada: Entrada
    tipo: TipoMensaje
    origen: IdentidadProceso
    contenido: Union[Lote, CierreProductor, PrepararCierre, AvanceReplica, None]


@dataclass(frozen=True)
class ResultadoDecodificacion:
    """Sobre decodificado y rechazos individuales con los índices originales."""

    sobre: SobreMensaje
    errores_por_registro: tuple[ErrorDecodificacionRegistro, ...]

    @property
    def registros_validos(self):
        """Devuelve los registros aceptados; un EOF no contiene registros."""
        return self.sobre.contenido.registros if self.sobre.tipo is TipoMensaje.DATOS else ()
