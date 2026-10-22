"""Estructuras compartidas por los procesos y el protocolo de comunicación."""

from dataclasses import dataclass
from enum import Enum
from typing import Generic, Optional, TypeVar, Union


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

    grupo: str
    datos: Datos[Contenido]
    clave_afinidad: Optional[str] = None


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
    instancia: int


class TipoMensaje(str, Enum):
    """Tipos admitidos en el protocolo interno entre procesos."""

    DATOS = "datos"
    EOF = "eof"


@dataclass(frozen=True)
class SobreMensaje:
    """Identifica la ejecución, entrada, tipo, origen y contenido de un mensaje.

    DATOS transporta contenido no nulo. EOF transporta contenido nulo e indica
    que el productor terminó sus aportes para esa entrada.
    """

    entrada: Entrada
    tipo: TipoMensaje
    origen: IdentidadProceso
    contenido: object
