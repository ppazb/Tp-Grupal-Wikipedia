"""Estructuras de configuración independientes de RabbitMQ y Pika."""

from dataclasses import dataclass, field
from typing import Tuple

from ..common.protocol.dto import IdentidadProceso
from .interfaz import ErrorMiddleware


class ErrorConfiguracion(ErrorMiddleware):
    """No se pudo leer la configuración o su contenido es inconsistente."""


@dataclass(frozen=True)
class ConfiguracionBroker:
    """Conexión al broker; todos los tiempos se expresan en segundos."""

    host: str
    puerto: int
    usuario: str
    contrasena: str = field(repr=False)
    virtual_host: str
    latido_segundos: int
    timeout_conexion_segundos: int
    timeout_bloqueo_segundos: int


@dataclass(frozen=True)
class LimitesComunicacion:
    """Límites para el consumidor, cada cola y cada mensaje serializado."""

    mensajes_sin_confirmar: int
    max_mensajes_por_cola: int
    max_bytes_por_mensaje: int


@dataclass(frozen=True)
class ConfiguracionGrupo:
    """Grupo lógico y cantidad de réplicas, estable durante la ejecución."""

    nombre: str
    instancias: int


@dataclass(frozen=True)
class Ruta:
    """Aporte de un grupo productor a una entrada lógica de otro grupo."""

    nombre: str
    origen: str
    destino: str
    entrada: str


@dataclass(frozen=True)
class ConfiguracionComunicacion:
    """Instantánea inmutable que el cargador entrega después de validarla.

    Puede transferirse a procesos hijos: solo contiene datos, no conexiones.
    Las tuplas evitan compartir colecciones mutables y admiten serialización
    para iniciar procesos mediante multiprocessing.
    """

    broker: ConfiguracionBroker
    proceso: IdentidadProceso
    limites: LimitesComunicacion
    grupos: Tuple[ConfiguracionGrupo, ...]
    rutas: Tuple[Ruta, ...]

    def obtener_grupo(self, nombre: str) -> ConfiguracionGrupo:
        """Busca un grupo declarado; un nombre desconocido es un error."""
        for grupo in self.grupos:
            if grupo.nombre == nombre:
                return grupo
        raise ErrorConfiguracion("No existe el grupo solicitado en la configuración")

    def participantes_entrada(self, grupo: str, entrada: str) -> int:
        """Cuenta productores físicos, sin confundirlos con réplicas receptoras.

        No implica que un EOF de cada productor baste para cerrar una entrada:
        el coordinador deberá garantizar además el procesamiento de los datos.
        """
        self.obtener_grupo(grupo)
        productores = {
            ruta.origen for ruta in self.rutas
            if ruta.destino == grupo and ruta.entrada == entrada
        }
        if not productores:
            raise ErrorConfiguracion("La entrada solicitada no tiene rutas productoras")
        return sum(self.obtener_grupo(origen).instancias for origen in productores)
