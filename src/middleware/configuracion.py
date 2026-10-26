"""Estructuras de configuración independientes de RabbitMQ y Pika."""

from dataclasses import dataclass, field
from typing import Tuple

from ..common.protocol.dto import IdentidadProceso
from ..common.protocol.registros import Esquema
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
    """Límites para recepción, colas y preparación local de publicaciones.

    Los cuerpos preparados ocupan como máximo max_envios_por_publicacion por
    max_bytes_por_mensaje, sin contar objetos del llamador ni temporales de codificación.
    mensajes_sin_confirmar limita entregas por canal receptor, no por grupo.
    max_mensajes_por_cola limita las colas de datos y sus permisos; las colas de
    control no tienen ese límite para permitir avanzar a las barreras de cierre.
    """

    mensajes_sin_confirmar: int
    max_mensajes_por_cola: int
    max_bytes_por_mensaje: int
    max_envios_por_publicacion: int


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
    esquemas: Tuple[Esquema, ...]


@dataclass(frozen=True)
class ConfiguracionComunicacion:
    """Agrupa parámetros inmutables de conexión, identidad, límites, grupos y rutas."""

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

    def obtener_ruta(self, origen: str, destino: str, entrada: str) -> Ruta:
        """Busca la ruta exacta entre dos grupos y una entrada lógica."""
        for ruta in self.rutas:
            if (ruta.origen, ruta.destino, ruta.entrada) == (origen, destino, entrada):
                return ruta
        raise ErrorConfiguracion("No existe la ruta solicitada en la configuración")

    def participantes_entrada(self, grupo: str, entrada: str) -> int:
        """Suma las instancias de los grupos productores de una entrada.

        Cuenta cada grupo una sola vez y rechaza entradas sin rutas productoras.
        """
        return len(self.productores_entrada(grupo, entrada))

    def productores_entrada(self, grupo: str, entrada: str) -> frozenset[IdentidadProceso]:
        """Identifica las instancias productoras de una entrada sin duplicarlas."""
        self.obtener_grupo(grupo)
        productores = {
            ruta.origen for ruta in self.rutas
            if ruta.destino == grupo and ruta.entrada == entrada
        }
        if not productores:
            raise ErrorConfiguracion("La entrada solicitada no tiene rutas productoras")
        return frozenset(
            IdentidadProceso(origen, instancia)
            for origen in productores
            for instancia in range(self.obtener_grupo(origen).instancias)
        )
