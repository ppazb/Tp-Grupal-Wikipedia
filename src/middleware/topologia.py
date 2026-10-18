"""Recursos y convenciones de RabbitMQ derivados de los grupos y sus rutas."""

from dataclasses import dataclass
from typing import Optional

import pika

from .broker_connection import ConexionBroker
from .configuracion import ConfiguracionComunicacion, ErrorConfiguracion
from .exchange_handler import declarar_exchange
from .interfaz import ErrorConexion, ErrorTopologia
from .queue_handler import declarar_cola, enlazar_cola


PREFIJO_RECURSOS = "wikipedia"
MAX_BYTES_NOMBRE_AMQP = 255


@dataclass(frozen=True)
class DestinoPublicacion:
    """Dirección física que usará el publicador interno del middleware."""

    exchange: str
    clave: str


@dataclass(frozen=True)
class ColasRecepcion:
    """Colas que consumirá una instancia, con datos y control separados."""

    reparto: str
    afinidad: str
    control: str


class TopologiaGrupos:
    """Planifica y declara la topología completa antes de empezar a publicar.

    Recibe configuración validada por cargar_configuracion. Los nombres físicos
    y claves solo se consultan desde los módulos de transporte, nunca del negocio.
    Los recursos pertenecen al despliegue y se comparten entre ejecuciones.

    Las colas de control pueden recibir EOF mientras aún se procesan datos en
    otras colas. Distribuir esos controles no implementa la barrera de fin.
    """

    def __init__(self, configuracion: ConfiguracionComunicacion):
        self._configuracion = configuracion
        self._exchanges = set()
        self._colas = set()
        self._enlaces = set()
        # Se validan todos los nombres antes de efectuar operaciones de red.
        for ruta in configuracion.rutas:
            self._agregar_ruta(ruta)

    def declarar(self, conexion: ConexionBroker) -> None:
        """Declara exchanges, luego colas y por último enlaces, sin purgar datos.

        Puede repetirse con los mismos parámetros desde distintos procesos. Una
        incompatibilidad con recursos existentes se informa como ErrorTopologia.
        El dueño de la conexión debe cerrarla, por ejemplo mediante with, también
        si falla una declaración. No se borran recursos compartidos como rollback.
        Cambiar la topología requiere gestionar los recursos anteriores con el
        sistema detenido: este método no elimina colas ni enlaces de otro diseño.
        """
        canal = conexion.canal
        recurso = "topología"
        try:
            for nombre in sorted(self._exchanges):
                recurso = f"exchange {nombre}"
                declarar_exchange(canal, nombre)
            for nombre in sorted(self._colas):
                recurso = f"cola {nombre}"
                declarar_cola(
                    canal, nombre, self._configuracion.limites.max_mensajes_por_cola,
                )
            for cola, exchange, clave in sorted(self._enlaces):
                recurso = f"enlace de {exchange} a {cola}"
                enlazar_cola(canal, cola, exchange, clave)
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion(f"Se perdió la conexión al declarar {recurso}") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorTopologia(f"No se pudo declarar {recurso}") from error

    def destino_datos(
        self, grupo: str, entrada: str, instancia: Optional[int] = None,
    ) -> DestinoPublicacion:
        """Resuelve una salida del productor local para el futuro publicador.

        Sin instancia apunta a reparto; con instancia apunta a afinidad. La
        elección mediante hash queda a cargo del middleware, no de los workers.
        """
        ruta = self._buscar_ruta(grupo, entrada)
        if instancia is not None:
            cantidad = self._configuracion.obtener_grupo(grupo).instancias
            if type(instancia) is not int or not 0 <= instancia < cantidad:
                raise ErrorConfiguracion("La instancia destinataria está fuera del grupo")
        return _destino_datos(ruta, instancia)

    def destino_control(self, grupo: str, entrada: str) -> DestinoPublicacion:
        """Resuelve una ruta enlazada a todas las colas de control del destino."""
        return _destino_control(self._buscar_ruta(grupo, entrada))

    def colas_recepcion(self) -> Optional[ColasRecepcion]:
        """Devuelve las colas locales; una fuente sin rutas entrantes no tiene colas."""
        proceso = self._configuracion.proceso
        if not any(ruta.destino == proceso.grupo for ruta in self._configuracion.rutas):
            return None
        return _colas_instancia(proceso.grupo, proceso.instancia)

    def _buscar_ruta(self, grupo, entrada):
        origen = self._configuracion.proceso.grupo
        for ruta in self._configuracion.rutas:
            if (ruta.origen, ruta.destino, ruta.entrada) == (origen, grupo, entrada):
                return ruta
        raise ErrorConfiguracion("No existe esa ruta de salida para el grupo local")

    def _agregar_ruta(self, ruta):
        """Acumula los recursos de la ruta, reutilizando los comunes a su grupo."""
        reparto = _destino_datos(ruta)
        control = _destino_control(ruta)
        self._exchanges.update((reparto.exchange, control.exchange))
        grupo = self._configuracion.obtener_grupo(ruta.destino)
        for instancia in range(grupo.instancias):
            colas = _colas_instancia(grupo.nombre, instancia)
            afinidad = _destino_datos(ruta, instancia)
            self._colas.update((colas.reparto, colas.afinidad, colas.control))
            # La cola de reparto es única por grupo. Cada control, en cambio,
            # alcanza a todas las instancias, incluso si no recibieron datos.
            self._enlaces.update((
                (colas.reparto, reparto.exchange, reparto.clave),
                (colas.afinidad, afinidad.exchange, afinidad.clave),
                (colas.control, control.exchange, control.clave),
            ))


def _nombre(*partes):
    """Centraliza el prefijo, separador y límite de nombres físicos y claves."""
    nombre = ".".join((PREFIJO_RECURSOS,) + tuple(str(parte) for parte in partes))
    if len(nombre.encode("utf-8")) > MAX_BYTES_NOMBRE_AMQP:
        raise ErrorConfiguracion("Un nombre de recurso o clave excede el límite AMQP")
    return nombre


def _colas_instancia(grupo, instancia):
    return ColasRecepcion(
        reparto=_nombre("cola", grupo, "reparto"),
        afinidad=_nombre("cola", grupo, "afinidad", instancia),
        control=_nombre("cola", grupo, "control", instancia),
    )


def _destino_datos(ruta, instancia=None):
    modo = ("reparto",) if instancia is None else ("afinidad", instancia)
    return DestinoPublicacion(
        exchange=_nombre("exchange", ruta.origen, "datos"),
        clave=_nombre(ruta.destino, ruta.entrada, *modo),
    )


def _destino_control(ruta):
    return DestinoPublicacion(
        exchange=_nombre("exchange", ruta.origen, "control"),
        clave=_nombre(ruta.destino, ruta.entrada),
    )
