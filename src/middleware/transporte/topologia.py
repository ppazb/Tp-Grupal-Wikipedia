"""Recursos y convenciones de RabbitMQ derivados de los grupos y sus rutas."""

from dataclasses import dataclass
import hashlib
from typing import Optional

import pika

from .conexion import ConexionBroker
from ..configuracion.modelos import ConfiguracionComunicacion, ErrorConfiguracion
from .exchanges import declarar_exchange
from ..interfaz import ErrorConexion, ErrorTopologia
from .colas import declarar_cola, enlazar_cola


PREFIJO_RECURSOS = "wikipedia"
MAX_BYTES_NOMBRE_AMQP = 255  # Límite en bytes de un shortstr de AMQP.


@dataclass(frozen=True)
class DestinoPublicacion:
    """Dirección física usada por el publicador interno del middleware."""

    exchange: str
    clave: str
    cola: Optional[str] = None


@dataclass(frozen=True)
class ColasRecepcion:
    """Nombres de las colas de afinidad y control de una instancia."""

    afinidad: str
    control: str


class TopologiaGrupos:
    """Calcula y declara exchanges, colas y enlaces a partir de la configuración.

    Los recursos se comparten entre ejecuciones. Las colas de datos y control
    son independientes y no establecen un orden de recepción entre ellas.
    """

    def __init__(self, configuracion: ConfiguracionComunicacion):
        self._configuracion = configuracion
        self._exchanges = set()
        self._colas = set()
        self._enlaces = set()
        self._colas_datos = set()
        # Se validan todos los nombres antes de efectuar operaciones de red.
        for ruta in configuracion.rutas:
            self._agregar_ruta(ruta)

    def declarar(self, conexion: ConexionBroker) -> None:
        """Declara los recursos compartidos de la topología e informa errores de conexión o declaración."""
        canal = conexion.canal
        recurso = "topología"
        try:
            for nombre in sorted(self._exchanges):
                recurso = f"exchange {nombre}"
                declarar_exchange(canal, nombre)
            for nombre in sorted(self._colas):
                recurso = f"cola {nombre}"
                declarar_cola(
                    canal, nombre,
                    self._configuracion.limites.max_mensajes_por_cola
                    if nombre in self._colas_datos else None,
                )
            for cola, exchange, clave in sorted(self._enlaces):
                recurso = f"enlace de {exchange} a {cola}"
                enlazar_cola(canal, cola, exchange, clave)
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion(f"Se perdió la conexión al declarar {recurso}") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorTopologia(f"No se pudo declarar {recurso}") from error

    def destino_datos(self, grupo: str, entrada: str, instancia: int) -> DestinoPublicacion:
        """Valida la instancia destinataria y resuelve su cola de afinidad."""
        if instancia is None:
            raise ErrorConfiguracion("Los datos requieren una instancia destinataria")
        ruta = self._buscar_ruta_de_salida(grupo, entrada)
        self._validar_instancia_destinataria(grupo, instancia)
        return _construir_destino_datos(ruta, instancia)

    def destino_control(self, grupo: str, entrada: str, instancia=None) -> DestinoPublicacion:
        """Resuelve control hacia todo el grupo o hacia una única instancia."""
        ruta = self._buscar_ruta_de_salida(grupo, entrada)
        self._validar_instancia_destinataria(grupo, instancia)
        return _construir_destino_control(ruta, instancia)

    def colas_recepcion(self) -> Optional[ColasRecepcion]:
        """Devuelve las colas locales; una fuente sin rutas entrantes no tiene colas."""
        proceso = self._configuracion.proceso
        if not any(ruta.destino == proceso.grupo for ruta in self._configuracion.rutas):
            return None
        return _construir_colas_instancia(proceso.grupo, proceso.instancia)

    def colas_datos(self):
        """Devuelve las colas de datos del despliegue, sin duplicar las compartidas."""
        return tuple(sorted(self._colas_datos))

    def cola_capacidad(self, cola_datos):
        """Resuelve la cola de permisos asociada a una cola de datos declarada."""
        if cola_datos not in self._colas_datos:
            raise ErrorConfiguracion("La cola no pertenece a la topología de datos")
        return _construir_nombre_amqp("capacidad", cola_datos)

    def marca_capacidad(self):
        """Identifica una topología y su límite de capacidad para validar el arranque."""
        partes = ["permisos-transaccionales-amqp"] + sorted(self._colas)
        partes.extend("|".join(enlace) for enlace in sorted(self._enlaces))
        partes.append(str(self._configuracion.limites.max_mensajes_por_cola))
        huella = hashlib.sha256("\n".join(partes).encode("utf-8")).hexdigest()
        return _construir_nombre_amqp("capacidad", "lista", huella)

    def _validar_instancia_destinataria(self, grupo, instancia):
        """Comprueba que una instancia explícita pertenezca al grupo destinatario."""
        if instancia is not None:
            cantidad = self._configuracion.obtener_grupo(grupo).instancias
            if type(instancia) is not int or not 0 <= instancia < cantidad:
                raise ErrorConfiguracion("La instancia destinataria está fuera del grupo")

    def _buscar_ruta_de_salida(self, grupo, entrada):
        """Obtiene la ruta desde el grupo local hacia el destino y entrada indicados."""
        return self._configuracion.obtener_ruta(
            self._configuracion.proceso.grupo, grupo, entrada,
        )

    def _agregar_ruta(self, ruta):
        """Acumula los recursos de la ruta, reutilizando los comunes a su grupo."""
        control = _construir_destino_control(ruta)
        self._exchanges.update((_construir_destino_datos(ruta, 0).exchange, control.exchange))
        grupo = self._configuracion.obtener_grupo(ruta.destino)
        for instancia in range(grupo.instancias):
            colas = _construir_colas_instancia(grupo.nombre, instancia)
            afinidad = _construir_destino_datos(ruta, instancia)
            control_individual = _construir_destino_control(ruta, instancia)
            self._colas.update((colas.afinidad, colas.control))
            self._colas_datos.add(colas.afinidad)
            self.cola_capacidad(colas.afinidad)
            # Control admite difusión y direccionamiento individual sobre las
            # mismas colas; cada clave individual tiene un único destinatario.
            self._enlaces.update((
                (colas.afinidad, afinidad.exchange, afinidad.clave),
                (colas.control, control.exchange, control.clave),
                (colas.control, control_individual.exchange, control_individual.clave),
            ))


def _construir_nombre_amqp(*partes):
    """Centraliza el prefijo, separador y límite de nombres físicos y claves."""
    nombre = ".".join((PREFIJO_RECURSOS,) + tuple(str(parte) for parte in partes))
    if len(nombre.encode("utf-8")) > MAX_BYTES_NOMBRE_AMQP:
        raise ErrorConfiguracion("Un nombre de recurso o clave excede el límite AMQP")
    return nombre


def _construir_colas_instancia(grupo, instancia):
    """Construye los nombres de afinidad y control de una réplica."""
    return ColasRecepcion(
        afinidad=_construir_nombre_amqp("cola", grupo, "afinidad", instancia),
        control=_construir_nombre_amqp("cola", grupo, "control", instancia),
    )


def _construir_destino_datos(ruta, instancia):
    """Construye la dirección de datos hacia la cola de afinidad de una réplica."""
    if instancia is None:
        raise ErrorConfiguracion("Los datos requieren una instancia destinataria")
    return DestinoPublicacion(
        exchange=_construir_nombre_amqp("exchange", ruta.origen, "datos"),
        clave=_construir_nombre_amqp(ruta.destino, ruta.entrada, "afinidad", instancia),
        cola=_construir_nombre_amqp("cola", ruta.destino, "afinidad", instancia),
    )


def _construir_destino_control(ruta, instancia=None):
    """Construye la dirección de controles hacia el grupo o una réplica."""
    sufijo = () if instancia is None else ("instancia", instancia)
    return DestinoPublicacion(
        exchange=_construir_nombre_amqp("exchange", ruta.origen, "control"),
        clave=_construir_nombre_amqp(ruta.destino, ruta.entrada, *sufijo),
    )
