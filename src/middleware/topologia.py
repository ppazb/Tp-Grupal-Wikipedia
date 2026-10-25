"""Recursos y convenciones de RabbitMQ derivados de los grupos y sus rutas."""

from dataclasses import dataclass
import hashlib
from typing import Optional

import pika

from ..common.protocol.dto import Entrada
from ..common.protocol.primitivas import ErrorSerializacion, EscritorBinario
from .broker_connection import ConexionBroker
from .configuracion import ConfiguracionComunicacion, ErrorConfiguracion
from .exchange_handler import declarar_exchange
from .interfaz import ErrorConexion, ErrorTopologia
from .queue_handler import declarar_cola, enlazar_cola


PREFIJO_RECURSOS = "wikipedia"
MAX_BYTES_NOMBRE_AMQP = 255


@dataclass(frozen=True)
class DestinoPublicacion:
    """Dirección física usada por el publicador interno del middleware."""

    exchange: str
    clave: str
    cola: Optional[str] = None


@dataclass(frozen=True)
class ColasRecepcion:
    """Nombres de las colas de reparto, afinidad y control de una instancia."""

    reparto: str
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
        """Declara exchanges, colas y enlaces, conservando los recursos existentes.

        Es repetible con los mismos parámetros. Las incompatibilidades elevan
        ErrorTopologia; las desconexiones, ErrorConexion. El llamador administra
        el cierre de la conexión, también si la declaración falla.
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
        """Resuelve una salida del productor local para el publicador.

        Sin instancia apunta a reparto; con instancia apunta a afinidad. La
        elección mediante hash queda a cargo del middleware, no de los workers.
        """
        ruta = self._buscar_ruta(grupo, entrada)
        if instancia is not None:
            cantidad = self._configuracion.obtener_grupo(grupo).instancias
            if type(instancia) is not int or not 0 <= instancia < cantidad:
                raise ErrorConfiguracion("La instancia destinataria está fuera del grupo")
        return _destino_datos(ruta, instancia)

    def destino_control(self, grupo: str, entrada: str, instancia=None) -> DestinoPublicacion:
        """Resuelve control hacia todo el grupo o hacia una única instancia."""
        ruta = self._buscar_ruta(grupo, entrada)
        if instancia is not None:
            cantidad = self._configuracion.obtener_grupo(grupo).instancias
            if type(instancia) is not int or not 0 <= instancia < cantidad:
                raise ErrorConfiguracion("La instancia destinataria está fuera del grupo")
        return _destino_control(ruta, instancia)

    def instancia_coordinadora(self, grupo: str, entrada: Entrada) -> int:
        """Asigna la barrera (ejecución, grupo, entrada) a una instancia estable."""
        cantidad = self._configuracion.obtener_grupo(grupo).instancias
        if type(entrada) is not Entrada:
            raise ErrorConfiguracion("Se esperaba una Entrada")
        escritor = EscritorBinario(self._configuracion.limites.max_bytes_por_mensaje)
        try:
            for texto in (entrada.id_ejecucion, grupo, entrada.nombre):
                if type(texto) is not str or not texto.strip():
                    raise ErrorConfiguracion("La barrera requiere identificadores no vacíos")
                escritor.texto(texto)
        except ErrorSerializacion as error:
            raise ErrorConfiguracion(str(error)) from error
        huella = hashlib.sha256(escritor.resultado()).digest()
        return int.from_bytes(huella, byteorder="big") % cantidad

    def colas_recepcion(self) -> Optional[ColasRecepcion]:
        """Devuelve las colas locales; una fuente sin rutas entrantes no tiene colas."""
        proceso = self._configuracion.proceso
        if not any(ruta.destino == proceso.grupo for ruta in self._configuracion.rutas):
            return None
        return _colas_instancia(proceso.grupo, proceso.instancia)

    def colas_datos(self):
        """Devuelve las colas de datos del despliegue, sin duplicar las compartidas."""
        return tuple(sorted(self._colas_datos))

    def cola_capacidad(self, cola_datos):
        """Resuelve la cola de permisos asociada a una cola de datos declarada."""
        if cola_datos not in self._colas_datos:
            raise ErrorConfiguracion("La cola no pertenece a la topología de datos")
        return _nombre("capacidad", cola_datos)

    def marca_capacidad(self):
        """Identifica una topología y su límite de capacidad para validar el arranque."""
        partes = sorted(self._colas)
        partes.extend("|".join(enlace) for enlace in sorted(self._enlaces))
        partes.append(str(self._configuracion.limites.max_mensajes_por_cola))
        huella = hashlib.sha256("\n".join(partes).encode("utf-8")).hexdigest()
        return _nombre("capacidad", "lista", huella)

    def _buscar_ruta(self, grupo, entrada):
        return self._configuracion.obtener_ruta(
            self._configuracion.proceso.grupo, grupo, entrada,
        )

    def _agregar_ruta(self, ruta):
        """Acumula los recursos de la ruta, reutilizando los comunes a su grupo."""
        reparto = _destino_datos(ruta)
        control = _destino_control(ruta)
        self._exchanges.update((reparto.exchange, control.exchange))
        grupo = self._configuracion.obtener_grupo(ruta.destino)
        for instancia in range(grupo.instancias):
            colas = _colas_instancia(grupo.nombre, instancia)
            afinidad = _destino_datos(ruta, instancia)
            control_individual = _destino_control(ruta, instancia)
            self._colas.update((colas.reparto, colas.afinidad, colas.control))
            self._colas_datos.update((colas.reparto, colas.afinidad))
            self.cola_capacidad(colas.reparto)
            self.cola_capacidad(colas.afinidad)
            # Control admite difusión y direccionamiento individual sobre las
            # mismas colas; cada clave individual tiene un único destinatario.
            self._enlaces.update((
                (colas.reparto, reparto.exchange, reparto.clave),
                (colas.afinidad, afinidad.exchange, afinidad.clave),
                (colas.control, control.exchange, control.clave),
                (colas.control, control_individual.exchange, control_individual.clave),
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
        cola=(
            _nombre("cola", ruta.destino, "reparto") if instancia is None
            else _nombre("cola", ruta.destino, "afinidad", instancia)
        ),
    )


def _destino_control(ruta, instancia=None):
    sufijo = () if instancia is None else ("instancia", instancia)
    return DestinoPublicacion(
        exchange=_nombre("exchange", ruta.origen, "control"),
        clave=_nombre(ruta.destino, ruta.entrada, *sufijo),
    )
