"""Publicación confirmada de datos usando los destinos lógicos del middleware.

Uso interno, con una conexión cuyo cierre administra el llamador:

    with ConexionBroker(configuracion.broker) as conexion:
        publicador = PublicadorDatos(configuracion, conexion)
        publicador.publicar("top_n", datos)

Este componente implementa la salida de datos; no implementa recepción ni EOF.
"""

import hashlib
from collections.abc import Sequence
from typing import Optional

import pika

from ..common.protocol.dto import Datos, EnvioDatos, SobreMensaje, TipoMensaje
from ..common.protocol.serializer import ErrorSerializacion, serializar_json
from .broker_connection import ConexionBroker
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion
from .interfaz import (
    ErrorConexion, ErrorMensaje, ErrorMiddleware, ErrorPublicacionParcial,
    PublicacionRechazada,
)
from .topologia import TopologiaGrupos


class PublicadorDatos:
    """Publica secuencialmente desde el proceso dueño de la conexión.

    Cada Datos es una unidad de publicación: una lista de registros viaja como
    un único lote. No acumula registros, divide lotes ni introduce temporizadores.
    Si el lote supera max_bytes_por_mensaje se rechaza antes de enviarlo.

    La confirmación significa aceptación del broker, no procesamiento remoto.
    Espera esa confirmación antes del siguiente envío: hay como máximo una
    publicación pendiente por instancia. Un NACK se informa mediante
    PublicacionRechazada; no implementa una espera por capacidad disponible.
    No hace reconexiones ni reintentos automáticos. La conexión debe permanecer
    en su contexto with también si falla la inicialización de este componente.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
    ):
        self._configuracion = configuracion
        self._conexion = conexion
        self._topologia = TopologiaGrupos(configuracion)
        self._codificador = CodificadorMensajes(configuracion.limites.max_bytes_por_mensaje)
        self._topologia.declarar(conexion)
        try:
            conexion.canal.confirm_delivery()
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudieron habilitar las confirmaciones") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudieron habilitar las confirmaciones") from error

    def publicar(
        self, grupo: str, datos: Datos, *, clave_afinidad: Optional[str] = None,
    ) -> None:
        """Envía a reparto o a la instancia correspondiente a la clave de afinidad.

        La afinidad usa la ejecución y la clave lógica; todos los registros de
        un lote con afinidad deben pertenecer a esa misma clave. Sin afinidad,
        la cola compartida reparte lotes completos entre los consumidores.
        """
        self.publicar_en_ramas((EnvioDatos(grupo, datos, clave_afinidad),))

    def publicar_en_ramas(self, envios: Sequence[EnvioDatos]) -> None:
        """Publica una lista acotada de salidas, en el orden recibido.

        Permite copiar Datos a varias ramas o enviar una proyección por rama.
        Valida y codifica todas las salidas antes de publicar la primera. Una
        lista vacía no publica nada. No hay atomicidad entre las publicaciones:
        si falla una, detiene el envío y comunica las salidas ya confirmadas.
        No se debe reintentar el conjunto completo, pues duplicaría esas salidas.
        Si supera max_envios_por_publicacion, rechaza sin codificar ni enviar.
        No acepta iteradores: el llamador debe construir una secuencia acotada.
        Un rechazo remoto posterior a un éxito queda como causa de
        ErrorPublicacionParcial. No se deben olvidar las salidas confirmadas.
        """
        if not isinstance(envios, Sequence):
            raise ErrorMensaje("Las salidas deben ser una secuencia acotada")
        if len(envios) > self._configuracion.limites.max_envios_por_publicacion:
            raise PublicacionRechazada("La llamada supera max_envios_por_publicacion")
        preparados = [self._preparar(envio) for envio in envios]
        confirmadas = []
        for identidad, destino, cuerpo in preparados:
            try:
                self._publicar_cuerpo(destino, cuerpo)
            except ErrorMiddleware as error:
                if confirmadas:
                    raise ErrorPublicacionParcial(confirmadas, identidad) from error
                raise
            confirmadas.append(identidad)

    def _preparar(self, envio):
        if not isinstance(envio, EnvioDatos) or not isinstance(envio.datos, Datos):
            raise ErrorMensaje("Cada salida debe ser un EnvioDatos con Datos")
        if type(envio.grupo) is not str or not envio.grupo.strip():
            raise ErrorMensaje("El grupo destinatario debe ser texto no vacío")
        sobre = SobreMensaje(
            entrada=envio.datos.entrada,
            tipo=TipoMensaje.DATOS,
            origen=self._configuracion.proceso,
            contenido=envio.datos.contenido,
        )
        cuerpo = self._codificador.codificar(sobre)
        instancia = self._instancia_destinataria(envio)
        destino = self._topologia.destino_datos(
            envio.grupo, sobre.entrada.nombre, instancia,
        )
        return (envio.grupo, sobre.entrada), destino, cuerpo

    def _instancia_destinataria(self, envio):
        clave = envio.clave_afinidad
        if clave is None:
            return None
        if type(clave) is not str or not clave:
            raise ErrorMensaje("La clave de afinidad debe ser texto no vacío")
        try:
            identificador = serializar_json((envio.datos.entrada.id_ejecucion, clave))
        except ErrorSerializacion as error:
            raise ErrorMensaje("La clave de afinidad no se puede codificar") from error
        # hash() de Python cambia entre procesos. SHA-256 fija la asignación para
        # todos los productores mientras la cantidad de instancias permanezca fija.
        huella = hashlib.sha256(identificador).digest()
        cantidad = self._configuracion.obtener_grupo(envio.grupo).instancias
        return int.from_bytes(huella, byteorder="big") % cantidad

    def _publicar_cuerpo(self, destino, cuerpo):
        try:
            self._conexion.canal.basic_publish(
                exchange=destino.exchange,
                routing_key=destino.clave,
                body=cuerpo,
                mandatory=True,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                ),
            )
        except pika.exceptions.UnroutableError as error:
            raise ErrorMensaje("El mensaje no llegó a una cola destinataria") from error
        except pika.exceptions.NackError as error:
            # reject-publish puede producir un NACK al llenarse una cola. No se
            # oculta el rechazo ni se reintenta mediante sondeo activo o sleeps.
            raise PublicacionRechazada("El broker no admitió la publicación") from error
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudo confirmar la publicación") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudo publicar el mensaje") from error
