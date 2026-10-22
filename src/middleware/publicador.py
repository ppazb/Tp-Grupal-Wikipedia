"""Publicación confirmada de datos hacia grupos lógicos."""

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
    """Publica lotes secuencialmente y espera la confirmación de cada envío.

    Cada Datos viaja como un mensaje sujeto al límite de tamaño configurado.
    Un NACK eleva PublicacionRechazada, sin reintentos automáticos.
    El llamador administra el cierre de la conexión, incluso ante errores.
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
        """Valida y publica una secuencia acotada de salidas, en el orden recibido.

        Rechaza iteradores y secuencias que superen max_envios_por_publicacion.
        Codifica todas las salidas antes de enviar la primera; una secuencia vacía
        no publica nada. Si un envío falla después de otros confirmados, eleva
        ErrorPublicacionParcial con esas salidas y la causa del fallo.
        Repetir el conjunto puede duplicar las salidas confirmadas.
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
            # Un NACK puede indicar una cola llena, pero no identifica la causa.
            raise PublicacionRechazada("El broker no admitió la publicación") from error
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            raise ErrorConexion("No se pudo confirmar la publicación") from error
        except pika.exceptions.AMQPError as error:
            raise ErrorMensaje("No se pudo publicar el mensaje") from error
