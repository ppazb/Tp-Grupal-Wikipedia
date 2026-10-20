"""Recepción de datos con confirmación manual; la coordinación EOF es separada."""

import logging
from typing import Callable

import pika

from ..common.protocol.dto import Datos, TipoMensaje
from .broker_connection import ConexionBroker
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion, ErrorConfiguracion
from .interfaz import ErrorConexion, ErrorMensaje, ErrorRecepcion
from .topologia import TopologiaGrupos


REGISTRO = logging.getLogger(__name__)


class ReceptorDatos:
    """Entrega un lote por llamada, desde reparto o afinidad de la instancia.

    Usa el canal exclusivamente para sus consumidores; puede compartirlo con
    PublicadorDatos. Se utiliza dentro del contexto de ConexionBroker, que debe
    cerrarse también ante errores. No consume controles ni genera FinEntrada:
    esa coordinación se incorporará con EOF.

    Los errores detienen la llamada y devuelven el mensaje a la cola cuando el
    canal sigue abierto. No descarta mensajes inválidos ni reintenta: el llamador
    debe resolver el error antes de volver a recibir. No garantiza exactamente
    una vez: si falla el ACK, el procesamiento puede haber concluido igualmente.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
    ):
        self._configuracion = configuracion
        self._conexion = conexion
        self._codificador = CodificadorMensajes(configuracion.limites.max_bytes_por_mensaje)
        topologia = TopologiaGrupos(configuracion)
        self._colas = topologia.colas_recepcion()
        if self._colas is None:
            raise ErrorConfiguracion("El grupo local no tiene rutas de recepción")
        topologia.declarar(conexion)
        self._recibiendo = False
        self._detenido = False
        self._consumidores = []
        self._pendiente = None
        self._entregado = False
        self._error_procesamiento = None

    def recibir(self, procesar: Callable[[Datos], None]) -> None:
        """Bloquea en E/S hasta procesar un lote o atender una detención.

        Confirma el lote solo después de retornar procesar. Las excepciones del
        consumidor se propagan sin traducirlas, incluso si son errores de Pika.
        No admite recepción reentrante ni compartir consumidores en este canal.
        """
        canal = self._conexion.canal
        if self._recibiendo:
            raise ErrorRecepcion("La recepción no admite llamadas reentrantes")
        if self._consumidores or self._pendiente is not None:
            raise ErrorRecepcion("Quedaron recursos pendientes de limpieza; cerrar la conexión")
        if not callable(procesar):
            raise ErrorRecepcion("El procesador debe ser invocable")
        if self._detenido:
            return
        self._recibiendo = True
        self._entregado = False
        self._error_procesamiento = None
        try:
            self._consumir(canal, procesar)
        except BaseException as error:
            try:
                self._liberar_consumidores(canal)
            except Exception:
                REGISTRO.exception("Falló la limpieza de recepción; se conserva el error original")
            if error is self._error_procesamiento:
                raise
            if isinstance(error, (pika.exceptions.AMQPConnectionError, OSError)):
                raise ErrorConexion("Se interrumpió la conexión durante la recepción") from error
            if isinstance(error, pika.exceptions.AMQPError):
                raise ErrorRecepcion("Falló la recepción o su confirmación") from error
            raise
        finally:
            self._recibiendo = False
            self._error_procesamiento = None

    def detener_recepcion(self) -> None:
        """Solicita una detención permanente, atendida por el bucle de Pika.

        No interrumpe el procesador en curso ni cierra recursos. Puede solicitarse
        desde el procesador o desde una notificación del bucle de eventos. No
        llamar directamente desde un manejador de señal: su adaptación queda
        para la integración de SIGTERM. No requiere crear threads.
        """
        if not self._detenido:
            self._conexion.programar(self._detener)
            self._detenido = True

    def _detener(self):
        self._detenido = True
        if self._recibiendo:
            self._liberar_consumidores(self._conexion.canal)

    def _consumir(self, canal, procesar):
        # El límite global abarca reparto y afinidad en conjunto, no por cola.
        canal.basic_qos(
            prefetch_count=self._configuracion.limites.mensajes_sin_confirmar,
            global_qos=True,
        )
        for cola in (self._colas.reparto, self._colas.afinidad):
            consumidor = canal.basic_consume(
                queue=cola,
                on_message_callback=lambda canal, entrega, propiedades, cuerpo:
                    self._procesar(canal, entrega, cuerpo, procesar),
                auto_ack=False,
            )
            self._consumidores.append(consumidor)
        canal.start_consuming()
        if not self._entregado and not self._detenido:
            raise ErrorRecepcion("El broker canceló la recepción sin entregar datos")
        self._liberar_consumidores(canal)

    def _procesar(self, canal, entrega, cuerpo, procesar):
        self._pendiente = entrega.delivery_tag
        if self._detenido:
            self._liberar_consumidores(canal)
            return
        sobre = self._codificador.decodificar(cuerpo)
        self._validar_origen(sobre)
        if sobre.tipo != TipoMensaje.DATOS:
            raise ErrorMensaje("Se recibió un control en una cola de datos")
        try:
            procesar(Datos(sobre.entrada, sobre.contenido))
        except BaseException as error:
            self._error_procesamiento = error
            raise
        canal.basic_ack(delivery_tag=self._pendiente)
        self._pendiente = None
        self._entregado = True
        # Cancelar devuelve también las entregas prefetched que Pika aún no
        # despachó. Nunca se confirman mensajes que el negocio no procesó.
        self._liberar_consumidores(canal)

    def _validar_origen(self, sobre):
        rutas = self._configuracion.rutas
        if not any(
            ruta.origen == sobre.origen.grupo
            and ruta.destino == self._configuracion.proceso.grupo
            and ruta.entrada == sobre.entrada.nombre
            for ruta in rutas
        ):
            raise ErrorMensaje("El mensaje no corresponde a una ruta de entrada local")
        grupo = self._configuracion.obtener_grupo(sobre.origen.grupo)
        if sobre.origen.instancia >= grupo.instancias:
            raise ErrorMensaje("La instancia productora no pertenece al grupo de origen")

    def _liberar_consumidores(self, canal):
        """Intenta todas las cancelaciones aun si una falla; conserva referencias."""
        errores = []
        if not canal.is_open:
            # Al cerrarse el canal, RabbitMQ recupera las entregas sin ACK.
            self._pendiente = None
            self._consumidores.clear()
            return
        if self._pendiente is not None:
            try:
                canal.basic_nack(delivery_tag=self._pendiente, requeue=True)
                self._pendiente = None
            except Exception as error:
                REGISTRO.exception("No se pudo devolver el mensaje sin confirmar")
                errores.append(error)
        for consumidor in tuple(self._consumidores):
            try:
                canal.basic_cancel(consumer_tag=consumidor)
                self._consumidores.remove(consumidor)
            except Exception as error:
                REGISTRO.exception("No se pudo cancelar el consumidor")
                errores.append(error)
        if errores:
            raise ErrorRecepcion("Falló la liberación de consumidores; ver registro") from errores[0]
