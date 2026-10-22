"""Recepción de lotes de datos con confirmación manual."""

import logging
from typing import Callable

import pika

from ..common.protocol.dto import Datos, TipoMensaje
from ..common.protocol.registros import Esquema
from .broker_connection import ConexionBroker
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion, ErrorConfiguracion
from .interfaz import ErrorConexion, ErrorMensajeInvalido, ErrorRecepcion
from .rechazos import ENTRADA_ERRORES, notificacion_rechazo
from .topologia import TopologiaGrupos


REGISTRO = logging.getLogger(__name__)


class ReceptorDatos:
    """Recibe un lote por llamada desde las colas de reparto y afinidad.

    Usa el canal exclusivamente para sus consumidores y puede compartirlo con
    PublicadorDatos. El llamador administra el cierre de la conexión.
    Los rechazos de datos se notifican antes de procesar los registros válidos.
    informar_error recibe Datos y retorna cuando la notificación fue aceptada.
    Si falla una notificación, retiene la entrega sin ACK hasta cerrar la conexión.
    Si falla el ACK, el procesamiento puede haber concluido igualmente.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, informar_error: Callable[[Datos], None],
    ):
        if not callable(informar_error):
            raise ErrorConfiguracion("Se requiere un manejador de notificaciones de error")
        self._configuracion = configuracion
        self._conexion = conexion
        self._informar_error = informar_error
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
        self._retener_pendiente = False

    def recibir(self, procesar: Callable[[Datos], None]) -> None:
        """Espera una entrega y procesa sus registros válidos o resuelve su rechazo.

        Confirma tras notificar los rechazos y procesar los registros válidos.
        Las excepciones de ambos manejadores se propagan sin traducirlas.
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
        """Solicita cancelar los consumidores y detener la recepción permanentemente.

        Deja terminar el procesamiento en curso y mantiene abierta la conexión.
        Se invoca desde el procesador o una notificación del bucle de eventos;
        no debe llamarse directamente desde un manejador de señales.
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
        try:
            resultado = self._codificador.decodificar(cuerpo)
            sobre = resultado.sobre
            ruta = self._validar_origen(sobre)
            if sobre.tipo != TipoMensaje.DATOS:
                raise ErrorMensajeInvalido(
                    "Se recibió un control en una cola de datos",
                    entrada=sobre.entrada, origen=sobre.origen,
                )
            if sobre.contenido.esquema not in ruta.esquemas:
                raise ErrorMensajeInvalido(
                    "El esquema no está admitido en la entrada",
                    entrada=sobre.entrada, origen=sobre.origen,
                    esquema=sobre.contenido.esquema.value,
                )
        except ErrorMensajeInvalido as error:
            self._rechazar_mensaje(canal, error)
            return
        for error in resultado.errores_por_registro:
            self._notificar(
                sobre.entrada, sobre.contenido.esquema.value, error.detalle, error.indice,
            )
        try:
            if resultado.registros_validos:
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
        try:
            ruta = self._configuracion.obtener_ruta(
                sobre.origen.grupo, self._configuracion.proceso.grupo, sobre.entrada.nombre,
            )
            grupo = self._configuracion.obtener_grupo(sobre.origen.grupo)
            if sobre.origen.instancia >= grupo.instancias:
                raise ErrorConfiguracion("La instancia productora no pertenece al grupo")
            return ruta
        except ErrorConfiguracion as error:
            raise ErrorMensajeInvalido("El origen no corresponde a una ruta local") from error

    def _notificar(self, entrada, esquema, detalle, indice=None):
        REGISTRO.warning(
            "Rechazo en ejecución %r, entrada %r: %s",
            entrada.id_ejecucion, entrada.nombre, detalle,
        )
        if esquema == Esquema.ERROR_REGISTRO or entrada.nombre == ENTRADA_ERRORES:
            return
        datos = notificacion_rechazo(entrada, esquema, detalle, indice=indice)
        try:
            self._informar_error(datos)
        except BaseException as error:
            # Evita reentregar en un bucle un lote cuyo aviso no pudo publicarse.
            self._retener_pendiente = True
            self._error_procesamiento = error
            raise

    def _rechazar_mensaje(self, canal, error):
        """Informa un mensaje inválido identificable y lo rechaza sin reencolar."""
        if error.entrada is not None and error.origen is not None:
            try:
                self._validar_origen(error)
            except ErrorMensajeInvalido:
                REGISTRO.warning("Mensaje inválido sin origen local válido: %s", error)
            else:
                self._notificar(error.entrada, error.esquema, str(error))
        else:
            REGISTRO.warning("Mensaje inválido sin ejecución identificable: %s", error)
        canal.basic_reject(delivery_tag=self._pendiente, requeue=False)
        self._pendiente = None
        self._entregado = True
        self._liberar_consumidores(canal)

    def _liberar_consumidores(self, canal):
        """Intenta todas las cancelaciones aun si una falla; conserva referencias."""
        errores = []
        if not canal.is_open:
            # Al cerrarse el canal, RabbitMQ recupera las entregas sin ACK.
            self._pendiente = None
            self._consumidores.clear()
            return
        if self._pendiente is not None and not self._retener_pendiente:
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
