"""Recepción de datos y controles con confirmación y devolución de capacidad."""

from functools import partial
import logging
from typing import Callable, Optional

import pika

from ..common.protocol.dto import Datos, Entrada, Evento, TipoMensaje
from ..common.protocol.registros import Esquema
from .broker_connection import ConexionBroker
from .capacidad import CapacidadDatos
from .codificacion import CodificadorMensajes
from .configuracion import ConfiguracionComunicacion, ErrorConfiguracion
from .interfaz import ErrorConexion, ErrorMensajeInvalido, ErrorRecepcion
from .rechazos import ENTRADA_ERRORES, notificacion_rechazo
from .topologia import TopologiaGrupos


REGISTRO = logging.getLogger(__name__)


class ReceptorDatos:
    """Entrega datos o un cierre por llamada y confirma después de procesarlos.

    El negocio se ejecuta fuera de los callbacks de Pika para poder esperar
    capacidad de salida. informar_error retorna cuando el aviso fue aceptado.
    Los controles de coordinación se atienden internamente entre entregas.
    Los consumidores del canal pertenecen exclusivamente a esta instancia.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, informar_error: Callable[[Datos], None], capacidad=None, topologia=None, coordinacion=None,
    ):
        if not callable(informar_error):
            raise ErrorConfiguracion("Se requiere un manejador de notificaciones de error")
        self._configuracion = configuracion
        self._conexion = conexion
        self._informar_error = informar_error
        self._coordinacion = coordinacion
        self._codificador = CodificadorMensajes(configuracion.limites.max_bytes_por_mensaje)
        if topologia is None:
            topologia = TopologiaGrupos(configuracion)
            topologia.declarar(conexion)
        self._colas = topologia.colas_recepcion()
        if self._colas is None:
            raise ErrorConfiguracion("El grupo local no tiene rutas de recepción")
        self._capacidad = capacidad or CapacidadDatos(conexion, topologia)
        self._recibiendo = False
        self._detenido = False
        self._consumidores = []
        self._pendiente = None
        self._cola_pendiente = None
        self._reservada = False
        self._entrega = None
        self._error_procesamiento = None
        self._retener_pendiente = False

    def recibir(
        self, procesar: Callable[[Evento], None], *,
        al_finalizar: Optional[Callable[[Entrada], None]] = None,
    ) -> None:
        """Atiende controles y entrega datos o ejecuta la finalización de una entrada."""
        canal = self._conexion.canal
        if self._recibiendo:
            raise ErrorRecepcion("La recepción no admite llamadas reentrantes")
        if self._consumidores or self._pendiente is not None or self._cola_pendiente is not None:
            raise ErrorRecepcion("Quedaron recursos pendientes de limpieza; cerrar la conexión")
        if not callable(procesar):
            raise ErrorRecepcion("El procesador debe ser invocable")
        if al_finalizar is not None and not callable(al_finalizar):
            raise ErrorRecepcion("El finalizador de entradas debe ser invocable")
        if self._detenido:
            return
        self._recibiendo = True
        self._error_procesamiento = None
        try:
            while not self._detenido:
                self._entrega = None
                self._esperar(canal)
                entregado = False
                self._conexion.comprobar_apagado()
                if self._entrega is not None and not self._detenido:
                    if self._cola_pendiente == self._colas.control:
                        entregado = self._procesar_control(canal, self._entrega, procesar, al_finalizar)
                    else:
                        self._procesar(canal, self._entrega, procesar)
                        entregado = True
                self._liberar_consumidores(canal)
                if entregado:
                    break
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
            self._entrega = None
            self._error_procesamiento = None

    def detener_recepcion(self) -> None:
        """Detiene nuevas entregas y deja completar el procesamiento en curso."""
        if not self._detenido:
            self._conexion.programar(self._detener)
            self._detenido = True

    def _detener(self):
        self._detenido = True
        self._cancelar_consumidores(self._conexion.canal)

    def _esperar(self, canal):
        canal.basic_qos(
            prefetch_count=self._configuracion.limites.mensajes_sin_confirmar,
            global_qos=True,
        )
        colas = (self._colas.reparto, self._colas.afinidad)
        if self._coordinacion is not None:
            colas = (self._colas.control,) + colas
        for cola in colas:
            consumidor = canal.basic_consume(
                queue=cola, on_message_callback=partial(self._capturar, cola), auto_ack=False,
            )
            self._consumidores.append(consumidor)
        canal.start_consuming()
        if self._entrega is None and not self._detenido:
            raise ErrorRecepcion("El broker canceló la recepción sin entregar datos")

    def _capturar(self, cola, canal, entrega, propiedades, cuerpo):
        self._pendiente = entrega.delivery_tag
        self._cola_pendiente = cola
        self._reservada = cola != self._colas.control and self._capacidad.tiene_reserva(propiedades)
        self._entrega = cuerpo
        # Conserva esta entrega sin ACK y devuelve las prefetched sin procesar.
        self._cancelar_consumidores(canal)

    def _procesar(self, canal, cuerpo, procesar):
        contabilizable = False
        try:
            if not self._reservada:
                raise ErrorMensajeInvalido("La entrega no tiene una reserva de capacidad")
            resultado = self._codificador.decodificar(cuerpo)
            sobre = resultado.sobre
            ruta = self._validar_origen(sobre)
            if sobre.tipo != TipoMensaje.DATOS:
                raise ErrorMensajeInvalido(
                    "Se recibió un control en una cola de datos",
                    entrada=sobre.entrada, origen=sobre.origen,
                )
            if self._coordinacion is not None:
                self._coordinacion.validar_datos(sobre)
            contabilizable = True
            if sobre.contenido.esquema not in ruta.esquemas:
                raise ErrorMensajeInvalido(
                    "El esquema no está admitido en la entrada",
                    entrada=sobre.entrada, origen=sobre.origen,
                    esquema=sobre.contenido.esquema.value,
                )
        except ErrorMensajeInvalido as error:
            self._rechazar_mensaje(canal, error)
            if contabilizable and self._coordinacion is not None:
                self._coordinacion.resolver_lote(sobre)
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
        self._resolver(canal, rechazar=False)
        if self._coordinacion is not None:
            self._coordinacion.resolver_lote(sobre)

    def _procesar_control(self, canal, cuerpo, procesar, al_finalizar):
        try:
            sobre = self._codificador.decodificar(cuerpo).sobre
            evento = self._coordinacion.recibir_control(sobre)
        except ErrorMensajeInvalido as error:
            REGISTRO.warning("Control inválido: %s", error)
            self._resolver(canal, rechazar=True)
            return False
        if evento is not None:
            try:
                if al_finalizar is None:
                    procesar(evento)
                else:
                    al_finalizar(evento.entrada)
            except BaseException as error:
                self._error_procesamiento = error
                raise
        self._resolver(canal, rechazar=False)
        if evento is not None:
            self._coordinacion.confirmar_fin(sobre.entrada)
        return evento is not None

    def _resolver(self, canal, *, rechazar):
        self._conexion.comprobar_apagado()
        if rechazar:
            canal.basic_reject(delivery_tag=self._pendiente, requeue=False)
        else:
            canal.basic_ack(delivery_tag=self._pendiente)
        self._pendiente = None
        if self._reservada:
            self._capacidad.devolver(self._cola_pendiente)
        self._cola_pendiente = None
        self._reservada = False

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
        try:
            self._informar_error(notificacion_rechazo(entrada, esquema, detalle, indice=indice))
        except BaseException as error:
            self._retener_pendiente = True
            self._error_procesamiento = error
            raise

    def _rechazar_mensaje(self, canal, error):
        if error.entrada is not None and error.origen is not None:
            try:
                self._validar_origen(error)
            except ErrorMensajeInvalido:
                REGISTRO.warning("Mensaje inválido sin origen local válido: %s", error)
            else:
                self._notificar(error.entrada, error.esquema, str(error))
        else:
            REGISTRO.warning("Mensaje inválido sin ejecución identificable: %s", error)
        self._resolver(canal, rechazar=True)

    def _cancelar_consumidores(self, canal):
        errores = []
        for consumidor in tuple(self._consumidores):
            try:
                if canal.is_open:
                    canal.basic_cancel(consumer_tag=consumidor)
                self._consumidores.remove(consumidor)
            except Exception as error:
                REGISTRO.exception("No se pudo cancelar el consumidor")
                errores.append(error)
        if errores:
            raise ErrorRecepcion("Falló la cancelación de consumidores") from errores[0]

    def _liberar_consumidores(self, canal):
        """Reencola una entrega interrumpida sin devolver su permiso asociado."""
        try:
            if self._pendiente is not None and not self._retener_pendiente and canal.is_open:
                canal.basic_nack(delivery_tag=self._pendiente, requeue=True)
                self._pendiente = None
                self._cola_pendiente = None
        finally:
            self._cancelar_consumidores(canal)
