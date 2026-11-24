"""Recepción de datos y controles con confirmación y devolución de capacidad."""

from functools import partial
import logging
from typing import Callable, Optional

import pika

from ...common.protocol.dto import Datos, Entrada, EntregaActual, Evento, TipoMensaje
from ...common.protocol.registros import Esquema
from .conexion import ConexionBroker
from .capacidad import CapacidadDatos
from ..mensajes.codificacion import CodificadorMensajes
from ..configuracion.modelos import ConfiguracionComunicacion, ErrorConfiguracion
from ..interfaz import ErrorConexion, ErrorMensajeInvalido, ErrorRecepcion, OperacionInterrumpida
from ..mensajes.rechazos import ENTRADA_ERRORES, notificacion_rechazo
from .topologia import TopologiaGrupos


REGISTRO = logging.getLogger(__name__)


class ReceptorDatos:
    """Entrega datos o un cierre por llamada y confirma después de procesarlos.

    El negocio se ejecuta fuera de los callbacks de Pika para poder esperar
    capacidad de salida. informar_error retorna cuando el aviso fue aceptado.
    Los controles EOF se atienden internamente entre entregas.
    Los consumidores del canal pertenecen exclusivamente a esta instancia.
    El callback debe retornar con sus efectos recuperables y salidas completadas;
    una excepción conserva la entrega para reintentar. EOF se persiste antes del ACK.
    """

    def __init__(
        self, configuracion: ConfiguracionComunicacion, conexion: ConexionBroker,
        *, informar_error: Callable[[Datos], None], capacidad=None, topologia=None, cierre=None,
    ):
        if not callable(informar_error):
            raise ErrorConfiguracion("Se requiere un manejador de notificaciones de error")
        self._configuracion = configuracion
        self._conexion = conexion
        self._informar_error = informar_error
        self._coordinacion = cierre
        self._codificador = CodificadorMensajes(
            configuracion.limites.max_bytes_por_mensaje,
            comprobar_detencion=conexion.comprobar_apagado,
        )
        if topologia is None:
            topologia = TopologiaGrupos(configuracion)
            topologia.declarar(conexion)
        self._colas = topologia.colas_recepcion()
        if self._colas is None:
            raise ErrorConfiguracion("El grupo local no tiene rutas de recepción")
        self._capacidad = capacidad or CapacidadDatos(conexion, topologia)
        self._canal = conexion.crear_canal()
        self._canal.tx_select()
        self._confirmacion_interrumpida = False
        self._recibiendo = False
        self._detenido = False
        self._consumidores = []
        self._pendiente = None
        self._cola_pendiente = None
        self._reservada = False
        self._entrega = None
        self._error_procesamiento = None
        self._retener_pendiente = False
        self.entrega_actual = None
        self.entrada_finalizando = None

    def recibir(
        self, procesar: Callable[[Evento], None], *,
        al_finalizar: Optional[Callable[[Entrada], None]] = None,
    ) -> None:
        """Atiende controles y entrega datos o ejecuta la finalización de una entrada."""
        self._conexion.canal
        canal = self._canal
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
            pendiente = (self._coordinacion.siguiente_finalizacion_pendiente()
                         if self._coordinacion is not None else None)
            if pendiente is not None:
                self._finalizar_negocio(pendiente, procesar, al_finalizar)
                return
            while not self._detenido:
                self._entrega = None
                self._esperar_entrega(canal)
                entregado = False
                self._conexion.comprobar_apagado()
                if self._entrega is not None and not self._detenido:
                    if self._cola_pendiente == self._colas.control:
                        entregado = self._procesar_control(canal, self._entrega, procesar, al_finalizar)
                    else:
                        self._procesar_datos(canal, self._entrega, procesar, al_finalizar)
                        entregado = True
                self._limpiar_recepcion_interrumpida(canal)
                if entregado:
                    break
        except BaseException as error:
            try:
                self._limpiar_recepcion_interrumpida(canal)
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
            self._conexion.programar(self._detener_consumidores)
            self._detenido = True

    def _detener_consumidores(self):
        """Marca la recepción como detenida y cancela sus suscripciones."""
        self._detenido = True
        self._cancelar_consumidores(self._canal)

    def _esperar_entrega(self, canal):
        """Suscribe las colas locales y atiende eventos hasta retener una entrega."""
        canal.basic_qos(
            prefetch_count=self._configuracion.limites.mensajes_sin_confirmar,
            global_qos=True,
        )
        colas = (self._colas.control, self._colas.afinidad)
        for cola in colas:
            consumidor = canal.basic_consume(
                queue=cola, on_message_callback=partial(self._retener_entrega, cola), auto_ack=False,
            )
            self._consumidores.append(consumidor)
        canal.start_consuming()
        if self._entrega is None and not self._detenido:
            raise ErrorRecepcion("El broker canceló la recepción sin entregar datos")

    def _retener_entrega(self, cola, canal, entrega, propiedades, cuerpo):
        """Retiene una entrega sin ACK y cancela los consumidores para procesarla."""
        self._pendiente = entrega.delivery_tag
        self._cola_pendiente = cola
        self._reservada = cola != self._colas.control and self._capacidad.tiene_reserva(propiedades)
        self._entrega = cuerpo
        # Conserva esta entrega sin ACK y devuelve las prefetched sin procesar.
        self._cancelar_consumidores(canal)

    def _procesar_datos(self, canal, cuerpo, procesar, al_finalizar=None):
        """Valida el lote, ejecuta el negocio y registra su resolución antes del ACK.

        Si el lote completa la entrada, ejecuta también su finalización antes del ACK."""
        contabilizable = False
        try:
            if not self._reservada:
                raise ErrorMensajeInvalido("La entrega no tiene una reserva de capacidad")
            resultado = self._codificador.decodificar(cuerpo)
            sobre = resultado.sobre
            ruta = self._validar_ruta_y_productor(sobre)
            if sobre.tipo != TipoMensaje.DATOS:
                raise ErrorMensajeInvalido(
                    "Se recibió un control en una cola de datos",
                    entrada=sobre.entrada, origen=sobre.origen,
                )
            if self._coordinacion is not None:
                self._coordinacion.validar_datos(sobre)
                if self._coordinacion.ejecucion_retirada(sobre.entrada.id_ejecucion):
                    self._confirmar_o_rechazar_entrega(canal, rechazar=False)
                    return
                if self._coordinacion.lote_resuelto(sobre):
                    # Copia de un lote ya registrado: con afinidad vuelve a esta réplica y se descarta.
                    self._confirmar_o_rechazar_entrega(canal, rechazar=False)
                    return
            contabilizable = True
            if sobre.contenido.esquema not in ruta.esquemas:
                raise ErrorMensajeInvalido(
                    "El esquema no está admitido en la entrada",
                    entrada=sobre.entrada, origen=sobre.origen,
                    esquema=sobre.contenido.esquema.value,
                )
        except ErrorMensajeInvalido as error:
            self._rechazar_mensaje(canal, error, sobre=sobre if contabilizable else None)
            return
        for error in resultado.errores_por_registro:
            self._notificar_rechazo(
                sobre.entrada, sobre.contenido.esquema.value, error.detalle, error.indice,
            )
        # Mientras corre el callback, el worker puede consultar qué lote procesa.
        self.entrega_actual = EntregaActual(
            sobre.entrada, sobre.origen, sobre.id_entrada, sobre.indice,
        )
        try:
            if resultado.registros_validos:
                procesar(Datos(sobre.entrada, sobre.contenido))
        except BaseException as error:
            self._error_procesamiento = error
            raise
        finally:
            self.entrega_actual = None
        if self._coordinacion is not None:
            self._coordinacion.resolver_lote(sobre)
            # Si el EOF llegó antes que el último dato, este lote completa la entrada.
            pendiente = self._coordinacion.finalizacion_pendiente(sobre.entrada)
            if pendiente is not None:
                self._finalizar_negocio(pendiente, procesar, al_finalizar)
        self._confirmar_o_rechazar_entrega(canal, rechazar=False)

    def _procesar_control(self, canal, cuerpo, procesar, al_finalizar):
        """Persiste el control y la finalización atendida antes de confirmar su entrega."""
        try:
            sobre = self._codificador.decodificar(cuerpo).sobre
            evento = self._coordinacion.recibir_control(sobre)
        except ErrorMensajeInvalido as error:
            REGISTRO.warning("Control inválido: %s", error)
            self._confirmar_o_rechazar_entrega(canal, rechazar=True)
            return False
        except BaseException as error:
            # Un fallo del WAL no es una desconexión recuperable del canal.
            self._error_procesamiento = error
            raise
        if evento is not None:
            self._finalizar_negocio(evento, procesar, al_finalizar)
        self._confirmar_o_rechazar_entrega(canal, rechazar=False)
        return evento is not None

    def _finalizar_negocio(self, evento, procesar, al_finalizar):
        """Ejecuta o retoma el cierre y persiste su terminación solo si el negocio retorna."""
        self.entrada_finalizando = evento.entrada
        try:
            self._conexion.comprobar_apagado()
            if al_finalizar is None:
                procesar(evento)
            else:
                al_finalizar(evento.entrada)
            self._conexion.comprobar_apagado()
            self._coordinacion.confirmar_fin(evento.entrada)
        except BaseException as error:
            self._error_procesamiento = error
            raise
        finally:
            self.entrada_finalizando = None

    def _confirmar_o_rechazar_entrega(self, canal, *, rechazar):
        """Resuelve la entrega pendiente y su permiso en una transacción."""
        self._conexion.comprobar_apagado()
        try:
            self._capacidad.resolver_entrega(
                canal, self._cola_pendiente, self._pendiente,
                rechazar=rechazar, reservada=self._reservada,
            )
        except BaseException:
            self._confirmacion_interrumpida = True
            raise
        self._pendiente = None
        self._cola_pendiente = None
        self._reservada = False

    def _validar_ruta_y_productor(self, sobre):
        """Obtiene la ruta entrante y comprueba que la réplica productora exista."""
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

    def _notificar_rechazo(self, entrada, esquema, detalle, indice=None):
        """Registra el rechazo y publica su aviso salvo que ya sea una notificación de error."""
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

    def _rechazar_mensaje(self, canal, error, *, sobre=None):
        """Notifica el rechazo y persiste su aporte identificable antes de retirar la entrega."""
        if error.entrada is not None and error.origen is not None:
            try:
                self._validar_ruta_y_productor(error)
            except ErrorMensajeInvalido:
                REGISTRO.warning("Mensaje inválido sin origen local válido: %s", error)
            else:
                self._notificar_rechazo(error.entrada, error.esquema, str(error))
        else:
            REGISTRO.warning("Mensaje inválido sin ejecución identificable: %s", error)
        if sobre is not None and self._coordinacion is not None:
            self._coordinacion.resolver_lote(sobre)
        self._confirmar_o_rechazar_entrega(canal, rechazar=True)

    def _cancelar_consumidores(self, canal):
        """Cancela todas las suscripciones e informa los fallos de cancelación."""
        errores = []
        for consumidor in tuple(self._consumidores):
            try:
                if canal.is_open:
                    canal.basic_cancel(consumer_tag=consumidor)
                self._consumidores.remove(consumidor)
            except OperacionInterrumpida:
                raise
            except Exception as error:
                REGISTRO.exception("No se pudo cancelar el consumidor")
                errores.append(error)
        if errores:
            raise ErrorRecepcion("Falló la cancelación de consumidores") from errores[0]

    def _limpiar_recepcion_interrumpida(self, canal):
        """Reencola una entrega interrumpida sin devolver su permiso asociado."""
        self._conexion.preparar_limpieza()
        if self._confirmacion_interrumpida:
            # No se conoce si tx_commit llegó al broker. Cerrar evita confirmar
            # otra vez o devolver un permiso adicional durante la limpieza.
            if canal.is_open:
                canal.close()
            self._consumidores.clear()
            return
        try:
            if self._pendiente is not None and not self._retener_pendiente and canal.is_open:
                canal.basic_nack(delivery_tag=self._pendiente, requeue=True)
                canal.tx_commit()
                self._pendiente = None
                self._cola_pendiente = None
        finally:
            self._cancelar_consumidores(canal)
