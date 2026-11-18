"""Capacidad durable por lote y avisos de disponibilidad mediante RabbitMQ."""

import logging
import sys
from uuid import UUID

import pika

from src.middleware.interfaz import ErrorCapacidad, OperacionInterrumpida
from src.middleware.configuracion.modelos import ErrorConfiguracion
from src.middleware.transporte.permisos import PERMISO, publicar_permiso
from src.middleware.persistencia.reservas import RegistroReservas
from src.middleware.transporte.turnos import TurnosCompartidos


REGISTRO = logging.getLogger(__name__)
CABECERA_CAPACIDAD = 'wikipedia_capacidad'
CABECERA_RESERVA = 'wikipedia_reserva'
VERSION_CAPACIDAD = 2
COLA_INEXISTENTE = 404


class CapacidadDatos:
    """Cuenta lotes distintos en un WAL compartido; los avisos solo despiertan esperas.

    Una retransmisión conserva su reserva. La resolución durable precede al ACK
    de datos y es idempotente por ID. Cada instancia admite una espera a la vez.
    """

    def __init__(self, configuracion, conexion, topologia):
        self._conexion = conexion
        self._topologia = topologia
        self._canal = conexion.crear_canal()
        self._consumidor = None
        self._entrega = None
        self._esperando = False
        self._detenido = False
        try:
            self._canal.queue_declare(queue=topologia.marca_capacidad(), passive=True)
            self._canal.confirm_delivery()
            self._canal.basic_qos(prefetch_count=1, global_qos=True)
        except pika.exceptions.ChannelClosedByBroker as error:
            if error.reply_code == COLA_INEXISTENTE:
                raise ErrorConfiguracion('Inicializar la capacidad antes de abrir el middleware') from error
            raise ErrorCapacidad('No se pudo abrir el canal de capacidad') from error
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad('No se pudo abrir el canal de capacidad') from error
        self._turnos = TurnosCompartidos(conexion, tipo_error=ErrorCapacidad)
        self._registro = RegistroReservas(configuracion, conexion, topologia, self._turnos)

    def adquirir(self, cola_datos, identificador, cuerpo):
        """Reserva el lote o espera un aviso; reintentar no consume otro lugar."""
        self._conexion.canal
        if self._esperando or self._consumidor is not None or self._entrega is not None:
            raise ErrorCapacidad('La reserva no admite reentrancia ni esperas pendientes')
        if self._detenido:
            raise OperacionInterrumpida('La espera de capacidad fue interrumpida')
        self._esperando = True
        try:
            while True:
                self._conexion.comprobar_apagado()
                if self._detenido:
                    raise OperacionInterrumpida('La espera de capacidad fue interrumpida')
                with self._registro.abrir_reserva_exclusiva(cola_datos) as reserva:
                    resultado = reserva.adquirir(identificador, cuerpo)
                    if reserva.disponibles:
                        # Pasa el aviso a otro esperante antes de confirmar el propio.
                        publicar_permiso(self._canal, self._topologia.cola_capacidad(cola_datos))
                self._confirmar_aviso()
                if resultado is not None:
                    return resultado
                # Solo se vuelve a consultar tras una entrega, no por sondeo.
                self._esperar_aviso(cola_datos)
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad('Falló la reserva de capacidad') from error
        finally:
            self._esperando = False
            self._limpiar_espera(sys.exc_info()[0] is not None)

    def _esperar_aviso(self, cola):
        """Espera un aviso de capacidad y valida su contenido."""
        self._consumidor = self._canal.basic_consume(
            queue=self._topologia.cola_capacidad(cola), auto_ack=False,
            on_message_callback=self._recibir_aviso,
        )
        self._canal.start_consuming()
        if self._detenido:
            raise OperacionInterrumpida('La espera de capacidad fue interrumpida')
        if self._entrega is None:
            raise ErrorCapacidad('El broker canceló la espera sin entregar un aviso')
        if self._entrega[1] != PERMISO:
            raise ErrorCapacidad('Aviso de capacidad inválido')

    def _recibir_aviso(self, _canal, metodo, _propiedades, cuerpo):
        """Conserva el aviso sin ACK y cancela su consumidor."""
        self._entrega = (metodo.delivery_tag, cuerpo)
        self._cancelar_consumidor()

    def _confirmar_aviso(self):
        """Confirma el aviso recibido y retira su referencia local."""
        if self._entrega is not None:
            self._canal.basic_ack(delivery_tag=self._entrega[0])
            self._entrega = None

    def _cancelar_consumidor(self):
        """Cancela el consumidor activo del canal."""
        if self._consumidor is not None and self._canal.is_open:
            self._canal.basic_cancel(consumer_tag=self._consumidor)
            self._consumidor = None

    def _limpiar_espera(self, preservar_error):
        """Reencola el aviso pendiente preservando el error original si corresponde."""
        self._conexion.preparar_limpieza()
        try:
            self._cancelar_consumidor()
            if self._entrega is not None and self._canal.is_open:
                self._canal.basic_nack(delivery_tag=self._entrega[0], requeue=True)
                self._entrega = None
        except (pika.exceptions.AMQPError, OSError) as error:
            if not preservar_error:
                raise ErrorCapacidad('No se pudo limpiar la espera; cerrar la conexión') from error
            REGISTRO.exception('Falló la limpieza del aviso; se conserva el error original')

    def validar_entrega(self, cola_datos, identificador, cuerpo):
        """Valida la reserva contra el historial común, incluso para duplicados."""
        with self._registro.abrir_reserva_exclusiva(cola_datos) as reserva:
            reserva.validar_entrega(identificador, cuerpo)

    def devolver(self, cola_datos, identificador, *, consumida):
        """Persiste la devolución y despierta esperantes antes de confirmar el dato."""
        self._conexion.canal
        try:
            with self._registro.abrir_reserva_exclusiva(cola_datos) as reserva:
                reserva.devolver(identificador, consumida=consumida)
                publicar_permiso(self._canal, self._topologia.cola_capacidad(cola_datos))
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad('Falló la devolución durable de capacidad') from error

    def interrumpir(self):
        """Cancela esperas conservando reservas asociadas a salidas recuperables."""
        self._detenido = True
        self._cancelar_consumidor()
        self._turnos.detener()

    def compactar(self):
        """Compacta reservas compartidas sin modificar la capacidad disponible."""
        self._registro.compactar()

    def cerrar_transporte(self):
        """Descarta índices locales; los canales los cierra la conexión propietaria."""
        self._registro.descartar_indices()

    @staticmethod
    def identificar_reserva(propiedades):
        """Lee el ID de reserva de las cabeceras; las ausentes o inválidas dan None."""
        cabeceras = propiedades.headers if propiedades is not None else None
        if not isinstance(cabeceras, dict):
            return None
        version = cabeceras.get(CABECERA_CAPACIDAD)
        valor = cabeceras.get(CABECERA_RESERVA)
        if type(version) is not int or version != VERSION_CAPACIDAD or type(valor) is not str:
            return None
        try:
            identificador = UUID(valor)
            return identificador if identificador.int else None
        except ValueError:
            return None
