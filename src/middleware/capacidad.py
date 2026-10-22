"""Permisos de capacidad compartidos mediante colas básicas de RabbitMQ."""

import logging
import sys

import pika

from .interfaz import ErrorCapacidad, OperacionInterrumpida
from .configuracion import ErrorConfiguracion


REGISTRO = logging.getLogger(__name__)
PERMISO = b"\x01"
CABECERA_CAPACIDAD = "wikipedia_capacidad"
VERSION_CAPACIDAD = 1
COLA_INEXISTENTE = 404


def publicar_permiso(canal, cola):
    """Publica un permiso y espera su confirmación antes de retornar."""
    canal.basic_publish(
        exchange="", routing_key=cola, body=PERMISO, mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/octet-stream",
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        ),
    )


class CapacidadDatos:
    """Reserva un lugar por publicación y lo devuelve al resolver su entrega.

    El canal de permisos comparte conexión con los datos, pero tiene su propio
    prefetch. Cada instancia permite una sola espera a la vez. Todos los procesos
    utilizan permisos inicializados para la misma topología y capacidad.
    """

    def __init__(self, conexion, topologia):
        self._conexion = conexion
        self._topologia = topologia
        self._canal = conexion.crear_canal()
        self._consumidor = None
        self._esperando = False
        self._detenido = False
        try:
            self._canal.queue_declare(queue=topologia.marca_capacidad(), passive=True)
            self._canal.confirm_delivery()
            self._canal.basic_qos(prefetch_count=1, global_qos=True)
        except pika.exceptions.ChannelClosedByBroker as error:
            if error.reply_code == COLA_INEXISTENTE:
                raise ErrorConfiguracion("Inicializar la capacidad del despliegue antes de abrir el middleware") from error
            raise ErrorCapacidad("No se pudo abrir el canal de permisos") from error
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("No se pudo abrir el canal de permisos") from error

    def adquirir(self, cola_datos):
        """Espera un permiso por eventos de E/S, sin sondeo ni temporizadores."""
        self._conexion.canal
        if self._esperando:
            raise ErrorCapacidad("La reserva de capacidad no admite reentrancia")
        if self._consumidor is not None:
            raise ErrorCapacidad("No se pudo cerrar la reserva anterior; cerrar la conexión")
        if self._detenido:
            raise OperacionInterrumpida("La espera de capacidad fue interrumpida")
        entrega = []
        self._esperando = True
        try:
            self._consumidor = self._canal.basic_consume(
                queue=self._topologia.cola_capacidad(cola_datos), auto_ack=False,
                on_message_callback=lambda canal, metodo, propiedades, cuerpo:
                    self._recibir_permiso(entrega, metodo, cuerpo),
            )
            self._canal.start_consuming()
            if self._detenido:
                raise OperacionInterrumpida("La espera de capacidad fue interrumpida")
            if not entrega:
                raise ErrorCapacidad("El broker canceló la espera sin entregar un permiso")
            etiqueta, cuerpo = entrega[0]
            if cuerpo != PERMISO:
                self._canal.basic_reject(delivery_tag=etiqueta, requeue=False)
                entrega.clear()
                raise ErrorCapacidad("El permiso recibido es inválido")
            self._canal.basic_ack(delivery_tag=etiqueta)
            entrega.clear()
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("Falló la reserva de capacidad") from error
        finally:
            self._esperando = False
            self._limpiar_espera(entrega, sys.exc_info()[0] is not None)

    def _recibir_permiso(self, entrega, metodo, cuerpo):
        entrega.append((metodo.delivery_tag, cuerpo))
        self._cancelar_consumidor()

    def _cancelar_consumidor(self):
        if self._consumidor is not None and self._canal.is_open:
            self._canal.basic_cancel(consumer_tag=self._consumidor)
            self._consumidor = None

    def _limpiar_espera(self, entrega, preservar_error):
        try:
            self._cancelar_consumidor()
            if entrega and self._canal.is_open:
                self._canal.basic_nack(delivery_tag=entrega[0][0], requeue=True)
        except (pika.exceptions.AMQPError, OSError) as error:
            if not preservar_error:
                raise ErrorCapacidad("No se pudo cancelar la reserva") from error
            REGISTRO.exception("Falló la limpieza de una reserva; se conserva el error original")

    def devolver(self, cola_datos):
        """Devuelve un permiso después de resolver una entrega o un envío rechazado."""
        self._conexion.canal
        try:
            publicar_permiso(self._canal, self._topologia.cola_capacidad(cola_datos))
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("No se pudo devolver el permiso de capacidad") from error

    def interrumpir(self):
        """Cancela la espera activa y rechaza reservas nuevas en esta instancia."""
        self._detenido = True
        self._cancelar_consumidor()

    @staticmethod
    def tiene_reserva(propiedades):
        """Comprueba la marca de capacidad de una publicación del middleware."""
        cabeceras = propiedades.headers if propiedades is not None else None
        valor = cabeceras.get(CABECERA_CAPACIDAD) if isinstance(cabeceras, dict) else None
        return type(valor) is int and valor == VERSION_CAPACIDAD
