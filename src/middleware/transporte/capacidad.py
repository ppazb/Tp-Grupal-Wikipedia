"""Intercambio de permisos y datos mediante transacciones AMQP."""

import logging

import pika

from ..interfaz import ErrorCapacidad, OperacionInterrumpida
from ..configuracion.modelos import ErrorConfiguracion


REGISTRO = logging.getLogger(__name__)
PERMISO = b"\x01"  # Cuerpo fijo de un permiso libre.
CABECERA_CAPACIDAD = "wikipedia_capacidad"
VERSION_CAPACIDAD = 2  # Identifica entregas publicadas con permisos transaccionales.
COLA_INEXISTENTE = 404  # Código NOT_FOUND de AMQP al consultar una cola ausente.


def publicar_permiso(canal, cola):
    """Incluye un permiso persistente en la transacción del canal proporcionado."""
    canal.basic_publish(
        exchange="", routing_key=cola, body=PERMISO, mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/octet-stream",
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        ),
    )


class CapacidadDatos:
    """Transfiere capacidad entre un permiso libre y una entrega pendiente.

    Cada copia física publicada consume un permiso; resolverla lo devuelve.
    El permiso permanece sin ACK hasta confirmar la publicación del dato en la
    misma transacción. RabbitMQ permanece disponible y la topología no se elimina
    durante la ejecución. El canal es exclusivo de esta instancia secuencial.
    """

    def __init__(self, conexion, topologia):
        self._conexion = conexion
        self._topologia = topologia
        self._canal = conexion.crear_canal()
        self._consumidor = None
        self._esperando = False
        self._detenido = False
        try:
            estado = self._canal.queue_declare(queue=topologia.marca_capacidad(), passive=True).method
            if estado.message_count != 1:
                raise ErrorConfiguracion("La inicialización de capacidad todavía no está completa")
            self._canal.tx_select()
            self._canal.basic_qos(prefetch_count=1, global_qos=True)
        except pika.exceptions.ChannelClosedByBroker as error:
            if error.reply_code == COLA_INEXISTENTE:
                raise ErrorConfiguracion("Inicializar la capacidad del despliegue antes de abrir el middleware") from error
            raise ErrorCapacidad("No se pudo abrir el canal de permisos") from error
        except (pika.exceptions.AMQPError, OSError) as error:
            raise ErrorCapacidad("No se pudo abrir el canal de permisos") from error

    def publicar_dato(self, destino, cuerpo, propiedades):
        """Consume un permiso y publica una copia del dato en una misma transacción."""
        self._conexion.canal
        if self._esperando or self._consumidor is not None:
            raise ErrorCapacidad("La reserva de capacidad no admite reentrancia")
        if self._detenido:
            raise OperacionInterrumpida("La espera de capacidad fue interrumpida")
        self._esperando = True
        try:
            etiqueta = self._esperar_permiso(destino.cola)
            self._conexion.comprobar_apagado()
            # La dirección física ya fue validada por la topología. El exchange
            # predeterminado entrega exactamente a esta cola, sin copias adicionales.
            self._canal.basic_publish(
                exchange="", routing_key=destino.cola, body=cuerpo,
                properties=propiedades, mandatory=True,
            )
            self._canal.basic_ack(delivery_tag=etiqueta)
            self._canal.tx_commit()
        except BaseException as error:
            self._cerrar_reserva_interrumpida()
            if isinstance(error, (pika.exceptions.AMQPError, OSError)):
                raise ErrorCapacidad("No se pudo completar el intercambio de permiso por dato") from error
            raise
        finally:
            self._esperando = False

    def _esperar_permiso(self, cola_datos):
        """Espera una entrega sin confirmarla, atendiendo eventos del broker."""
        entrega = []
        self._consumidor = self._canal.basic_consume(
            queue=self._topologia.cola_capacidad(cola_datos), auto_ack=False,
            on_message_callback=lambda _canal, metodo, _propiedades, cuerpo:
                self._recibir_permiso(entrega, metodo, cuerpo),
        )
        self._canal.start_consuming()
        if self._detenido:
            raise OperacionInterrumpida("La espera de capacidad fue interrumpida")
        if not entrega:
            raise ErrorCapacidad("El broker canceló la espera sin entregar un permiso")
        etiqueta, cuerpo = entrega[0]
        if cuerpo != PERMISO:
            raise ErrorCapacidad("El permiso recibido es inválido")
        return etiqueta

    def _recibir_permiso(self, entrega, metodo, cuerpo):
        """Conserva una sola entrega y cancela la suscripción sin confirmar el permiso."""
        entrega.append((metodo.delivery_tag, cuerpo))
        self._cancelar_consumidor()

    def _cancelar_consumidor(self):
        """Cancela la suscripción de permisos conservando la entrega sin ACK."""
        if self._consumidor is not None and self._canal.is_open:
            self._canal.basic_cancel(consumer_tag=self._consumidor)
            self._consumidor = None

    def _cerrar_reserva_interrumpida(self):
        """Cierra el canal para reencolar entregas no confirmadas sin inventar devoluciones."""
        self._detenido = True
        try:
            self._conexion.preparar_limpieza()
            if self._canal.is_open:
                self._canal.close()
        except Exception:
            REGISTRO.exception("Falló el cierre de la reserva; se conserva el error original")

    def resolver_entrega(self, canal, cola_datos, etiqueta, *, rechazar, reservada):
        """Confirma o rechaza el dato y devuelve su permiso en la misma transacción."""
        if reservada:
            publicar_permiso(canal, self._topologia.cola_capacidad(cola_datos))
        if rechazar:
            canal.basic_reject(delivery_tag=etiqueta, requeue=False)
        else:
            canal.basic_ack(delivery_tag=etiqueta)
        canal.tx_commit()

    def interrumpir(self):
        """Cancela la espera y rechaza reservas nuevas en esta instancia."""
        self._detenido = True
        self._cancelar_consumidor()

    @staticmethod
    def tiene_reserva(propiedades):
        """Comprueba que el dato pertenece al intercambio transaccional de capacidad."""
        cabeceras = propiedades.headers if propiedades is not None else None
        valor = cabeceras.get(CABECERA_CAPACIDAD) if isinstance(cabeceras, dict) else None
        return type(valor) is int and valor == VERSION_CAPACIDAD
