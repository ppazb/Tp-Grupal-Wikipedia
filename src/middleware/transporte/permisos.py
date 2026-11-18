"""Mensajes de turnos exclusivos y avisos de capacidad."""

import pika


PERMISO = b'\x01'


def publicar_permiso(canal, cola):
    """Publica un turno o aviso en un canal con confirmaciones habilitadas."""
    canal.basic_publish(
        exchange='', routing_key=cola, body=PERMISO, mandatory=True,
        properties=pika.BasicProperties(
            content_type='application/octet-stream',
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        ),
    )
