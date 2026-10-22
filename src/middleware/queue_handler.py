"""Declaración y enlaces de colas internas del transporte RabbitMQ."""


def declarar_cola(canal, nombre: str, max_mensajes: int) -> None:
    """Declara una cola durable con límite de mensajes y rechazo de nuevos envíos.

    El límite cuenta mensajes listos para entregar, excluyendo entregas sin ACK.
    """
    canal.queue_declare(
        queue=nombre,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={
            "x-queue-type": "classic",
            "x-max-length": max_mensajes,
            "x-overflow": "reject-publish",
        },
    )


def enlazar_cola(canal, cola: str, exchange: str, clave: str) -> None:
    """Conecta una cola a una ruta exacta de un exchange."""
    canal.queue_bind(queue=cola, exchange=exchange, routing_key=clave)
