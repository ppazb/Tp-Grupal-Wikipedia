"""Declaración y enlaces de colas internas del transporte RabbitMQ."""


def declarar_cola(canal, nombre: str, max_mensajes=None) -> None:
    """Declara una cola durable, con límite opcional y rechazo de nuevos envíos.

    El límite cuenta mensajes listos para entregar, excluyendo entregas sin ACK.
    """
    argumentos = {"x-queue-type": "classic"}
    if max_mensajes is not None:
        argumentos.update({"x-max-length": max_mensajes, "x-overflow": "reject-publish"})
    canal.queue_declare(
        queue=nombre,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments=argumentos,
    )


def enlazar_cola(canal, cola: str, exchange: str, clave: str) -> None:
    """Conecta una cola a una ruta exacta de un exchange."""
    canal.queue_bind(queue=cola, exchange=exchange, routing_key=clave)
