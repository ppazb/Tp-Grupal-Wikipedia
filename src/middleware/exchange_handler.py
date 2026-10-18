"""Declaración de exchanges internos."""


def declarar_exchange(canal, nombre: str) -> None:
    """Las claves exactas permiten separar destino, entrada y modo de envío."""
    canal.exchange_declare(
        exchange=nombre,
        exchange_type="direct",
        durable=True,
        auto_delete=False,
    )
