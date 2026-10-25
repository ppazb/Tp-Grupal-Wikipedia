"""Declaración de exchanges internos."""


def declarar_exchange(canal, nombre: str) -> None:
    """Declara un exchange directo y durable para enrutar por claves exactas."""
    canal.exchange_declare(
        exchange=nombre,
        exchange_type="direct",
        durable=True,
        auto_delete=False,
    )
