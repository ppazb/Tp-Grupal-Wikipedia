"""API pública del middleware de comunicación por grupos."""

from .interfaz import (
    Datos,
    Entrada,
    EntradaFinalizada,
    ErrorCierre,
    ErrorConexion,
    ErrorMensaje,
    ErrorMiddleware,
    Evento,
    FinEntrada,
    Middleware,
    MiddlewareCerrado,
)

__all__ = [
    "Datos",
    "Entrada",
    "EntradaFinalizada",
    "ErrorCierre",
    "ErrorConexion",
    "ErrorMensaje",
    "ErrorMiddleware",
    "Evento",
    "FinEntrada",
    "Middleware",
    "MiddlewareCerrado",
]
