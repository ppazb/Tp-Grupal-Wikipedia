"""Captura y despacho de señales del sistema operativo para graceful shutdown."""

import logging
import signal
from typing import Callable

logger = logging.getLogger(__name__)


class ManejadorSenales:
    """Registra manejadores para SIGTERM y SIGINT ejecutando un callback de cierre."""

    def __init__(self, callback: Callable[[], None]):
        self._callback = callback
        self._senales_registradas = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                previo = signal.signal(sig, self._manejador)
                self._senales_registradas.append((sig, previo))
            except (ValueError, OSError) as error:
                # Ocurre si no se ejecuta en el hilo principal (e.g. en subprocesos/tests)
                logger.debug("No se pudo registrar señal %s: %s", sig, error)

    def _manejador(self, signo: int, frame) -> None:
        nombre_senal = signal.Signals(signo).name if hasattr(signal, "Signals") else str(signo)
        logger.info("Señal capturada: %s. Iniciando detención graceful...", nombre_senal)
        if callable(self._callback):
            self._callback()

    def restaurar(self) -> None:
        """Restaura los manejadores de señal originales."""
        for sig, previo in self._senales_registradas:
            try:
                signal.signal(sig, previo)
            except (ValueError, OSError):
                pass

