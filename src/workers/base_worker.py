"""Ciclo secuencial de recepción y liberación de ejecuciones para workers."""

import logging
from abc import ABC, abstractmethod

from src.common.protocol.dto import Entrada
from src.middleware.rechazos import ENTRADA_ERRORES

logger = logging.getLogger(__name__)


class Worker(ABC):
    """Conecta callbacks de negocio con un middleware proporcionado por el proceso.

    El propietario abre y cierra el middleware. La detención se solicita mediante
    detener(), fuera de un manejador de señal, para terminar el ciclo sin polling.
    """

    # Grupos que reciben las notificaciones de rechazo de este worker. Se cierran al terminar la
    # ejecución: el destino no la da por completa hasta recibir el EOF de todos sus productores.
    destinos_errores = ()

    def __init__(self, middleware):
        self._middleware = middleware
        self._detenido = False
        self._completada = None

    def ejecutar(self):
        """Consume entradas y libera su ejecución después de confirmar el cierre local."""
        while not self._detenido:
            self._completada = None
            self._middleware.recibir(self.procesar, al_finalizar=self._finalizar)
            if self._completada is not None:
                self._middleware.liberar_ejecucion(self._completada)

    def detener(self):
        """Detiene nuevas recepciones y deja completar el callback en curso."""
        self._detenido = True
        self._middleware.detener_recepcion()

    def _finalizar(self, entrada):
        self.finalizar_entrada(entrada)
        if not self._middleware.entradas_pendientes(entrada.id_ejecucion):
            for destino in self.destinos_errores:
                self._middleware.finalizar_entrada(destino, Entrada(entrada.id_ejecucion, ENTRADA_ERRORES))
            self._completada = entrada.id_ejecucion

    @abstractmethod
    def procesar(self, datos):
        """Procesa los registros de una entrada."""

    @abstractmethod
    def finalizar_entrada(self, entrada):
        """Emite resultados pendientes y cierra las salidas que completó esta entrada."""


def destinos_errores(configuracion):
    """Grupos a los que el proceso local envía notificaciones de rechazo."""
    grupo = configuracion.proceso.grupo
    return tuple(sorted({
        ruta.destino for ruta in configuracion.rutas
        if ruta.origen == grupo and ruta.entrada == ENTRADA_ERRORES
    }))


class WorkerBase(Worker):
    """Abre el middleware desde configuración y adapta los callbacks de los nodos."""

    def __init__(self, ruta_configuracion):
        from src.middleware.cargador_configuracion import cargar_configuracion
        from src.middleware.rabbitmq import MiddlewareRabbitMQ

        self.configuracion = cargar_configuracion(ruta_configuracion)
        self.middleware = MiddlewareRabbitMQ(self.configuracion)
        super().__init__(self.middleware)
        self._clientes_vistos = set()
        self.destinos_errores = destinos_errores(self.configuracion)

    def iniciar(self):
        """Ejecuta el ciclo común y restaura señales al cerrar el middleware."""
        from src.common.runtime.signal_handler import ManejadorSenales

        senales = ManejadorSenales(self.detener)
        try:
            with self.middleware:
                self.ejecutar()
        finally:
            senales.restaurar()

    def anunciar_cliente(self, nombre, id_ejecucion):
        """Registra en el log la primera vez que llegan datos de una ejecución a este worker."""
        if id_ejecucion not in self._clientes_vistos:
            self._clientes_vistos.add(id_ejecucion)
            logger.info("%s: llegó un nuevo cliente (ejecución '%s')", nombre, id_ejecucion)

    def finalizar_entrada(self, entrada):
        """Delega la finalización de negocio al callback del nodo."""
        self.al_finalizar(entrada)
        self._clientes_vistos.discard(entrada.id_ejecucion)

    @abstractmethod
    def al_finalizar(self, entrada):
        """Emite pendientes y cierra las salidas correspondientes a la entrada."""
