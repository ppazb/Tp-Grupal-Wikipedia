"""Ciclo secuencial de recepción y liberación de ejecuciones para workers."""

from abc import ABC, abstractmethod


class Worker(ABC):
    """Conecta callbacks de negocio con un middleware proporcionado por el proceso.

    El propietario abre y cierra el middleware. La detención se solicita mediante
    detener(), fuera de un manejador de señal, para terminar el ciclo sin polling.
    """

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
            self._completada = entrada.id_ejecucion

    @abstractmethod
    def procesar(self, datos):
        """Procesa los registros de una entrada."""

    @abstractmethod
    def finalizar_entrada(self, entrada):
        """Emite resultados pendientes y cierra las salidas que completó esta entrada."""


class WorkerBase(Worker):
    """Abre el middleware desde configuración y adapta los callbacks de los nodos."""

    def __init__(self, ruta_configuracion):
        from src.middleware.cargador_configuracion import cargar_configuracion
        from src.middleware.rabbitmq import MiddlewareRabbitMQ

        self.configuracion = cargar_configuracion(ruta_configuracion)
        self.middleware = MiddlewareRabbitMQ(self.configuracion)
        super().__init__(self.middleware)

    def iniciar(self):
        """Ejecuta el ciclo común y restaura señales al cerrar el middleware."""
        from src.common.runtime.signal_handler import ManejadorSenales

        senales = ManejadorSenales(self.detener)
        try:
            with self.middleware:
                self.ejecutar()
        finally:
            senales.restaurar()

    def finalizar_entrada(self, entrada):
        """Delega la finalización de negocio al callback del nodo."""
        self.al_finalizar(entrada)

    @abstractmethod
    def al_finalizar(self, entrada):
        """Emite pendientes y cierra las salidas correspondientes a la entrada."""
