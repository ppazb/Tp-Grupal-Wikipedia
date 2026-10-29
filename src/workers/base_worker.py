from src.common.runtime.signal_handler import ManejadorSenales
from src.middleware.cargador_configuracion import cargar_configuracion
from src.middleware.rabbitmq import MiddlewareRabbitMQ

class WorkerBase:
    """Clase base que encapsula el ciclo de vida del middleware y el graceful quit."""
    def __init__(self, ruta_configuracion: str):
        self.configuracion = cargar_configuracion(ruta_configuracion)
        self.middleware = MiddlewareRabbitMQ(self.configuracion)
        self.detener = False
        self.senales = ManejadorSenales(self._detener_worker)

    def _detener_worker(self):
        self.detener = True
        self.middleware.detener_recepcion()

    def iniciar(self):
        """Mantiene la escucha activa procesando eventos hasta recibir SIGTERM."""
        with self.middleware:
            while not self.detener:
                self.middleware.recibir(self.procesar, al_finalizar=self.al_finalizar)

    def procesar(self, datos):
        raise NotImplementedError("Debe ser implementado por la clase hija")

    def al_finalizar(self, entrada):
        raise NotImplementedError("Debe ser implementado por la clase hija")