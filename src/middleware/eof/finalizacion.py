"""Secuencia común de finalización con estado recuperable provisto por el negocio."""

from abc import ABC, abstractmethod

from ..interfaz import ErrorSeguimientoEntrada


class FinalizadorRecuperable(ABC):
    """Prepara, publica y confirma un cierre usando el WAL del worker.

    Se utiliza como callback al_finalizar. Cada operación debe poder repetirse
    después de una caída. El estado preparado se conserva hasta la limpieza
    segura de la ejecución, incluso después de confirmar el negocio.
    """

    def __init__(self, middleware):
        self._middleware = middleware

    def __call__(self, entrada):
        """Completa salidas y cierres antes de confirmar la finalización del negocio."""
        self.preparar_finalizacion(entrada)
        for envio in self.publicaciones_preparadas(entrada):
            self._validar_ejecucion(entrada, envio.datos.entrada)
            self._middleware.publicar(envio)
        for grupo, salida in self.cierres_de_salida(entrada):
            self._validar_ejecucion(entrada, salida)
            self._middleware.finalizar_entrada(grupo, salida)
        self.confirmar_negocio(entrada)

    @staticmethod
    def _validar_ejecucion(entrada, salida):
        """Impide publicar o cerrar salidas de otro cliente durante la finalización."""
        if entrada.id_ejecucion != salida.id_ejecucion:
            raise ErrorSeguimientoEntrada('La salida de finalización pertenece a otra ejecución')

    @abstractmethod
    def preparar_finalizacion(self, entrada):
        """Persiste o recupera el estado necesario para reconstruir las salidas del cierre.

        Retorna después de sincronizar su WAL. Repetirla conserva la preparación
        original; no descarta acumulados ni vuelve a aplicar sus contribuciones.
        """

    @abstractmethod
    def publicaciones_preparadas(self, entrada):
        """Itera los envíos reconstruibles con idénticos IDs, contenidos, destinos y orden.

        Entrega todos los envíos preparados: el middleware omite los confirmados.
        No retira datos necesarios para reconstruirlos ni renumera los índices.
        """

    @abstractmethod
    def cierres_de_salida(self, entrada):
        """Itera pares (grupo, Entrada) cuyas dependencias se completaron, aun sin resultados."""

    @abstractmethod
    def confirmar_negocio(self, entrada):
        """Persiste de forma idempotente la finalización sin borrar su preparación recuperable."""
