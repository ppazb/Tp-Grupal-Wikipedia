from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada


class FormatterQ1(WorkerBase):
    """
    Formateador de salida para Query 1.
    Transfiere los resultados filtrados hacia gateway_resultado.
    """

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q1_formatter", datos.entrada.id_ejecucion)
        id_ejecucion = datos.entrada.id_ejecucion
        nueva_entrada = Entrada(id_ejecucion, "resultados_finales_q1")
        self.middleware.publicar("gateway_resultados", Datos(nueva_entrada, datos.contenido))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Indica al gateway (EOF) de la Query 1."""
        id_ejecucion = entrada.id_ejecucion
        nueva_entrada = Entrada(id_ejecucion, "resultados_finales_q1")
        self.middleware.finalizar_entrada("gateway_resultados", nueva_entrada)
