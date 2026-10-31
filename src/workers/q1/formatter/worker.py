import logging

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada

logger = logging.getLogger(__name__)


class FormatterQ1(WorkerBase):
    """
    Formateador de salida para Query 1.
    Transfiere los resultados filtrados hacia gateway_resultado.
    """

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q1_formatter", datos.entrada.id_ejecucion)
        id_ejecucion = datos.entrada.id_ejecucion
        cantidad = len(datos.contenido.registros)
        logger.info(
            "q1_formatter: transfiriendo %d resultados Q1 para ejecución '%s' hacia gateway_resultados",
            cantidad, id_ejecucion
        )
        nueva_entrada = Entrada(id_ejecucion, "resultados_finales_q1")
        self.middleware.publicar("gateway_resultados", Datos(nueva_entrada, datos.contenido))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Indica al gateway (EOF) de la Query 1."""
        id_ejecucion = entrada.id_ejecucion
        logger.info("q1_formatter: cierre (EOF) recibido para '%s'. Notificando a gateway_resultados.", id_ejecucion)
        nueva_entrada = Entrada(id_ejecucion, "resultados_finales_q1")
        self.middleware.finalizar_entrada("gateway_resultados", nueva_entrada)
