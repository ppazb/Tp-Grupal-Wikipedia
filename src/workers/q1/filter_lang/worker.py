import logging

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, ResultadoQ1, Idioma

logger = logging.getLogger(__name__)


class FilterLangQ1(WorkerBase):
    """
    Filtra articulos conservando únicamente artículos en idioma ingles  y
    emite el  mensaje  con unicamente el resultado necesario(url, fecha).
    """

    def procesar(self, datos: Datos) -> None:
        self.anunciar_cliente("q1_filter_lang", datos.entrada.id_ejecucion)
        registros_ingles = []
        for referencia in datos.contenido.registros:
            if referencia.idioma == Idioma.INGLES:
                # Me quedo solo con URL y fecha
                registros_ingles.append(
                    ResultadoQ1(referencia.url, referencia.fecha)
                )

        id_ejecucion = datos.entrada.id_ejecucion
        logger.info(
            "q1_filter_lang: filtrados %d/%d artículos en inglés para ejecución '%s'",
            len(registros_ingles), len(datos.contenido.registros), id_ejecucion
        )

        if registros_ingles:
            nueva_entrada = Entrada(id_ejecucion, "filtrados_ing_q1")
            nuevo_lote = Lote(Esquema.RESULTADO_Q1, tuple(registros_ingles))
            self.middleware.publicar("q1_formatter", Datos(nueva_entrada, nuevo_lote))

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga (EOF) hacia el formateador (su nodo siguiente)."""
        id_ejecucion = entrada.id_ejecucion
        logger.info("q1_filter_lang: cierre (EOF) recibido para '%s'. Propagando a q1_formatter.", id_ejecucion)
        nueva_entrada = Entrada(id_ejecucion, "filtrados_ing_q1")
        self.middleware.finalizar_entrada("q1_formatter", nueva_entrada)
