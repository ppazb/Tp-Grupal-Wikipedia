import logging
from typing import Optional

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import (
    Esquema, Lote, ReferenciaHistorica, ReferenciaBiografica, TextoArticulo
)

logger = logging.getLogger(__name__)


def _extraer_anio(fecha: str) -> Optional[int]:
    """Extrae el año de cuatro dígitos de una cadena de fecha."""
    if not fecha:
        return None
    limpia = fecha.strip()
    if len(limpia) >= 4 and limpia[:4].isdigit():
        return int(limpia[:4])
    return None


class DateRouter(WorkerBase):
    """
    Bifurcador Consume los articulos que envia el gateway, recorta las columnas
    que no se necesitan y replica mensajes hacia las ramas correspondientes según el año
    del artículo Por ej: Query 1: filtra artículos hasta 2020 inclusive y envia
    (url, fecha, idioma), minimizando la carga en la red.
    """

    def procesar(self, datos: Datos) -> None:
        historicos_q1 = []
        biograficos_q2 = []
        textos_historicos = []
        textos_recientes = []

        total_articulos = len(datos.contenido.registros)
        for articulo in datos.contenido.registros:

            anio = _extraer_anio(articulo.fecha)
            if anio is not None and anio <= 2020:
                # Query 1: únicamente URL, fecha e idioma
                historicos_q1.append(
                    ReferenciaHistorica(articulo.url, articulo.fecha, articulo.idioma)
                )
                # Query 2: artículos hasta 2020 con nombre, url, secciones e imagen
                biograficos_q2.append(
                    ReferenciaBiografica(
                        articulo.nombre, articulo.url, articulo.secciones, articulo.url_imagen
                    )
                )
                # NLP / Q5 histórico: abstract, idioma y url
                if articulo.abstract:
                    textos_historicos.append(
                        TextoArticulo(articulo.url, articulo.idioma, articulo.abstract)
                    )
            elif anio is not None and anio >= 2021:
                # NLP / Q5 candidatos recientes: abstract, idioma y url
                if articulo.abstract:
                    textos_recientes.append(
                        TextoArticulo(articulo.url, articulo.idioma, articulo.abstract)
                    )
            elif anio is None:
                logger.warning(
                    "date_router: artículo con formato de fecha inválido ignorado: %s (fecha: '%s')",
                    articulo.url, articulo.fecha
                )

        id_ejecucion = datos.entrada.id_ejecucion
        logger.info(
            "date_router: procesado lote de %d artículos para ejecución '%s'. "
            "Candidatos Q1 (<=2020): %d, Históricos NLP: %d, Recientes NLP: %d",
            total_articulos, id_ejecucion, len(historicos_q1), len(textos_historicos), len(textos_recientes)
        )

        # Envio hacia nodo de la rama Query 1
        if historicos_q1:
            entrada_q1 = Entrada(id_ejecucion, "historicos_q1")
            lote_q1 = Lote(Esquema.REFERENCIA_HISTORICA, tuple(historicos_q1))
            self.middleware.publicar("q1_filter_lang", Datos(entrada_q1, lote_q1))

        # Envio hacia nodo de la rama Query 2
        if biograficos_q2:
            entrada_q2 = Entrada(id_ejecucion, "historicos_q2")
            lote_q2 = Lote(Esquema.REFERENCIA_BIOGRAFICA, tuple(biograficos_q2))
            self.middleware.publicar("q2_filter_bio", Datos(entrada_q2, lote_q2))

        # Envio hacia rama NLP / Q5: Históricos
        if textos_historicos:
            try:
                entrada_hist = Entrada(id_ejecucion, "textos_historicos")
                lote_hist = Lote(Esquema.TEXTO_ARTICULO, tuple(textos_historicos))
                self.middleware.publicar("word_occurrences", Datos(entrada_hist, lote_hist))
            except Exception as error:
                logger.debug("date_router: no se publicó en word_occurrences (historicos): %s", error)

        # Envio hacia rama NLP / Q5: Recientes
        if textos_recientes:
            try:
                entrada_rec = Entrada(id_ejecucion, "textos_recientes")
                lote_rec = Lote(Esquema.TEXTO_ARTICULO, tuple(textos_recientes))
                self.middleware.publicar("word_occurrences", Datos(entrada_rec, lote_rec))
            except Exception as error:
                logger.debug("date_router: no se publicó en word_occurrences (recientes): %s", error)


    def al_finalizar(self, entrada: Entrada) -> None:
        """
        Propaga la barrera de cierre (EOF) a todas las ramas dependientes
        para habilitar la consolidación de resultados en nodos subsiguientes.
        """
        id_ejecucion = entrada.id_ejecucion
        logger.info("date_router: cierre (EOF) recibido para '%s'. Propagando a ramas.", id_ejecucion)

        # Rama Query 1 obligatoria
        self.middleware.finalizar_entrada("q1_filter_lang", Entrada(id_ejecucion, "historicos_q1"))

        # Rama NLP / Q5
        try:
            self.middleware.finalizar_entrada("word_occurrences", Entrada(id_ejecucion, "textos_historicos"))
            self.middleware.finalizar_entrada("word_occurrences", Entrada(id_ejecucion, "textos_recientes"))
        except Exception as error:
            logger.debug("date_router: no se propagó EOF a word_occurrences: %s", error)

        # Rama Query 2
        self.middleware.finalizar_entrada("q2_filter_bio", Entrada(id_ejecucion, "historicos_q2"))
