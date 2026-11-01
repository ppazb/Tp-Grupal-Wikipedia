import logging
from src.middleware.interfaz import OperacionInterrumpida
from typing import List

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import (
    Esquema, Lote, Palabra, CantidadPalabras, Candidato, TextoArticulo
)
from .tokenizer import extraer_palabras_unicas

logger = logging.getLogger(__name__)


class WordOccurrencesWorker(WorkerBase):
    """Tokeniza abstracts de Wikipedia para alimentar Q4 (frecuencias) y Q5 (promedio y candidatos)."""

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        entrada_nombre = datos.entrada.nombre
        registros = datos.contenido.registros

        palabras_q4: List[Palabra] = []
        cantidades_q5: List[CantidadPalabras] = []
        candidatos_q5: List[Candidato] = []

        for articulo in registros:
            self.comprobar_detencion()
            if not isinstance(articulo, TextoArticulo):
                continue
            
            palabras = extraer_palabras_unicas(articulo.abstract, self.comprobar_detencion)
            cantidad = len(palabras)

            # datos  para Q4 (palabras únicas por abstract sin stopwords)
            for pal in palabras:
                self.comprobar_detencion()
                palabras_q4.append(Palabra(palabra=pal))

            # datos para Q5 según el período del artículo
            if entrada_nombre == "textos_historicos":
                cantidades_q5.append(CantidadPalabras(cantidad=cantidad))
            elif entrada_nombre == "textos_recientes":
                candidatos_q5.append(Candidato(url=articulo.url, cantidad=cantidad))

        logger.info(
            "word_occurrences: procesados %d abstracts de '%s' para '%s'. "
            "Palabras Q4: %d | Cantidades Q5: %d | Candidatos Q5: %d",
            len(registros), entrada_nombre, id_ejecucion,
            len(palabras_q4), len(cantidades_q5), len(candidatos_q5)
        )

        # Publicar hacia Q5 Average (históricos)
        if cantidades_q5:
            try:
                entrada_avg = Entrada(id_ejecucion, "cantidades_historicas")
                lote_avg = Lote(Esquema.CANTIDAD_PALABRAS, tuple(cantidades_q5))
                self.middleware.publicar("q5_average", Datos(entrada_avg, lote_avg))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: no se publicó en q5_average: %s", error)

        # Publicar hacia Q5 Comparator (candidatos)
        if candidatos_q5:
            try:
                entrada_comp = Entrada(id_ejecucion, "candidatos")
                lote_comp = Lote(Esquema.CANDIDATO, tuple(candidatos_q5))
                self.middleware.publicar("q5_comparator", Datos(entrada_comp, lote_comp))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: no se publicó en q5_comparator: %s", error)

        # Publicar hacia Q4 Frecuencias (todas las palabras únicas por abstract)
        if palabras_q4:
            try:
                entrada_palabras = Entrada(id_ejecucion, "palabras")
                lote_palabras = Lote(Esquema.PALABRA, tuple(palabras_q4))
                self.middleware.publicar("frecuencias", Datos(entrada_palabras, lote_palabras))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: no se publicó en frecuencias: %s", error)

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga el cierre correspondiente al tramo completado."""
        id_ejecucion = entrada.id_ejecucion
        logger.info("word_occurrences: cierre recibido para entrada '%s' de '%s'", entrada.nombre, id_ejecucion)

        if entrada.nombre == "textos_historicos":
            try:
                self.middleware.finalizar_entrada("q5_average", Entrada(id_ejecucion, "cantidades_historicas"))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en q5_average: %s", error)

        elif entrada.nombre == "textos_recientes":
            try:
                self.middleware.finalizar_entrada("q5_comparator", Entrada(id_ejecucion, "candidatos"))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en q5_comparator: %s", error)

        # Si ya no quedan entradas pendientes para esta ejecución, cerramos la salida hacia frecuencias (Q4)
        if not self.middleware.entradas_pendientes(id_ejecucion):
            try:
                self.middleware.finalizar_entrada("frecuencias", Entrada(id_ejecucion, "palabras"))
            except OperacionInterrumpida:
                raise
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en frecuencias: %s", error)

