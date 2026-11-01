import logging
import zlib
from typing import List

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import (
    Esquema, Lote, Palabra, PalabraIdioma, CantidadPalabras, Candidato, TextoArticulo
)
from .tokenizer import extraer_palabras_unicas

logger = logging.getLogger(__name__)


class WordOccurrencesWorker(WorkerBase):
    """Tokeniza abstracts de Wikipedia para alimentar Q3 (intersección), Q4 (frecuencias) y Q5 (promedio y candidatos)."""

    def __init__(self, ruta_configuracion: str):
        super().__init__(ruta_configuracion)
        # Las palabras se reparten entre las instancias de Q3 según su hash
        self._particiones_q3 = self.configuracion.obtener_grupo("q3_intersection").instancias

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        entrada_nombre = datos.entrada.nombre
        registros = datos.contenido.registros

        palabras_q4: List[Palabra] = []
        cantidades_q5: List[CantidadPalabras] = []
        candidatos_q5: List[Candidato] = []
        pares_q3 = [set() for _ in range(self._particiones_q3)]  # (palabra, idioma) sin repetir, por partición

        for articulo in registros:
            if not isinstance(articulo, TextoArticulo):
                continue
            
            palabras = extraer_palabras_unicas(articulo.abstract)
            cantidad = len(palabras)

            # datos  para Q4 (palabras únicas por abstract sin stopwords)
            for pal in palabras:
                palabras_q4.append(Palabra(palabra=pal))
                # dato para Q3: la misma palabra siempre va a la misma partición
                pares_q3[zlib.crc32(pal.encode("utf-8")) % self._particiones_q3].add((pal, articulo.idioma))

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

        # Hacia Q5 Average (históricos), Q5 Comparator (candidatos), Q4 Frecuencias (palabras únicas por
        # abstract) y Q3 Intersección (pares palabra-idioma). Cada salida se parte en los mensajes que
        # hagan falta para respetar el tamaño máximo.
        self._publicar_por_presupuesto("q5_average", Entrada(id_ejecucion, "cantidades_historicas"), Esquema.CANTIDAD_PALABRAS, cantidades_q5)
        self._publicar_por_presupuesto("q5_comparator", Entrada(id_ejecucion, "candidatos"), Esquema.CANDIDATO, candidatos_q5)
        self._publicar_por_presupuesto("frecuencias", Entrada(id_ejecucion, "palabras"), Esquema.PALABRA, palabras_q4)
        for particion, pares in enumerate(pares_q3):
            self._publicar_por_presupuesto(
                "q3_intersection", Entrada(id_ejecucion, "palabras_idioma"), Esquema.PALABRA_IDIOMA,
                [PalabraIdioma(palabra, idioma) for palabra, idioma in sorted(pares)],
                clave_afinidad=str(particion),
            )

    def _publicar_por_presupuesto(self, grupo: str, entrada: Entrada, esquema: Esquema, registros: list,
                                  clave_afinidad: str = None) -> None:
        """
        Publica los registros en tantos lotes como haga falta para no superar max_bytes_por_mensaje.

        Se llena cada mensaje según el presupuesto de bytes del middleware; con clave de afinidad,
        todos van a la misma instancia del destino.
        """
        if not registros:
            return
        pendientes: list = []
        presupuesto = self.middleware.crear_presupuesto_lote(entrada, esquema)
        for registro in registros:
            if not presupuesto.admitir(registro):
                self.middleware.publicar(grupo, Datos(entrada, Lote(esquema, tuple(pendientes))),
                                         clave_afinidad=clave_afinidad)
                pendientes = []
                presupuesto = self.middleware.crear_presupuesto_lote(entrada, esquema)
                if not presupuesto.admitir(registro):
                    raise ValueError(f"Un registro de {esquema.name} no cabe en un lote vacío")
            pendientes.append(registro)
        self.middleware.publicar(grupo, Datos(entrada, Lote(esquema, tuple(pendientes))),
                                 clave_afinidad=clave_afinidad)

    def al_finalizar(self, entrada: Entrada) -> None:
        """Propaga el cierre correspondiente al tramo completado."""
        id_ejecucion = entrada.id_ejecucion
        logger.info("word_occurrences: cierre recibido para entrada '%s' de '%s'", entrada.nombre, id_ejecucion)

        if entrada.nombre == "textos_historicos":
            try:
                self.middleware.finalizar_entrada("q5_average", Entrada(id_ejecucion, "cantidades_historicas"))
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en q5_average: %s", error)

        elif entrada.nombre == "textos_recientes":
            try:
                self.middleware.finalizar_entrada("q5_comparator", Entrada(id_ejecucion, "candidatos"))
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en q5_comparator: %s", error)

        # Si ya no quedan entradas pendientes para esta ejecución, cerramos la salida hacia frecuencias (Q4)
        if not self.middleware.entradas_pendientes(id_ejecucion):
            try:
                self.middleware.finalizar_entrada("frecuencias", Entrada(id_ejecucion, "palabras"))
            except Exception as error:
                logger.debug("word_occurrences: error al finalizar en frecuencias: %s", error)
            # Q3 necesita el cierre de todas sus particiones: lo coordina el middleware
            self.middleware.finalizar_entrada("q3_intersection", Entrada(id_ejecucion, "palabras_idioma"))

