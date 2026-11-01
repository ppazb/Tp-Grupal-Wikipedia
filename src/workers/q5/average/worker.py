import logging
from src.middleware.interfaz import OperacionInterrumpida
from typing import Dict

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Esquema, Lote, CantidadPalabras, ResumenHistorico

logger = logging.getLogger(__name__)


class AverageQ5Worker(WorkerBase):
    """Calcula la suma y cantidad de artículos históricos (<= 2020) para determinar el promedio."""

    def __init__(self, ruta_configuracion: str):
        super().__init__(ruta_configuracion)
        # Diccionario aislado por ejecución para soporte multi-client concurrente
        self._acumuladores: Dict[str, Dict[str, int]] = {}
        self.registrar_limpieza(self._acumuladores.clear)

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        if id_ejecucion not in self._acumuladores:
            self._acumuladores[id_ejecucion] = {"suma": 0, "articulos": 0}

        subtotal_palabras = 0
        articulos_lote = 0

        for registro in datos.contenido.registros:
            self.comprobar_detencion()
            if isinstance(registro, CantidadPalabras):
                subtotal_palabras += registro.cantidad
                articulos_lote += 1

        self._acumuladores[id_ejecucion]["suma"] += subtotal_palabras
        self._acumuladores[id_ejecucion]["articulos"] += articulos_lote

        logger.debug(
            "q5_average: lote de %d artículos para '%s'. Acumulado: %d palabras en %d artículos",
            articulos_lote, id_ejecucion,
            self._acumuladores[id_ejecucion]["suma"],
            self._acumuladores[id_ejecucion]["articulos"]
        )

    def al_finalizar(self, entrada: Entrada) -> None:
        """Al recibir EOF consolidado de históricos, emite el resumen a q5_comparator y libera memoria."""
        id_ejecucion = entrada.id_ejecucion
        totales = self._acumuladores.pop(id_ejecucion, {"suma": 0, "articulos": 0})
        suma = totales["suma"]
        articulos = totales["articulos"]
        promedio = (suma / articulos) if articulos > 0 else 0.0

        logger.info(
            "q5_average: cierre para '%s'. Total suma=%d, total articulos=%d (promedio histórico: %.2f)",
            id_ejecucion, suma, articulos, promedio
        )

        resumen = ResumenHistorico(suma=suma, articulos=articulos)
        entrada_resumen = Entrada(id_ejecucion, "resumen_historico")
        lote_resumen = Lote(Esquema.RESUMEN_HISTORICO, (resumen,))

        try:
            self.middleware.publicar("q5_comparator", Datos(entrada_resumen, lote_resumen))
            self.middleware.finalizar_entrada("q5_comparator", entrada_resumen)
        except OperacionInterrumpida:
            raise
        except Exception as error:
            logger.error("q5_average: error al publicar resumen a q5_comparator: %s", error)
            raise
