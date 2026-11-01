import logging
import os
from typing import Dict, List, Tuple

from src.workers.base_worker import WorkerBase
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import (
    Esquema, Lote, Candidato, ResumenHistorico
)
from .storage import AlmacenamientoCandidatos

logger = logging.getLogger(__name__)


class ComparatorQ5Worker(WorkerBase):
    """Sincroniza el promedio histórico con los artículos recientes (> 2020) y emite el resultado final."""

    def __init__(self, ruta_configuracion: str):
        super().__init__(ruta_configuracion)
        directorio_temp = os.environ.get("TEMP_DIR", "data/temp")
        self._storage = AlmacenamientoCandidatos(
            directorio_temp, comprobar_detencion=self.comprobar_detencion,
        )
        self.registrar_limpieza(self._storage.cerrar)
        # Resumen histórico por ejecución: id_ejecucion -> (suma, articulos)
        self._resumenes: Dict[str, Tuple[int, int]] = {}
        self.registrar_limpieza(self._resumenes.clear)

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        entrada_nombre = datos.entrada.nombre

        if entrada_nombre == "candidatos":
            candidatos = []
            for registro in datos.contenido.registros:
                self.comprobar_detencion()
                if isinstance(registro, Candidato):
                    candidatos.append(registro)
            if candidatos:
                self._storage.guardar_candidatos(id_ejecucion, candidatos)
                logger.debug("q5_comparator: guardados %d candidatos en disco para '%s'", len(candidatos), id_ejecucion)

        elif entrada_nombre == "resumen_historico":
            for reg in datos.contenido.registros:
                self.comprobar_detencion()
                if isinstance(reg, ResumenHistorico):
                    self._resumenes[id_ejecucion] = (reg.suma, reg.articulos)
                    promedio = (reg.suma / reg.articulos) if reg.articulos > 0 else 0.0
                    logger.info(
                        "q5_comparator: recibido resumen histórico para '%s': suma=%d, articulos=%d, promedio=%.2f",
                        id_ejecucion, reg.suma, reg.articulos, promedio
                    )

    def al_finalizar(self, entrada: Entrada) -> None:
        """Verifica si ambas entradas (candidatos y resumen) cerraron para esta ejecución."""
        id_ejecucion = entrada.id_ejecucion
        logger.info("q5_comparator: cierre recibido para entrada '%s' de '%s'", entrada.nombre, id_ejecucion)

        # Si aún quedan entradas pendientes, no procesamos aún (esperamos la barrera completa)
        if self.middleware.entradas_pendientes(id_ejecucion):
            logger.info("q5_comparator: '%s' aún tiene entradas pendientes. Esperando barrera...", id_ejecucion)
            return

        logger.info("q5_comparator: todas las entradas cerradas para '%s'. Iniciando comparación y emisión.", id_ejecucion)
        self._filtrar_y_publicar_resultados(id_ejecucion)

    def _filtrar_y_publicar_resultados(self, id_ejecucion: str) -> None:
        suma, articulos = self._resumenes.pop(id_ejecucion, (0, 0))
        promedio = (suma / articulos) if articulos > 0 else 0.0

        logger.info("q5_comparator: evaluando candidatos de '%s' contra promedio histórico de %.4f", id_ejecucion, promedio)

        entrada_salida = Entrada(id_ejecucion, "q5")
        presupuesto = self.middleware.crear_presupuesto_lote(entrada_salida, Esquema.CANDIDATO)
        lote_actual: List[Candidato] = []
        total_evaluados = 0
        total_superaron = 0

        try:
            for candidato in self._storage.iterar_candidatos(id_ejecucion):
                self.comprobar_detencion()
                total_evaluados += 1
                if candidato.cantidad > promedio:
                    total_superaron += 1
                    if not presupuesto.admitir(candidato):
                        self._publicar_lote(entrada_salida, lote_actual)
                        lote_actual.clear()
                        presupuesto = self.middleware.crear_presupuesto_lote(entrada_salida, Esquema.CANDIDATO)
                        if not presupuesto.admitir(candidato):
                            raise ValueError("El candidato supera el tamaño máximo permitido para un lote vacío")
                    lote_actual.append(candidato)

            # Publicar último lote remanente
            if lote_actual:
                self._publicar_lote(entrada_salida, lote_actual)

            logger.info(
                "q5_comparator: ejecución '%s' completada. Evaluados: %d | Superaron promedio: %d",
                id_ejecucion, total_evaluados, total_superaron
            )

            # Finalizar entrada hacia gateway_resultados
            self.middleware.finalizar_entrada("gateway_resultados", entrada_salida)

        finally:
            # Liberación de recursos en disco
            self._storage.limpiar_ejecucion(id_ejecucion)

    def _publicar_lote(self, entrada: Entrada, registros: List[Candidato]) -> None:
        if not registros:
            return
        datos = Datos(entrada, Lote(Esquema.CANDIDATO, tuple(registros)))
        self.middleware.publicar("gateway_resultados", datos)
