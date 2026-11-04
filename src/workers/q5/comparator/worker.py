from contextlib import closing
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
        # Tamaño de lote para escrituras agrupadas en disco
        self._batch_size_q5 = int(os.environ.get("BATCH_SIZE_Q5", os.environ.get("batch_size_q5", "1000")))
        # Buffer de candidatos en memoria antes de persistir a disco: id_ejecucion -> List[Candidato]
        self._buffer_candidatos: Dict[str, List[Candidato]] = {}
        self.registrar_limpieza(self._buffer_candidatos.clear)

    def _volcar_buffer(self, id_ejecucion: str) -> None:
        """Escribe los candidatos acumulados en memoria al archivo temporal de la ejecución."""
        candidatos = self._buffer_candidatos.pop(id_ejecucion, None)
        if candidatos:
            self._storage.guardar_candidatos(id_ejecucion, candidatos)
            logger.debug(
                "q5_comparator: volcados %d candidatos a disco para '%s'",
                len(candidatos), id_ejecucion
            )

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q5_comparator", id_ejecucion)
        entrada_nombre = datos.entrada.nombre

        if entrada_nombre == "candidatos":
            candidatos = []
            for registro in datos.contenido.registros:
                self.comprobar_detencion()
                if isinstance(registro, Candidato):
                    candidatos.append(registro)
            if candidatos:
                if id_ejecucion not in self._buffer_candidatos:
                    self._buffer_candidatos[id_ejecucion] = []
                self._buffer_candidatos[id_ejecucion].extend(candidatos)

                if len(self._buffer_candidatos[id_ejecucion]) >= self._batch_size_q5:
                    self._volcar_buffer(id_ejecucion)

        elif entrada_nombre == "resumen_historico":
            for reg in datos.contenido.registros:
                self.comprobar_detencion()
                if isinstance(reg, ResumenHistorico):
                    self._resumenes[id_ejecucion] = (reg.suma, reg.articulos)

    def al_finalizar(self, entrada: Entrada) -> None:
        """Verifica si ambas entradas (candidatos y resumen) cerraron para esta ejecución."""
        id_ejecucion = entrada.id_ejecucion
        if entrada.nombre == "candidatos":
            self._volcar_buffer(id_ejecucion)

        # Si aún quedan entradas pendientes, no procesamos aún (esperamos la barrera completa)
        if self.middleware.entradas_pendientes(id_ejecucion):
            return

        self._filtrar_y_publicar_resultados(id_ejecucion)

    def _filtrar_y_publicar_resultados(self, id_ejecucion: str) -> None:
        self._volcar_buffer(id_ejecucion)
        suma, articulos = self._resumenes.pop(id_ejecucion, (0, 0))
        promedio = (suma / articulos) if articulos > 0 else 0.0

        entrada_salida = Entrada(id_ejecucion, "q5")
        presupuesto = self.middleware.crear_presupuesto_lote(entrada_salida, Esquema.CANDIDATO)
        lote_actual: List[Candidato] = []

        try:
            with closing(self._storage.iterar_candidatos(id_ejecucion)) as candidatos:
                for candidato in candidatos:
                    self.comprobar_detencion()
                    if candidato.cantidad > promedio:
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

        except BaseException:
            try:
                self._limpiar_candidatos(id_ejecucion)
            except Exception:
                logger.exception("Falló la limpieza de candidatos; se conserva el error original")
            raise
        else:
            self._limpiar_candidatos(id_ejecucion)

        self.middleware.finalizar_entrada("gateway_resultados", entrada_salida)

    def _limpiar_candidatos(self, id_ejecucion):
        self._buffer_candidatos.pop(id_ejecucion, None)
        self._storage.limpiar_ejecucion(id_ejecucion)

    def _publicar_lote(self, entrada: Entrada, registros: List[Candidato]) -> None:
        if not registros:
            return
        datos = Datos(entrada, Lote(Esquema.CANDIDATO, tuple(registros)))
        self.middleware.publicar("gateway_resultados", datos)
