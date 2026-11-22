import logging
from typing import Dict, Iterable, Tuple

from src.workers.base_worker import WorkerBase
from src.workers.deduplicacion import DedupDesdeAfinidad, DedupDesdeCompartida
from src.workers.registro_wal import codificar, decodificar, entrega_de
from src.workers.wal_ejecuciones import WalsPorEjecucion
from src.common.protocol.dto import Datos, Entrada
from src.common.protocol.registros import Candidato, Esquema, Lote, ResumenHistorico
from .registros_wal import (
    TIPO_CANDIDATOS, TIPO_CANDIDATOS_EVALUADOS, TIPO_RESUMEN,
    escribir_candidatos, escribir_resumen, leer_candidatos, leer_resumen,
)

logger = logging.getLogger(__name__)

ENTRADA_CANDIDATOS = "candidatos"
ENTRADA_RESUMEN = "resumen_historico"
GRUPO_RESULTADOS = "gateway_resultados"


class ComparatorQ5Worker(WorkerBase):
    """
    Sincroniza el promedio histórico con los artículos recientes (> 2020) y emite el resultado final.

    Su WAL guarda los candidatos que llegan antes del resumen. Cuando llega el resumen se
    evalúan y publican; los que llegan después se evalúan y publican al llegar.
    """

    def __init__(self, ruta_configuracion: str, directorio_wal: str = None):
        super().__init__(ruta_configuracion)
        # Resumen histórico por ejecución: id_ejecucion -> (suma, articulos)
        self._resumenes: Dict[str, Tuple[int, int]] = {}
        self.registrar_limpieza(self._resumenes.clear)
        # Los candidatos vienen de word_occurrences (cola compartida) y el resumen de q5_average (afinidad)
        self._dedup_candidatos = DedupDesdeCompartida()
        self._dedup_resumen = DedupDesdeAfinidad()
        self._wals = WalsPorEjecucion(directorio_wal)
        self.registrar_limpieza(self._wals.cerrar)
        self._recuperar()

    def _recuperar(self) -> None:
        # al arrancar releo el wal para recuperar el resumen y rearmar los dedup
        for id_ejecucion, registros in self._wals.recuperar():
            for datos in registros:
                tipo, identidad, lector = decodificar(datos)
                if tipo == TIPO_RESUMEN:
                    self._resumenes[id_ejecucion] = leer_resumen(lector)
                    self._dedup_resumen.registrar(entrega_de(Entrada(id_ejecucion, ENTRADA_RESUMEN), identidad))
                else:
                    self._dedup_candidatos.registrar(entrega_de(Entrada(id_ejecucion, ENTRADA_CANDIDATOS), identidad))

    def procesar(self, datos: Datos) -> None:
        id_ejecucion = datos.entrada.id_ejecucion
        self.anunciar_cliente("q5_comparator", id_ejecucion)
        # si la ejecucion ya cerro, descarto el lote
        if self._wals.esta_cerrada(id_ejecucion):
            return
        entrega = self.middleware.entrega_actual

        if datos.entrada.nombre == ENTRADA_CANDIDATOS:
            # si el lote ya lo procese lo descarto
            if self._dedup_candidatos.es_duplicado(entrega):
                return
            candidatos = []
            for registro in datos.contenido.registros:
                self.comprobar_detencion()
                if isinstance(registro, Candidato):
                    candidatos.append(registro)
            if id_ejecucion in self._resumenes:
                # ya tengo el promedio: evaluo y publico al toque, en el wal solo guardo la identidad
                self._publicar_mayores(id_ejecucion, candidatos, self._promedio(id_ejecucion))
                self._wals.agregar(id_ejecucion, codificar(TIPO_CANDIDATOS_EVALUADOS, entrega))
            else:
                # todavia no llego el resumen: guardo los candidatos en el wal para evaluarlos despues
                self._wals.agregar(id_ejecucion, codificar(
                    TIPO_CANDIDATOS, entrega, lambda escritor: escribir_candidatos(escritor, candidatos),
                ))
            # publico primero y persisto despues: si me caigo en el medio, el lote vuelve y
            # sale con los mismos ids, asi q el siguiente lo descarta
            self._dedup_candidatos.registrar(entrega)

        elif datos.entrada.nombre == ENTRADA_RESUMEN:
            if self._dedup_resumen.es_duplicado(entrega):
                return
            resumenes = [r for r in datos.contenido.registros if isinstance(r, ResumenHistorico)]
            if not resumenes:
                return
            suma, articulos = resumenes[-1].suma, resumenes[-1].articulos
            promedio = (suma / articulos) if articulos > 0 else 0.0
            # llego el resumen: evaluo los candidatos q estaban guardados en el wal
            self._publicar_mayores(id_ejecucion, self._candidatos_guardados(id_ejecucion), promedio)
            self._wals.agregar(id_ejecucion, codificar(
                TIPO_RESUMEN, entrega, lambda escritor: escribir_resumen(escritor, suma, articulos),
            ))
            self._resumenes[id_ejecucion] = (suma, articulos)
            self._dedup_resumen.registrar(entrega)

    def al_finalizar(self, entrada: Entrada) -> None:
        """Con ambas entradas cerradas, cierra la salida hacia el gateway y la ejecución."""
        id_ejecucion = entrada.id_ejecucion
        if self._wals.esta_cerrada(id_ejecucion):
            return
        # si aun quedan entradas pendientes no cierro todavia (espero a q cierren las dos)
        if self.middleware.entradas_pendientes(id_ejecucion):
            return

        if id_ejecucion not in self._resumenes:
            # no llego el resumen (no deberia pasar): comparo contra un promedio de cero
            self._publicar_mayores(id_ejecucion, self._candidatos_guardados(id_ejecucion), 0.0)
        self.middleware.finalizar_entrada(GRUPO_RESULTADOS, Entrada(id_ejecucion, "q5"))

        # marco la ejecucion como cerrada, borro su wal y me olvido del estado
        self._wals.cerrar_ejecucion(id_ejecucion)
        self._resumenes.pop(id_ejecucion, None)
        self._dedup_candidatos.cerrar_ejecucion(id_ejecucion)
        self._dedup_resumen.cerrar_ejecucion(id_ejecucion)

    def _promedio(self, id_ejecucion: str) -> float:
        suma, articulos = self._resumenes[id_ejecucion]
        return (suma / articulos) if articulos > 0 else 0.0

    def _candidatos_guardados(self, id_ejecucion: str) -> Iterable[Candidato]:
        """Candidatos guardados en el WAL de la ejecución, de a uno y en orden."""
        for datos in self._wals.iterar(id_ejecucion):
            tipo, _, lector = decodificar(datos)
            if tipo == TIPO_CANDIDATOS:
                yield from leer_candidatos(lector)

    def _publicar_mayores(self, id_ejecucion: str, candidatos: Iterable[Candidato], promedio: float) -> None:
        """Publica los candidatos por encima del promedio, en tantos mensajes como haga falta."""
        entrada_salida = Entrada(id_ejecucion, "q5")
        presupuesto = self.middleware.crear_presupuesto_lote(entrada_salida, Esquema.CANDIDATO)
        lote_actual = []
        for candidato in candidatos:
            self.comprobar_detencion()
            if candidato.cantidad <= promedio:
                continue
            if not presupuesto.admitir(candidato):
                self._publicar_lote(entrada_salida, lote_actual)
                lote_actual = []
                presupuesto = self.middleware.crear_presupuesto_lote(entrada_salida, Esquema.CANDIDATO)
                if not presupuesto.admitir(candidato):
                    raise ValueError("El candidato supera el tamaño máximo permitido para un lote vacío")
            lote_actual.append(candidato)
        self._publicar_lote(entrada_salida, lote_actual)

    def _publicar_lote(self, entrada: Entrada, registros: list) -> None:
        if registros:
            datos = Datos(entrada, Lote(Esquema.CANDIDATO, tuple(registros)))
            self.middleware.publicar(self.envio(GRUPO_RESULTADOS, datos))
